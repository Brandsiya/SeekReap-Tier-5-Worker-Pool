import os
import json
import time
import psycopg2
import requests
from http.server import HTTPServer, BaseHTTPRequestHandler
from threading import Thread


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(
            json.dumps({"status": "ok", "worker": "processing"}).encode()
        )

    def log_message(self, format, *args):
        pass


def run_health_server():
    port   = int(os.environ.get("PORT", 8080))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    print(f"Health server on port {port}")
    server.serve_forever()


Thread(target=run_health_server, daemon=True).start()

TIER3_URL   = os.environ.get("TIER3_URL", "https://seekreap-tier-3-dev.fly.dev")
TIER4_URL   = os.environ.get("TIER4_URL", "https://seekreap-tier-4-dev.fly.dev")
MAX_RETRIES = 3


def get_db():
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise ValueError("DATABASE_URL not set")
    return psycopg2.connect(url, connect_timeout=30)


def _set_submission_state(cur, submission_id, status,
                          risk_score=None, risk_level=None,
                          failure_reason=None):
    """
    Single writer for submissions + content_submissions.
    Guards against regressing a completed/failed row to an earlier state.
    Caller must conn.commit() after this.
    """
    if status == "completed":
        cur.execute(
            """
            UPDATE submissions
            SET status             = 'completed',
                completed_at       = NOW(),
                overall_risk_score = %s,
                risk_level         = %s
            WHERE id = %s
              AND status NOT IN ('completed', 'failed')
            """,
            (risk_score, risk_level, submission_id),
        )
    elif status == "failed":
        cur.execute(
            """
            UPDATE submissions
            SET status         = 'failed',
                failure_reason = %s
            WHERE id = %s
              AND status NOT IN ('completed', 'failed')
            """,
            (failure_reason, submission_id),
        )
    elif status == "processing":
        cur.execute(
            """
            UPDATE submissions
            SET status = 'processing'
            WHERE id = %s
              AND status NOT IN ('completed', 'failed')
            """,
            (submission_id,),
        )
    else:
        cur.execute(
            """
            UPDATE submissions
            SET status = %s
            WHERE id = %s
              AND status NOT IN ('completed', 'failed')
            """,
            (status, submission_id),
        )

    cur.execute(
        """
        INSERT INTO content_submissions (submission_id, status, updated_at)
        VALUES (%s, %s, NOW())
        ON CONFLICT (submission_id)
        DO UPDATE SET
            status     = EXCLUDED.status,
            updated_at = NOW()
        WHERE content_submissions.status NOT IN ('completed', 'failed')
        """,
        (submission_id, status),
    )


def process_job(job_id, submission_id):
    """
    Returns: 'success' | 'retry' | 'failed'
    Never raises — all exceptions are caught and mapped to 'failed'.
    """
    print(f"Processing job {job_id} for submission {submission_id}")
    conn = None
    cur  = None
    try:
        conn = get_db()
        cur  = conn.cursor()

        _set_submission_state(cur, submission_id, "processing")
        conn.commit()

        cur.execute(
            "SELECT content_hash, work_type, plan, title FROM submissions WHERE id = %s",
            (submission_id,),
        )
        sub = cur.fetchone()
        if not sub:
            reason = f"submission {submission_id} not found in DB"
            print(reason)
            _set_submission_state(cur, submission_id, "failed", failure_reason=reason)
            cur.execute(
                "UPDATE job_queue SET status='failed', failure_reason=%s WHERE job_id=%s",
                (reason, job_id),
            )
            conn.commit()
            return "failed"

        content_hash, work_type, plan, title = sub
        content_hash = content_hash or "unknown"
        work_type    = work_type    or "other"
        plan         = plan         or "free"
        title        = title        or "Untitled"

        payload = {
            "submission_id": submission_id,
            "content_id":    submission_id,
            "content_hash":  content_hash,
            "content_type":  work_type,
            "title":         title,
            "contentdata": {
                "audio_similarity":  0.0,
                "visual_similarity": 0.0,
                "duplicate_content": False,
                "flags":             [],
            },
        }

        resp    = None
        ok      = False
        t3_data = {}
        try:
            resp = requests.post(f"{TIER3_URL}/api/analyze", json=payload, timeout=30)
            if resp.status_code == 200:
                try:
                    t3_data = resp.json()
                    ok      = True
                except Exception:
                    print(f"Tier-3 invalid JSON: {resp.text[:200]}")
            else:
                print(f"Tier-3 HTTP {resp.status_code}: {resp.text[:200]}")
        except Exception as ex:
            print(f"Tier-3 unreachable: {ex} — completing with default low-risk")
            ok      = True
            t3_data = {"risk_score": 0, "risk_level": "low"}

        if ok:
            risk_score = t3_data.get("risk_score", 0)
            risk_level = t3_data.get("risk_level", "low")

            _set_submission_state(
                cur, submission_id, "completed",
                risk_score=risk_score,
                risk_level=risk_level,
            )
            cur.execute(
                """
                UPDATE job_queue
                SET status       = 'completed',
                    completed_at = NOW()
                WHERE job_id = %s
                """,
                (job_id,),
            )
            conn.commit()
            print(f"Job {job_id} completed — plan={plan} risk={risk_score}")
            return "success"

        else:
            cur.execute(
                """
                UPDATE job_queue
                SET attempts = attempts + 1
                WHERE job_id = %s
                RETURNING attempts
                """,
                (job_id,),
            )
            attempts = cur.fetchone()[0]
            reason   = (
                f"Tier-3 HTTP {resp.status_code}" if resp else "Tier-3 unreachable"
            )

            if attempts < MAX_RETRIES:
                cur.execute(
                    "UPDATE job_queue SET status='pending' WHERE job_id=%s",
                    (job_id,),
                )
                conn.commit()
                print(f"Job {job_id} attempt {attempts}/{MAX_RETRIES} — re-queued")
                return "retry"
            else:
                _set_submission_state(
                    cur, submission_id, "failed", failure_reason=reason
                )
                cur.execute(
                    """
                    UPDATE job_queue
                    SET status         = 'failed',
                        failure_reason = %s
                    WHERE job_id = %s
                    """,
                    (reason, job_id),
                )
                conn.commit()
                print(f"Job {job_id} permanently failed after {attempts} attempts: {reason}")
                return "failed"

    except Exception as e:
        import traceback
        print(f"Job {job_id} error: {e}")
        traceback.print_exc()
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        return "failed"
    finally:
        if cur:
            try:
                cur.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass


if __name__ == "__main__":
    print("SeekReap Tier-5 Worker starting...")

    try:
        c = get_db()
        c.close()
        print("DB connection OK")
    except Exception as e:
        print(f"DB connection failed: {e}")

    while True:
        conn = None
        cur  = None
        try:
            conn = get_db()
            cur  = conn.cursor()
            cur.execute(
                """
                SELECT job_id, submission_id FROM job_queue
                WHERE status = 'pending'
                ORDER BY created_at
                LIMIT 1
                FOR UPDATE SKIP LOCKED
                """
            )
            job = cur.fetchone()

            if job:
                job_id, submission_id = job
                print(f"Found job {job_id}")

                cur.execute(
                    """
                    UPDATE job_queue
                    SET status                = 'processing',
                        processing_started_at = NOW()
                    WHERE job_id = %s
                    """,
                    (job_id,),
                )
                conn.commit()
                cur.close()
                conn.close()
                cur  = None
                conn = None

                result = process_job(job_id, str(submission_id))

                if result in ("failed", None):
                    try:
                        conn2 = get_db()
                        cur2  = conn2.cursor()
                        cur2.execute(
                            """
                            UPDATE job_queue
                            SET status         = 'failed',
                                failure_reason = 'process_job returned failed without updating queue'
                            WHERE job_id = %s
                              AND status   = 'processing'
                            """,
                            (job_id,),
                        )
                        conn2.commit()
                        cur2.close()
                        conn2.close()
                    except Exception:
                        pass

            else:
                print("No pending jobs, sleeping 10s...")
                cur.close()
                conn.close()
                cur  = None
                conn = None
                time.sleep(10)

        except Exception as e:
            print(f"Main loop error: {e}")
            time.sleep(10)
        finally:
            if cur:
                try:
                    cur.close()
                except Exception:
                    pass
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
