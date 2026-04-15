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

TIER3_URL = os.environ.get("TIER3_URL", "https://seekreap-tier-3-dev.fly.dev")
TIER4_URL = os.environ.get("TIER4_URL", "https://seekreap-tier-4-dev.fly.dev")


def get_db():
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise ValueError("DATABASE_URL not set")
    return psycopg2.connect(url, connect_timeout=30)


def _set_submission_state(cur, submission_id, status,
                          risk_score=None, risk_level=None,
                          failure_reason=None):
    """
    Single helper that keeps submissions + content_submissions in sync.
    Call this instead of writing to either table directly.
    """
    if status == "completed":
        cur.execute(
            """
            UPDATE submissions
            SET status = 'completed',
                completed_at = NOW(),
                overall_risk_score = %s,
                risk_level = %s
            WHERE id = %s
            """,
            (risk_score, risk_level, submission_id),
        )
    elif status == "failed":
        cur.execute(
            """
            UPDATE submissions
            SET status = 'failed',
                failure_reason = %s
            WHERE id = %s
            """,
            (failure_reason, submission_id),
        )
    elif status == "processing":
        cur.execute(
            "UPDATE submissions SET status = 'processing' WHERE id = %s",
            (submission_id,),
        )
    else:
        cur.execute(
            "UPDATE submissions SET status = %s WHERE id = %s",
            (status, submission_id),
        )

    # Mirror every transition to content_submissions
    cur.execute(
        "UPDATE content_submissions SET status = %s WHERE submission_id = %s",
        (status, submission_id),
    )


def process_job(job_id, submission_id):
    print(f"Processing job {job_id} for submission {submission_id}")
    conn = None
    cur  = None
    try:
        conn = get_db()
        cur  = conn.cursor()

        # ── Mark processing ───────────────────────────────────────────────
        _set_submission_state(cur, submission_id, "processing")
        conn.commit()

        # ── Fetch submission details ──────────────────────────────────────
        cur.execute(
            "SELECT content_hash, work_type, plan, title FROM submissions WHERE id = %s",
            (submission_id,),
        )
        sub = cur.fetchone()
        if not sub:
            print(f"Submission {submission_id} not found in DB")
            return False

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

        # ── Call Tier 3 (stateless compute) ──────────────────────────────
        try:
            resp    = requests.post(f"{TIER3_URL}/api/analyze", json=payload, timeout=30)
            ok      = resp.status_code == 200
            t3_data = resp.json() if ok else {}
            if not ok:
                print(f"Tier-3 HTTP {resp.status_code}: {resp.text[:200]}")
        except Exception as ex:
            print(f"Tier-3 unreachable: {ex} — completing with default low-risk")
            ok      = True
            t3_data = {"risk_score": 0, "risk_level": "low"}

        # ── Tier-5 is sole writer — update ALL tables atomically ──────────
        if ok:
            risk_score = t3_data.get("risk_score", 0)
            risk_level = t3_data.get("risk_level", "low")

            _set_submission_state(
                cur, submission_id, "completed",
                risk_score=risk_score,
                risk_level=risk_level,
            )
            # job_queue follows the same transition
            cur.execute(
                "UPDATE job_queue SET status = 'completed', completed_at = NOW() WHERE job_id = %s",
                (job_id,),
            )
            conn.commit()
            print(f"Job {job_id} completed — plan={plan} risk={risk_score}")
            return True
        else:
            reason = f"Tier-3 HTTP {resp.status_code}"
            _set_submission_state(
                cur, submission_id, "failed", failure_reason=reason
            )
            cur.execute(
                "UPDATE job_queue SET status = 'failed', failure_reason = %s WHERE job_id = %s",
                (reason, job_id),
            )
            conn.commit()
            print(f"Job {job_id} failed: {reason}")
            return False

    except Exception as e:
        print(f"Job {job_id} error: {e}")
        import traceback
        traceback.print_exc()
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        return False
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

                # Claim the job before releasing the connection
                cur.execute(
                    "UPDATE job_queue SET status = 'processing', "
                    "processing_started_at = NOW() WHERE job_id = %s",
                    (job_id,),
                )
                conn.commit()
                cur.close()
                conn.close()
                cur  = None
                conn = None

                success = process_job(job_id, str(submission_id))

                # process_job writes its own final status, but guard
                # against the edge case where it returned False without
                # updating the queue (e.g. exception before first commit).
                if not success:
                    try:
                        conn2 = get_db()
                        cur2  = conn2.cursor()
                        cur2.execute(
                            """
                            UPDATE job_queue
                            SET status = 'failed',
                                failure_reason = 'process_job returned False'
                            WHERE job_id = %s AND status = 'processing'
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
