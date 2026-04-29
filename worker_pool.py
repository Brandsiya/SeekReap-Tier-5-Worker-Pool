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
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
        self.wfile.write(json.dumps({"status": "ok", "worker": "processing"}).encode())

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

    def log_message(self, format, *args):
        pass


def run_health_server():
    port = int(os.environ.get("PORT", 8080))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    print(f"✅ Health server on port {port}")
    server.serve_forever()


Thread(target=run_health_server, daemon=True).start()

TIER3_URL = os.environ.get('TIER3_URL', 'https://seekreap-tier-3-dev.fly.dev')
TIER4_URL = os.environ.get('TIER4_URL', 'https://seekreap-tier-4-dev.fly.dev')


def get_db():
    url = os.environ.get('DATABASE_URL')
    if not url:
        raise ValueError("DATABASE_URL not set")
    return psycopg2.connect(url, connect_timeout=30)


def generate_perceptual_fingerprint(file_url, file_type, content_hash, content_text=None):
    """Call Tier-3 to generate perceptual fingerprint"""
    try:
        payload = {
            "content_type": file_type,
            "content_path": file_url,
            "content_hash": content_hash
        }
        if content_text:
            payload["content_text"] = content_text
        
        response = requests.post(
            f"{TIER3_URL}/api/fingerprint",
            json=payload,
            timeout=30
        )
        if response.status_code == 200:
            data = response.json()
            return data.get("fingerprint")
        else:
            print(f"Fingerprint generation returned {response.status_code}")
            return None
    except Exception as e:
        print(f"Fingerprint error: {e}")
        return None


def find_similar_submissions(fingerprint, threshold=0.85):
    """Find existing submissions similar to this fingerprint"""
    try:
        response = requests.post(
            f"{TIER3_URL}/api/similarity/search",
            json={
                "fingerprint": fingerprint,
                "threshold": threshold
            },
            timeout=30
        )
        if response.status_code == 200:
            data = response.json()
            return data.get("matches", [])
    except Exception as e:
        print(f"Similarity search error: {e}")
    return []


def process_job(job_id, submission_id):
    print(f"📋 Processing job {job_id} for submission {submission_id}")
    conn = None
    cur = None
    try:
        conn = get_db()
        cur = conn.cursor()

        cur.execute("UPDATE submissions SET status = 'processing' WHERE id = %s", (submission_id,))
        conn.commit()

        # Fetch submission details
        cur.execute(
            "SELECT content_hash, work_type, plan, title FROM submissions WHERE id = %s",
            (submission_id,)
        )
        sub = cur.fetchone()
        if not sub:
            print(f"❌ Submission {submission_id} not found")
            return False

        content_hash = sub[0] or "unknown"
        work_type = sub[1] or "other"
        plan = sub[2] or "free"
        title = sub[3] or "Untitled"

        payload = {
            "submission_id": submission_id,
            "content_id": submission_id,
            "content_hash": content_hash,
            "content_type": work_type,
            "title": title,
            "contentdata": {
                "audio_similarity": 0.0,
                "visual_similarity": 0.0,
                "duplicate_content": False,
                "flags": [],
            },
        }

        # Call Tier-3 for analysis
        try:
            resp = requests.post(f"{TIER3_URL}/api/analyze", json=payload, timeout=30)
            ok = resp.status_code == 200
            t3_data = resp.json() if ok else {}
        except Exception as ex:
            print(f"⚠️ Tier-3 unreachable: {ex} — completing without analysis")
            ok = True
            t3_data = {"risk_score": 0, "risk_level": "low"}

        if ok:
            # Generate perceptual fingerprint
            fingerprint = generate_perceptual_fingerprint(None, work_type, content_hash, title)
            
            # Find similar submissions
            matches = []
            if fingerprint:
                matches = find_similar_submissions(fingerprint, threshold=0.85)
            
            # Calculate risk score from matches
            risk_score = max([m.get("similarity_score", 0) for m in matches], default=0)
            if risk_score >= 0.85:
                risk_level = "critical"
            elif risk_score >= 0.6:
                risk_level = "high"
            elif risk_score >= 0.3:
                risk_level = "medium"
            else:
                risk_level = "low"
            
            # Store fingerprint and matches in database
            if fingerprint:
                cur.execute("""
                    UPDATE submissions 
                    SET fingerprint_data = %s::jsonb,
                        fingerprint_type = %s,
                        overall_risk_score = %s,
                        risk_level = %s,
                        status = 'completed',
        _trigger_tier7_audit(submission_id)  # Tier-7 audit
                        completed_at = NOW()
                    WHERE id = %s
                """, (
                    json.dumps(fingerprint),
                    fingerprint.get("type"),
                    risk_score,
                    risk_level,
                    submission_id
                ))
            else:
                cur.execute("""
                    UPDATE submissions 
                    SET overall_risk_score = %s,
                        risk_level = %s,
                        status = 'completed',
                        completed_at = NOW()
                    WHERE id = %s
                """, (risk_score, risk_level, submission_id))
            
            conn.commit()
            print(f"✅ Job {job_id} completed — plan={plan} risk={risk_score}")
            return True
        else:
            reason = f"Tier-3 HTTP {resp.status_code}"
            cur.execute(
                "UPDATE submissions SET status='failed', failure_reason=%s WHERE id=%s",
                (reason, submission_id)
            )
            conn.commit()
            print(f"❌ Job {job_id} failed: {reason}")
            return False

    except Exception as e:
        print(f"❌ Job {job_id} error: {e}")
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



# ── Tier-7 audit trigger ─────────────────────────────────────────────────────
def _trigger_tier7_audit(submission_id):
    """
    Non-blocking fire-and-forget POST to Tier-7 after a job completes.
    Fails silently so Tier-5 is never blocked by Tier-7 unavailability.
    """
    import threading
    import urllib.request, urllib.error, json as _json

    def _post():
        tier7_url    = os.environ.get('TIER7_URL', '')
        tier7_secret = os.environ.get('TIER7_SECRET', 'seekreap-tier7-internal')
        if not tier7_url:
            return
        try:
            body = _json.dumps({'submission_id': str(submission_id)}).encode()
            req  = urllib.request.Request(
                tier7_url.rstrip('/') + '/api/audit/trigger',
                data=body,
                headers={'Content-Type': 'application/json',
                         'x-tier7-secret': tier7_secret},
                method='POST'
            )
            urllib.request.urlopen(req, timeout=5)
        except Exception:
            pass

    threading.Thread(target=_post, daemon=True).start()

if __name__ == "__main__":
    print("🚀 SeekReap Tier-5 Worker starting...")
    print("🔄 Worker will poll every 2 seconds with heartbeat")

    try:
        c = get_db()
        c.close()
        print("✅ DB connection OK")
    except Exception as e:
        print(f"❌ DB connection failed: {e}")

    heartbeat_counter = 0
    
    while True:
        try:
            conn = get_db()
            cur = conn.cursor()
            
            # Heartbeat every 10 loops (20 seconds)
            heartbeat_counter += 1
            if heartbeat_counter % 10 == 0:
                print(f"💓 heartbeat: worker alive (loop {heartbeat_counter})")
            
            cur.execute("""
                SELECT job_id, submission_id FROM job_queue
                WHERE status = 'pending'
                ORDER BY created_at
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            """)
            job = cur.fetchone()

            if job:
                job_id, submission_id = job
                print(f"🔍 Found job {job_id}")
                cur.execute("UPDATE job_queue SET status='processing' WHERE job_id=%s", (job_id,))
                conn.commit()
                cur.close()
                conn.close()

                success = process_job(job_id, str(submission_id))

                conn2 = get_db()
                cur2 = conn2.cursor()
                cur2.execute(
                    "UPDATE job_queue SET status=%s WHERE job_id=%s",
                    ('completed' if success else 'failed', job_id)
                )
                conn2.commit()
                cur2.close()
                conn2.close()
                print("💤 No pending jobs, sleeping 2s...")
            else:
                # Short sleep with heartbeat - prevents Fly from killing
                print("polling job queue...")
                time.sleep(2)

        except Exception as e:
            print(f"⚠️ Main loop error: {e}")
            time.sleep(2)
