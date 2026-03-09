import threading
import os
import time
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DB_URL    = os.environ.get("DATABASE_URL")
PORT      = int(os.environ.get("PORT", 8080))
TIER4_URL = os.environ.get("TIER4_URL")

def _get_db():
    return psycopg2.connect(DB_URL)

def _update_submission_status(submission_id, status):
    if not DB_URL:
        return
    try:
        conn = _get_db()
        cur = conn.cursor()
        cur.execute("UPDATE submissions SET status = %s, completed_at = NOW() WHERE id = %s", (status, submission_id))
        conn.commit(); cur.close(); conn.close()
    except Exception as e:
        logger.error("DB update failed for %s: %s", submission_id, e)

def _notify_tier4(submission_id, status):
    if not TIER4_URL:
        return
    try:
        requests.post(f"{TIER4_URL}/api/job-update", json={"submission_id": submission_id, "status": status}, timeout=10)
    except Exception as e:
        logger.error("Could not notify Tier-4: %s", e)

def process_submission(submission_id, payload):
    def _run():
        try:
            _update_submission_status(submission_id, "PROCESSING")
            time.sleep(2)
            _update_submission_status(submission_id, "COMPLETED")
            _notify_tier4(submission_id, "COMPLETED")
        except Exception as e:
            logger.error("Worker failed for %s: %s", submission_id, e)
            _update_submission_status(submission_id, "FAILED")
            _notify_tier4(submission_id, "FAILED")
    threading.Thread(target=_run, daemon=True).start()

def process_tasks():
    logger.info("Tier-5: polling loop starting...")
    while True:
        if not DB_URL:
            time.sleep(30); continue
        try:
            conn = _get_db()
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute("""
                UPDATE video_patterns SET status = 'processing', updated_at = CURRENT_TIMESTAMP
                WHERE id = (SELECT id FROM video_patterns WHERE status = 'pending' ORDER BY created_at ASC FOR UPDATE SKIP LOCKED LIMIT 1)
                RETURNING id, video_id
            """)
            task = cur.fetchone()
            if task:
                time.sleep(10)
                cur.execute("UPDATE video_patterns SET status = 'completed' WHERE id = %s", (task['id'],))
            conn.commit(); cur.close(); conn.close()
        except Exception as e:
            logger.error("Polling error: %s", e)
        time.sleep(15)

class WorkerHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args): pass
    def _send_json(self, code, data):
        body = json.dumps(data).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def do_GET(self):
        if self.path == "/health":
            self._send_json(200, {"status": "healthy", "tier": 5})
        else:
            self._send_json(404, {"error": "not found"})
    def do_POST(self):
        if self.path == "/process":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = json.loads(self.rfile.read(length))
                sid = body.get("submission_id")
                if not sid:
                    self._send_json(400, {"error": "submission_id required"}); return
                process_submission(sid, body)
                self._send_json(202, {"status": "accepted", "submission_id": sid})
            except Exception as e:
                self._send_json(500, {"error": str(e)})
        else:
            self._send_json(404, {"error": "not found"})

if __name__ == "__main__":
    threading.Thread(target=process_tasks, daemon=True).start()
    logger.info("Tier-5 HTTP server starting on port %d", PORT)
    HTTPServer(("", PORT), WorkerHandler).serve_forever()
