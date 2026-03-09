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
TIER4_URL = os.environ.get("TIER4_URL")   # for status callbacks

# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------
def _get_db():
    return psycopg2.connect(DB_URL)

def _update_submission_status(submission_id: str, status: str):
    try:
        conn = _get_db()
        cur  = conn.cursor()
        cur.execute(
            "UPDATE submissions SET status = %s, updated_at = NOW() WHERE id = %s",
            (status, submission_id),
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error("DB update failed for %s: %s", submission_id, e)

def _notify_tier4(submission_id: str, status: str):
    if not TIER4_URL:
        return
    try:
        requests.post(
            f"{TIER4_URL}/api/job-update",
            json={"submission_id": submission_id, "status": status},
            timeout=10,
        )
        logger.info("Notified Tier-4: %s → %s", submission_id, status)
    except Exception as e:
        logger.error("Could not notify Tier-4: %s", e)

# ---------------------------------------------------------------------------
# Core worker logic (runs in background thread)
# ---------------------------------------------------------------------------
def process_submission(submission_id: str, payload: dict):
    def _run():
        logger.info("Processing submission: %s", submission_id)
        try:
            _update_submission_status(submission_id, "PROCESSING")

            # ----------------------------------------------------------------
            # TODO: Replace with real analysis logic
            # e.g. run ML model, scan content, write results to DB/GCS
            # ----------------------------------------------------------------
            time.sleep(2)   # placeholder
            result_status = "COMPLETED"
            # ----------------------------------------------------------------

            _update_submission_status(submission_id, result_status)
            _notify_tier4(submission_id, result_status)
            logger.info("Submission %s done: %s", submission_id, result_status)
        except Exception as e:
            logger.error("Worker failed for %s: %s", submission_id, e)
            _update_submission_status(submission_id, "FAILED")
            _notify_tier4(submission_id, "FAILED")

    threading.Thread(target=_run, daemon=True).start()

# ---------------------------------------------------------------------------
# Legacy polling loop (video_patterns table — keep running alongside HTTP)
# ---------------------------------------------------------------------------
def process_tasks():
    logger.info("SeekReap Worker Tier-5: legacy polling loop starting...")
    while True:
        try:
            conn = _get_db()
            cur  = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute("""
                UPDATE video_patterns
                SET status = 'processing', updated_at = CURRENT_TIMESTAMP
                WHERE id = (
                    SELECT id FROM video_patterns
                    WHERE status = 'pending'
                    ORDER BY created_at ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                ) RETURNING id, video_id
            """)
            task = cur.fetchone()
            if task:
                logger.info("Processing video: %s", task['video_id'])
                time.sleep(10)
                cur.execute("UPDATE video_patterns SET status = 'completed' WHERE id = %s", (task['id'],))
                logger.info("Completed video: %s", task['video_id'])
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            logger.error("Polling error: %s", e)
        time.sleep(15)

# ---------------------------------------------------------------------------
# HTTP server — handles /health and /process
# ---------------------------------------------------------------------------
class WorkerHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass  # suppress default access log noise

    def _send_json(self, code: int, data: dict):
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
                body   = json.loads(self.rfile.read(length))
                submission_id = body.get("submission_id")
                if not submission_id:
                    self._send_json(400, {"error": "submission_id required"})
                    return
                process_submission(submission_id, body)
                self._send_json(202, {"status": "accepted", "submission_id": submission_id})
            except Exception as e:
                self._send_json(500, {"error": str(e)})
        else:
            self._send_json(404, {"error": "not found"})

def run_http_server():
    server = HTTPServer(("", PORT), WorkerHandler)
    logger.info("Tier-5 HTTP server on port %d", PORT)
    server.serve_forever()

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    threading.Thread(target=run_http_server, daemon=True).start()
    process_tasks()
