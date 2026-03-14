import time
import httpx
import redis
import logging
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Health Check Server for Cloud Run ---
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")

def run_health_server():
    """Runs a simple HTTP server on port 8080 for Cloud Run health checks."""
    server = HTTPServer(("0.0.0.0", 8080), Handler)
    logger.info("Health check server listening on port 8080")
    server.serve_forever()

# Start health server in background thread
threading.Thread(target=run_health_server, daemon=True).start()
logger.info("Health check server started in background thread")

# --- Redis Connection ---
try:
    r = redis.Redis(host="127.0.0.1", port=6379, db=0, decode_responses=True)
    r.ping()
    logger.info("Connected to Redis successfully")
except redis.exceptions.ConnectionError as e:
    logger.error(f"Failed to connect to Redis: {e}")
    exit(1)

# --- Core Worker Functions ---
def call_tier3(submission_id):
    try:
        resp = httpx.post("http://localhost:8000/api/analyze", json={
            "content_id": submission_id,
            "content_type": "youtube_video",
            "content_data": {"audio_similarity": 0.8, "visual_similarity": 0.6}
        }, timeout=30.0)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"Error calling Tier-3 for {submission_id}: {e}")
        return {"error": str(e)}

def update_tier4(submission_id, analysis):
    try:
        resp = httpx.post("http://localhost:8081/api/finalize", json={
            "submission_id": submission_id,
            "analysis": analysis
        }, timeout=30.0)
        resp.raise_for_status()
        logger.info(f"Updated Tier-4 for {submission_id}: {resp.text}")
    except Exception as e:
        logger.error(f"Error updating Tier-4 for {submission_id}: {e}")

# --- Main Worker Loop ---
if __name__ == "__main__":
    logger.info("Tier-5 main worker loop started...")
    while True:
        try:
            # Use blpop for blocking pop with timeout
            job = r.blpop("jobs", timeout=5)
            if job:
                job_id = job[1]
                logger.info(f"Processing job {job_id}")
                analysis = call_tier3(job_id)
                if "error" not in analysis:
                    update_tier4(job_id, analysis)
                else:
                    logger.error(f"Skipping Tier-4 update for {job_id} due to Tier-3 error.")
        except redis.exceptions.ConnectionError as e:
            logger.error(f"Redis connection lost: {e}. Reconnecting...")
            time.sleep(5)
            continue
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {e}")
            time.sleep(5)
