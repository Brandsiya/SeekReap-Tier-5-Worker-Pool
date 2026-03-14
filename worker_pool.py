import time
import httpx
import redis
import logging
import threading
import os
from http.server import HTTPServer, BaseHTTPRequestHandler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Health Check Server for Cloud Run ---
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/health' or self.path == '/':
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'OK')
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        # Suppress access logs to reduce noise
        return

def run_health_server():
    """Runs a simple HTTP server on the PORT defined by Cloud Run."""
    port = int(os.environ.get('PORT', 8080))
    server_address = ('0.0.0.0', port)
    httpd = HTTPServer(server_address, HealthCheckHandler)
    logger.info(f"Health check server listening on port {port}")
    httpd.serve_forever()
# --- End of Health Check Server ---

# --- Redis Connection ---
try:
    r = redis.Redis(host="127.0.0.1", port=6379, db=0, decode_responses=True)
    r.ping()
    logger.info("Connected to Redis successfully")
except redis.exceptions.ConnectionError as e:
    logger.error(f"Failed to connect to Redis: {e}")
    # Depending on your strategy, you might want to exit or retry.
    # For this example, we'll exit as the worker cannot function without Redis.
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
    # Start the health check server in a background thread
    health_thread = threading.Thread(target=run_health_server, daemon=True)
    health_thread.start()
    logger.info("Health check server started in background thread.")

    logger.info("Tier-5 main worker loop started...")
    while True:
        try:
            # Use blpop for blocking pop with timeout instead of non-blocking rpop + sleep
            # This is more efficient. It waits up to 5 seconds for a job.
            job = r.blpop("jobs", timeout=5)
            if job:
                # job is a tuple: (queue_name, job_id)
                job_id = job[1]
                logger.info(f"Processing job {job_id}")
                analysis = call_tier3(job_id)
                # Basic check if the analysis was successful before updating Tier-4
                if "error" not in analysis:
                    update_tier4(job_id, analysis)
                else:
                    logger.error(f"Skipping Tier-4 update for {job_id} due to Tier-3 error.")
        except redis.exceptions.ConnectionError as e:
            logger.error(f"Redis connection lost: {e}. Reconnecting...")
            # Implement reconnection logic or exit
            time.sleep(5)
            continue
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {e}")
            time.sleep(5)  # Avoid tight loop on persistent errors
