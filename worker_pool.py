import time
import httpx
import logging
import threading
import os
import psycopg2
import psycopg2.extras
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Health Check Server for Cloud Run ---
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")

def run_health_server():
    port = int(os.environ.get('PORT', 8080))
    server = HTTPServer(("0.0.0.0", port), Handler)
    logger.info(f"Health check server listening on port {port}")
    server.serve_forever()

# Start health server in background thread
threading.Thread(target=run_health_server, daemon=True).start()
logger.info("Health check server started in background thread")

# --- Database Connection ---
def get_db():
    return psycopg2.connect(os.environ.get('DATABASE_URL'))

# --- Tier URLs ---
TIER3_URL = os.environ.get('TIER3_URL', 'https://seekreap-tier3-tif2gmgi4q-uc.a.run.app')
TIER4_URL = os.environ.get('TIER4_URL', 'https://seekreap-tier4-308655322607.us-central1.run.app')
MAX_RETRIES = 3

# --- Core Worker Functions ---
def call_tier3(submission_id, content_hash, content_type):
    try:
        url = f"{TIER3_URL}/api/analyze"
        logger.info(f"Calling Tier-3 at {url}")
        resp = httpx.post(url, json={
            "content_id": submission_id,
            "content_hash": content_hash,
            "content_type": content_type,
            "content_data": {"audio_similarity": 0.8, "visual_similarity": 0.6}
        }, timeout=30.0)
        resp.raise_for_status()
        logger.info(f"Tier-3 response: {resp.status_code}")
        return resp.json()
    except Exception as e:
        logger.error(f"Error calling Tier-3 for {submission_id}: {e}")
        return {"error": str(e)}

def update_tier4(submission_id, analysis):
    try:
        url = f"{TIER4_URL}/api/finalize"
        logger.info(f"Calling Tier-4 finalize at {url}")
        resp = httpx.post(url, json={
            "submission_id": submission_id,
            "analysis": analysis
        }, timeout=30.0)
        resp.raise_for_status()
        logger.info(f"Tier-4 response: {resp.status_code} - {resp.text}")
        return True
    except Exception as e:
        logger.error(f"Error updating Tier-4 for {submission_id}: {e}")
        if hasattr(e, 'response') and e.response:
            logger.error(f"Response status: {e.response.status_code}, body: {e.response.text}")
        return False

def process_job(job):
    """Process a single job from the queue."""
    job_id = job['job_id']
    submission_id = job['submission_id']
    attempts = job['attempts']
    
    logger.info(f"Processing job {job_id} for submission {submission_id} (attempt {attempts + 1})")

    # Get content details from submissions table
    conn = None
    cur = None
    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("""
            SELECT content_hash, content_type, title, description
            FROM submissions 
            WHERE id = %s
        """, (submission_id,))
        submission = cur.fetchone()
        
        if not submission:
            logger.error(f"Submission {submission_id} not found")
            return False, "Submission not found"
            
        content_hash = submission['content_hash']
        content_type = submission['content_type']
        
        # Call Tier-3 for analysis
        analysis = call_tier3(submission_id, content_hash, content_type)
        
        # Update Tier-4 with results
        if "error" not in analysis:
            success = update_tier4(submission_id, analysis)
            if success:
                return True, None
            else:
                return False, "Tier-4 update failed"
        else:
            return False, analysis.get("error")
            
    except Exception as e:
        logger.error(f"Error in process_job: {e}")
        return False, str(e)
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

# --- Main Worker Loop ---
if __name__ == "__main__":
    logger.info("Tier-5 worker started, polling PostgreSQL for jobs...")
    logger.info(f"TIER3_URL: {TIER3_URL}")
    logger.info(f"TIER4_URL: {TIER4_URL}")

    # Test database connection
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        conn.close()
        logger.info("Database connection successful")
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        exit(1)

    while True:
        conn = None
        cur = None
        try:
            conn = get_db()
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

            # Find pending jobs
            cur.execute("""
                SELECT job_id, submission_id, attempts
                FROM job_queue
                WHERE status = 'pending'
                ORDER BY created_at ASC
                LIMIT 5
                FOR UPDATE SKIP LOCKED
            """)

            jobs = cur.fetchall()
            
            if jobs:
                for job in jobs:
                    job_id = job['job_id']
                    attempts = job['attempts']
                    logger.info(f"Found job {job_id} (attempt {attempts + 1}), marking as processing")

                    # Mark as processing
                    cur.execute("""
                        UPDATE job_queue
                        SET status = 'processing',
                            attempts = attempts + 1
                        WHERE job_id = %s
                    """, (job_id,))
                    conn.commit()

                    # Process the job
                    success, error_msg = process_job(job)

                    if success:
                        # Mark as completed
                        cur.execute("""
                            UPDATE job_queue
                            SET status = 'completed'
                            WHERE job_id = %s
                        """, (job_id,))
                        logger.info(f"Job {job_id} completed successfully")
                    else:
                        # Check if we should retry
                        new_attempts = attempts + 1
                        if new_attempts >= MAX_RETRIES:
                            cur.execute("""
                                UPDATE job_queue
                                SET status = 'failed'
                                WHERE job_id = %s
                            """, (job_id,))
                            logger.error(f"Job {job_id} failed permanently: {error_msg}")
                        else:
                            cur.execute("""
                                UPDATE job_queue
                                SET status = 'pending'
                                WHERE job_id = %s
                            """, (job_id,))
                            logger.warning(f"Job {job_id} failed (attempt {new_attempts}), will retry: {error_msg}")

                    conn.commit()
            else:
                logger.debug("No pending jobs found, sleeping...")
                time.sleep(3)

        except Exception as e:
            logger.error(f"Error in main loop: {e}")
            import traceback
            traceback.print_exc()
            time.sleep(5)
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()
