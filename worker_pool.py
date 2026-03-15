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
TIER4_URL = os.environ.get('TIER4_URL', 'https://seekreap-tier4-tif2gmgi4q-uc.a.run.app')

# --- Core Worker Functions ---
def call_tier3(submission_id, content_hash, content_type):
    try:
        url = f"{TIER3_URL}/api/analyze"
        resp = httpx.post(url, json={
            "content_id": submission_id,
            "content_hash": content_hash,
            "content_type": content_type,
            "content_data": {"audio_similarity": 0.8, "visual_similarity": 0.6}
        }, timeout=30.0)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"Error calling Tier-3 for {submission_id}: {e}")
        return {"error": str(e)}

def update_tier4(submission_id, analysis):
    try:
        url = f"{TIER4_URL}/api/finalize"
        resp = httpx.post(url, json={
            "submission_id": submission_id,
            "analysis": analysis
        }, timeout=30.0)
        resp.raise_for_status()
        logger.info(f"Updated Tier-4 for {submission_id}: {resp.text}")
        return True
    except Exception as e:
        logger.error(f"Error updating Tier-4 for {submission_id}: {e}")
        return False

def process_job(job):
    """Process a single job from the queue."""
    job_id, submission_id, content_hash, content_type = job
    logger.info(f"Processing job {job_id} for submission {submission_id}")
    
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

# --- Main Worker Loop ---
if __name__ == "__main__":
    logger.info("Tier-5 worker started, polling PostgreSQL for jobs...")
    
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
            
            # Find a queued job
            cur.execute("""
                SELECT id, submission_id, content_hash, content_type
                FROM job_queue
                WHERE status = 'QUEUED'
                ORDER BY created_at ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            """)
            
            job = cur.fetchone()
            
            if job:
                job_id = job['id']
                logger.info(f"Found job {job_id}, marking as PROCESSING")
                
                # Mark as processing
                cur.execute("""
                    UPDATE job_queue
                    SET status = 'PROCESSING',
                        updated_at = NOW(),
                        attempts = attempts + 1
                    WHERE id = %s
                """, (job_id,))
                conn.commit()
                
                # Process the job
                success, error_msg = process_job(job)
                
                if success:
                    # Mark as completed
                    cur.execute("""
                        UPDATE job_queue
                        SET status = 'COMPLETED',
                            updated_at = NOW()
                        WHERE id = %s
                    """, (job_id,))
                else:
                    # Mark as failed with error
                    cur.execute("""
                        UPDATE job_queue
                        SET status = 'FAILED',
                            updated_at = NOW(),
                            last_error = %s
                        WHERE id = %s
                    """, (error_msg, job_id))
                    logger.error(f"Job {job_id} failed: {error_msg}")
                
                conn.commit()
                logger.info(f"Job {job_id} processing complete")
            else:
                # No jobs, sleep briefly
                time.sleep(3)
                
        except Exception as e:
            logger.error(f"Error in main loop: {e}")
            time.sleep(5)
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()
