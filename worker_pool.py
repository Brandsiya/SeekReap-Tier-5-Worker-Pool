import time
import httpx
import logging
import threading
import os
from http.server import HTTPServer, BaseHTTPRequestHandler
from google.cloud import pubsub_v1

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

# --- Pub/Sub Setup ---
PROJECT_ID = os.environ.get('PROJECT_ID', 'seekreap-production')
subscription_name = os.environ.get('PUBSUB_SUBSCRIPTION', 'seekreap-worker-sub')
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(PROJECT_ID, subscription_name)

# --- Tier URLs ---
TIER3_URL = os.environ.get('TIER3_URL', 'https://seekreap-tier3-tif2gmgi4q-uc.a.run.app')
TIER4_URL = os.environ.get('TIER4_URL', 'https://seekreap-tier4-tif2gmgi4q-uc.a.run.app')

# --- Core Worker Functions ---
def call_tier3(submission_id):
    try:
        url = f"{TIER3_URL}/api/analyze"
        resp = httpx.post(url, json={
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
        url = f"{TIER4_URL}/api/finalize"
        resp = httpx.post(url, json={
            "submission_id": submission_id,
            "analysis": analysis
        }, timeout=30.0)
        resp.raise_for_status()
        logger.info(f"Updated Tier-4 for {submission_id}: {resp.text}")
    except Exception as e:
        logger.error(f"Error updating Tier-4 for {submission_id}: {e}")

def process_message(message):
    """Process a single Pub/Sub message."""
    try:
        submission_id = message.data.decode('utf-8')
        logger.info(f"Processing job {submission_id}")
        
        # Call Tier-3 for analysis
        analysis = call_tier3(submission_id)
        
        # Update Tier-4 with results
        if "error" not in analysis:
            update_tier4(submission_id, analysis)
        else:
            logger.error(f"Skipping Tier-4 update for {submission_id} due to Tier-3 error.")
        
        # Acknowledge the message
        message.ack()
        logger.info(f"Successfully processed and acknowledged job {submission_id}")
        
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        # Negative acknowledgement - message will be redelivered
        message.nack()

# --- Main Worker Loop ---
if __name__ == "__main__":
    logger.info("Tier-5 worker started, listening for Pub/Sub messages...")
    logger.info(f"Subscribing to: {subscription_path}")
    
    # Start listening for messages
    streaming_pull_future = subscriber.subscribe(
        subscription_path, 
        callback=process_message
    )
    
    try:
        # Keep the main thread alive
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
    except Exception as e:
        logger.error(f"Error in subscriber: {e}")
        streaming_pull_future.cancel()
    
    logger.info("Worker stopped")
