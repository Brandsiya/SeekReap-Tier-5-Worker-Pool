import time, httpx, redis, logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

r = redis.Redis(host="127.0.0.1", port=6379, db=0, decode_responses=True)
r.ping()
logger.info("Connected to Redis successfully")

def call_tier3(submission_id):
    resp = httpx.post("http://localhost:8000/api/analyze", json={
        "content_id": submission_id,
        "content_type": "youtube_video",
        "content_data": {"audio_similarity":0.8,"visual_similarity":0.6}
    })
    return resp.json()

def update_tier4(submission_id, analysis):
    resp = httpx.post("http://localhost:8081/api/finalize", json={
        "submission_id": submission_id,
        "analysis": analysis
    })
    logger.info(f"Updated Tier-4 for {submission_id}: {resp.text}")

if __name__ == "__main__":
    logger.info("Tier-5 worker started...")
    while True:
        job_id = r.rpop("jobs")
        if job_id:
            logger.info(f"Processing job {job_id}")
            analysis = call_tier3(job_id)
            update_tier4(job_id, analysis)
        else:
            time.sleep(5)
