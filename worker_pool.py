#!/usr/bin/env python3
import os
import time
import signal
import sys
import psycopg2
from contextlib import closing
from urllib.parse import urlparse

# Graceful shutdown
stop_signal = False
def shutdown_handler(signum, frame):
    global stop_signal
    stop_signal = True
signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)

# Load database URL from environment
DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    print("DATABASE_URL environment variable not set")
    sys.exit(1)

# Parse DATABASE_URL for psycopg2
url = urlparse(DATABASE_URL)
DB_PARAMS = {
    "dbname": url.path[1:],   # remove leading slash
    "user": url.username,
    "password": url.password,
    "host": url.hostname,
    "port": url.port,
    "sslmode": "require"
}

def process_job(job):
    job_id, content_id = job
    print(f"Processing job {job_id} ({content_id})")
    with closing(psycopg2.connect(**DB_PARAMS)) as conn:
        with conn.cursor() as cur:
            # Mark started
            cur.execute(
                "UPDATE job_queue SET status='running', started_at=NOW() WHERE job_id=%s",
                (job_id,)
            )
            conn.commit()

            # Simulate job processing (replace with real logic)
            time.sleep(2)

            # Mark completed
            cur.execute(
                "UPDATE job_queue SET status='completed', completed_at=NOW() WHERE job_id=%s",
                (job_id,)
            )
            conn.commit()
            print(f"Completed job {job_id}")

def main():
    print("Worker pool started")
    while not stop_signal:
        try:
            with closing(psycopg2.connect(**DB_PARAMS)) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT job_id, content_id FROM job_queue WHERE status='pending' ORDER BY job_id ASC LIMIT 1 FOR UPDATE SKIP LOCKED"
                    )
                    job = cur.fetchone()
                    if job:
                        process_job(job)
                    else:
                        time.sleep(1)
        except Exception as e:
            print(f"DB connection lost or error: {e}")
            time.sleep(2)

    print("Worker pool exiting")

if __name__ == "__main__":
    main()
