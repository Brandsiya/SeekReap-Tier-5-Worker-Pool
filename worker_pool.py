#!/usr/bin/env python3
import psycopg2
import requests
import time
import signal
import sys
from multiprocessing import Process, current_process
from contextlib import closing

# --- Config ---
DB_CONFIG = {
    'dbname': 'seekreap_neon_db',
    'user': 'neondb_owner',
    'password': 'npg_5KSxRpgkzN7D',
    'host': 'ep-rapid-base-ai27r1sa-pooler.c-4.us-east-1.aws.neon.tech',
    'port': 5432
}

TIER3_URL = 'https://seekreap-tier-3-private.onrender.com/verify'
TIER4_URL = 'https://seekreap-tier-4-orchestrator-nrn4.onrender.com/process'

NUM_WORKERS = 3
FETCH_SLEEP = 1  # seconds if no jobs available
MAX_RETRIES = 3

# --- Graceful shutdown ---
running = True
def shutdown_handler(sig, frame):
    global running
    print(f"{current_process().name} received shutdown signal")
    running = False

signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)

# --- Worker functions ---
def fetch_job(conn):
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE job_queue
            SET status = 'in_progress', updated_at = NOW()
            WHERE job_id = (
                SELECT job_id
                FROM job_queue
                WHERE status = 'pending' AND retry_count < %s
                ORDER BY created_at
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            RETURNING job_id, content_id, retry_count;
        """, (MAX_RETRIES,))
        return cur.fetchone()

def call_tier3_api(content_id):
    resp = requests.post(TIER3_URL, json={'content_id': content_id})
    resp.raise_for_status()
    return resp.json()

def call_tier4_api(tier3_result):
    resp = requests.post(TIER4_URL, json=tier3_result)
    resp.raise_for_status()
    return resp.json()

def process_job(conn, job):
    job_id, content_id, retry_count = job
    try:
        t3_result = call_tier3_api(content_id)
        t4_result = call_tier4_api(t3_result)
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE job_queue
                SET status = 'done', updated_at = NOW()
                WHERE job_id = %s
            """, (job_id,))
        conn.commit()
        print(f"{current_process().name}: Job {job_id} done")
    except Exception as e:
        with conn.cursor() as cur:
            if retry_count + 1 >= MAX_RETRIES:
                cur.execute("""
                    UPDATE job_queue
                    SET status = 'failed', updated_at = NOW()
                    WHERE job_id = %s
                """, (job_id,))
                print(f"{current_process().name}: Job {job_id} failed permanently")
            else:
                cur.execute("""
                    UPDATE job_queue
                    SET status = 'pending', retry_count = retry_count + 1, updated_at = NOW()
                    WHERE job_id = %s
                """, (job_id,))
                print(f"{current_process().name}: Job {job_id} re-queued (retry {retry_count + 1})")
        conn.commit()

def worker_loop():
    global running
    conn = None
    while running:
        try:
            if not conn:
                conn = psycopg2.connect(**DB_CONFIG)
            job = fetch_job(conn)
            if job:
                process_job(conn, job)
            else:
                time.sleep(FETCH_SLEEP)
        except psycopg2.OperationalError as e:
            print(f"{current_process().name}: DB connection lost: {e}, reconnecting...")
            if conn:
                conn.close()
            conn = None
            time.sleep(2)
    if conn:
        conn.close()
    print(f"{current_process().name} exiting")
# --- Start worker pool ---
if __name__ == "__main__":
    processes = []
    for i in range(NUM_WORKERS):
        p = Process(target=worker_loop, name=f"Worker-{i+1}")
        p.start()
        processes.append(p)

    for p in processes:
        p.join()
