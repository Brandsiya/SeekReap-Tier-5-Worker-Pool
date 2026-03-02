import os
import time
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')
# Ensure this matches your Render URL for Tier-4
TIER4_URL = os.getenv('TIER4_URL', 'https://seekreap-tier-4-orchestrator.onrender.com')

def start_worker():
    print("🐍 Tier-5 Python Worker: Monitoring Clean Schema")
    while True:
        conn = None
        try:
            conn = psycopg2.connect(DATABASE_URL)
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            # Atomic fetch-and-lock
            cur.execute("""
                SELECT job_id, job_type FROM submissions 
                WHERE status = 'pending' 
                ORDER BY created_at ASC LIMIT 1 
                FOR UPDATE SKIP LOCKED
            """)
            job = cur.fetchone()
            
            if job:
                jid = job['job_id']
                print(f"🚀 Found Job {jid}: {job['job_type']}")
                
                cur.execute("UPDATE submissions SET status = 'processing', started_at = NOW() WHERE job_id = %s", (jid,))
                conn.commit()
                
                time.sleep(2) # Work simulation
                
                cur.execute("UPDATE submissions SET status = 'completed', completed_at = NOW() WHERE job_id = %s", (jid,))
                conn.commit()
                print(f"✅ Finished Job {jid}")
                
                # Notify Tier-4
                try:
                    requests.post(f"{TIER4_URL}/api/job-update", json={"job_id": jid, "status": "completed"}, timeout=5)
                except:
                    print("⚠️ Tier-4 unreachable")
            else:
                conn.rollback()
                time.sleep(5)
                
            cur.close()
            conn.close()
        except Exception as e:
            if conn: conn.close()
            print(f"❌ Worker Error: {e}")
            time.sleep(10)

if __name__ == "__main__":
    start_worker()
