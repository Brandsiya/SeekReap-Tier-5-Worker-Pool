import threading
import http.server
import socketserver
import os
import time
import psycopg2
from psycopg2.extras import RealDictCursor

DB_URL = os.environ.get("DATABASE_URL")

def run_health_server():
    port = int(os.environ.get("PORT", 8080))
    handler = http.server.SimpleHTTPRequestHandler
    with socketserver.TCPServer(("", port), handler) as httpd:
        print(f"✅ Health server online on port {port}", flush=True)
        httpd.serve_forever()

def process_tasks():
    print("🚀 SeekReap Worker Tier-5: Initializing task processor...", flush=True)
    while True:
        try:
            conn = psycopg2.connect(DB_URL)
            # Use RealDictCursor to handle JSON data easily
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            # 1. Claim a task (Atomic update to prevent race conditions)
            cur.execute("""
                UPDATE video_patterns 
                SET status = 'processing', updated_at = CURRENT_TIMESTAMP 
                WHERE id = (
                    SELECT id FROM video_patterns 
                    WHERE status = 'pending' 
                    ORDER BY created_at ASC 
                    FOR UPDATE SKIP LOCKED 
                    LIMIT 1
                ) RETURNING id, video_id;
            """)
            
            task = cur.fetchone()
            
            if task:
                print(f"📦 [WORKER] Processing Video: {task['video_id']} (ID: {task['id']})", flush=True)
                # Simulate analysis (e.g., scanning audio/video patterns)
                time.sleep(10) 
                
                # 2. Mark as completed
                cur.execute("UPDATE video_patterns SET status = 'completed' WHERE id = %s", (task['id'],))
                print(f"✔️ [WORKER] Successfully analyzed: {task['video_id']}", flush=True)
            
            conn.commit()
            cur.close()
            conn.close()
            
        except Exception as e:
            print(f"❌ [WORKER] Error during polling: {e}", flush=True)
        
        time.sleep(15) # Wait before next poll

if __name__ == "__main__":
    # Start health check
    threading.Thread(target=run_health_server, daemon=True).start()
    # Start processing loop
    process_tasks()
