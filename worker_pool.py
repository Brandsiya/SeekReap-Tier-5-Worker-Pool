#!/usr/bin/env python
# worker_pool.py - Runs as a Cloud Run Job with Neon PostgreSQL
import os
import sys
import time
import json
import psycopg2
import psycopg2.extras
from datetime import datetime
from dotenv import load_dotenv

def get_db_connection():
    """Get database connection to Neon PostgreSQL"""
    try:
        host = os.getenv('DB_HOST')
        port = os.getenv('DB_PORT')
        database = os.getenv('DB_NAME')
        user = os.getenv('DB_USER')
        password = os.getenv('DB_PASSWORD')
        sslmode = os.getenv('DB_SSLMODE', 'require')
        
        print(f"🔌 Connecting to Neon at {host}:{port}/{database}")
        
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            sslmode=sslmode,
            connect_timeout=30
        )
        return conn
    except Exception as e:
        print(f"❌ Failed to connect to Neon: {e}")
        raise

def process_pending_tasks(conn):
    """Find and process pending tasks"""
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    # Get all pending tasks
    cur.execute("""
        SELECT id, task_type, payload, created_at
        FROM task_queue
        WHERE status = 'pending'
        ORDER BY created_at ASC
        LIMIT 10
    """)
    
    tasks = cur.fetchall()
    print(f"📋 Found {len(tasks)} pending tasks")
    
    for task in tasks:
        task_id = task['id']
        task_type = task['task_type']
        payload = task['payload']
        created_at = task['created_at']
        
        print(f"🚀 Processing Task {task_id} ({task_type})")
        print(f"📦 Payload received: {json.dumps(payload)}")
        print(f"⏰ Created at: {created_at}")
        
        try:
            # SIMULATE WORK - In production, replace with actual logic
            # For smoke_test, we just verify the payload
            if task_type == 'smoke_test':
                message = payload.get('message', 'No message')
                print(f"💬 Smoke test message: {message}")
                
                # Verify the message contains expected text
                if "worker is alive" in message:
                    print(f"✅ Smoke test passed!")
                else:
                    print(f"⚠️ Smoke test message format unexpected")
            
            # SIMULATE WORK DURATION (2 seconds)
            print(f"⏳ Working on task...")
            time.sleep(2)
            
            # Mark task as completed
            cur.execute("""
                UPDATE task_queue
                SET status = 'completed',
                    updated_at = NOW(),
                    completed_at = NOW()
                WHERE id = %s
            """, (task_id,))
            
            print(f"✅ Task {task_id} completed successfully")
            
        except Exception as e:
            # Mark task as failed
            error_msg = str(e)
            cur.execute("""
                UPDATE task_queue
                SET status = 'failed',
                    error_log = %s,
                    updated_at = NOW()
                WHERE id = %s
            """, (error_msg, task_id))
            
            print(f"❌ Task {task_id} failed: {error_msg}")
        
        conn.commit()
    
    cur.close()
    return len(tasks)

def main():
    start_time = time.time()
    execution_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"🚀 Worker job started at {execution_time}")
    print(f"🔧 Worker Pool - Task Processor")
    print("=" * 50)
    
    try:
        # Connect to Neon
        conn = get_db_connection()
        
        # Test connection
        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()
        print(f"📊 Database version: {version[0][:50]}...")
        cur.close()
        
        # Process pending tasks
        tasks_processed = process_pending_tasks(conn)
        
        conn.close()
        
        elapsed = time.time() - start_time
        print("=" * 50)
        print(f"📊 Summary: Processed {tasks_processed} tasks")
        print(f"✅ Worker job completed successfully in {elapsed:.2f} seconds")
        sys.exit(0)
        
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"❌ Worker Error after {elapsed:.2f} seconds: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
