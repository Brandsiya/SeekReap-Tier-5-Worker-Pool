#!/usr/bin/env python
# worker_pool.py - Concurrent-safe worker with FOR UPDATE SKIP LOCKED
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
        # Set isolation level to READ COMMITTED (default in PostgreSQL)
        # This ensures we see committed data only
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED)
        return conn
    except Exception as e:
        print(f"❌ Failed to connect to Neon: {e}")
        raise

def process_pending_tasks(conn):
    """Find and claim tasks using FOR UPDATE SKIP LOCKED"""
    tasks_processed = 0
    
    while True:
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        # 1. Try to claim ONE task exclusively using atomic UPDATE with subquery
        cur.execute("""
            UPDATE task_queue
            SET status = 'processing',
                updated_at = NOW()
            WHERE id = (
                SELECT id
                FROM task_queue
                WHERE status = 'pending'
                ORDER BY created_at ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            RETURNING id, task_type, payload, created_at;
        """)
        
        task = cur.fetchone()
        
        # If no tasks left, break the loop
        if not task:
            print("📭 No more pending tasks found.")
            cur.close()
            break

        # 2. Process the claimed task
        task_id = task['id']
        task_type = task['task_type']
        payload = task['payload']
        created_at = task['created_at']
        
        print(f"🔒 Claimed Task {task_id} ({task_type}) exclusively at {datetime.now().strftime('%H:%M:%S')}")
        print(f"📦 Payload: {json.dumps(payload)}")
        print(f"⏰ Created at: {created_at}")
        
        try:
            # Process based on task type
            if task_type == 'smoke_test':
                message = payload.get('message') or payload.get('msg', 'No message')
                print(f"💬 Processing: {message}")
                
                # Simulate varying work duration (1-3 seconds)
                # This helps demonstrate concurrent processing
                work_time = 1 + (task_id % 3)  # 1-3 seconds based on task ID
                print(f"⏳ Working for {work_time} seconds...")
                time.sleep(work_time)
                
                print(f"✅ Task {task_id} completed successfully")
                
                # Mark as completed
                cur.execute("""
                    UPDATE task_queue
                    SET status = 'completed',
                        completed_at = NOW()
                    WHERE id = %s
                """, (task_id,))
            
            elif task_type == 'error_test':
                # Test error handling
                raise Exception("Simulated error for testing")
            
            else:
                print(f"⚠️ Unknown task type: {task_type}")
                cur.execute("""
                    UPDATE task_queue
                    SET status = 'failed',
                        error_log = 'Unknown task type',
                        updated_at = NOW()
                    WHERE id = %s
                """, (task_id,))
            
        except Exception as e:
            # Task failed - mark as failed with error log
            error_msg = str(e)
            print(f"❌ Task {task_id} failed: {error_msg}")
            cur.execute("""
                UPDATE task_queue
                SET status = 'failed',
                    error_log = %s,
                    updated_at = NOW()
                WHERE id = %s
            """, (error_msg, task_id))
        
        # Commit the transaction for THIS task
        # This releases the lock and makes changes visible to other workers
        conn.commit()
        cur.close()
        tasks_processed += 1
        
        # Small delay to prevent tight loop if many tasks
        time.sleep(0.1)
    
    return tasks_processed

def main():
    start_time = time.time()
    execution_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    worker_id = os.getenv('CLOUD_RUN_TASK_INDEX', '0')
    
    print(f"🚀 Worker {worker_id} started at {execution_time}")
    print(f"🔧 Concurrent Worker Pool - FOR UPDATE SKIP LOCKED")
    print("=" * 60)
    
    try:
        # Connect to Neon
        conn = get_db_connection()
        
        # Test connection and show isolation level
        cur = conn.cursor()
        cur.execute("SHOW transaction_isolation;")
        isolation = cur.fetchone()
        print(f"📊 Transaction isolation: {isolation[0]}")
        
        cur.execute("SELECT version();")
        version = cur.fetchone()
        print(f"📊 Database version: {version[0][:50]}...")
        cur.close()
        
        # Process pending tasks with concurrent-safe logic
        tasks_processed = process_pending_tasks(conn)
        
        conn.close()
        
        elapsed = time.time() - start_time
        print("=" * 60)
        print(f"📊 Summary: Worker {worker_id} processed {tasks_processed} tasks")
        print(f"✅ Worker job completed successfully in {elapsed:.2f} seconds")
        sys.exit(0)
        
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"❌ Worker Error after {elapsed:.2f} seconds: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
