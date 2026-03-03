#!/usr/bin/env python
# worker_pool.py - Runs as a Cloud Run Job with Neon PostgreSQL
import os
import sys
import time
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

def get_db_connection():
    """Get database connection to Neon PostgreSQL"""
    try:
        # Get connection details from environment variables
        host = os.getenv('DB_HOST', 'ep-rapid-base-ai27r1sa-pooler.c-4.us-east-1.aws.neon.tech')
        port = os.getenv('DB_PORT', '5432')
        database = os.getenv('DB_NAME', 'seekreap_neon_db')
        user = os.getenv('DB_USER', 'neondb_owner')
        password = os.getenv('DB_PASSWORD', 'npg_yX7aHMwIqQC4')
        sslmode = os.getenv('DB_SSLMODE', 'require')
        
        print(f"🔌 Connecting to Neon at {host}:{port}/{database}")
        
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            sslmode=sslmode,
            connect_timeout=30,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5
        )
        print("✅ Successfully connected to Neon PostgreSQL")
        return conn
    except Exception as e:
        print(f"❌ Failed to connect to Neon: {e}")
        raise

def main():
    start_time = time.time()
    print(f"🚀 Worker job started at {time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Load environment variables (optional, since we set them in gcloud)
        load_dotenv()
        
        # Connect to Neon
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        # Test the connection
        cur.execute("SELECT version();")
        version = cur.fetchone()
        print(f"📊 Database version: {version[0][:50]}...")
        
        # Get list of tables
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        tables = cur.fetchall()
        print(f"📋 Found {len(tables)} tables in database")
        
        if tables:
            print(f"   Tables: {', '.join([t[0] for t in tables[:5]])}")
            if len(tables) > 5:
                print(f"   ... and {len(tables) - 5} more")
        
        # TODO: Add your actual worker tasks here
        # Example: Check for pending jobs
        # cur.execute("SELECT COUNT(*) FROM your_table WHERE status = 'pending'")
        # count = cur.fetchone()[0]
        # print(f"⏳ Pending jobs: {count}")
        
        cur.close()
        conn.close()
        
        elapsed = time.time() - start_time
        print(f"✅ Worker job completed successfully in {elapsed:.2f} seconds")
        sys.exit(0)
        
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"❌ Worker Error after {elapsed:.2f} seconds: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
