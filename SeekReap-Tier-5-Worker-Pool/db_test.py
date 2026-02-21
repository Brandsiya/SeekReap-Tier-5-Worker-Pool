import os
import psycopg2
from psycopg2.extras import RealDictCursor

def main():
    try:
        # Get DATABASE_URL from environment
        database_url = os.environ.get('DATABASE_URL')
        if not database_url:
            raise ValueError("DATABASE_URL environment variable is not set!")

        # Connect to the database
        with psycopg2.connect(database_url, cursor_factory=RealDictCursor) as conn:
            with conn.cursor() as cur:
                # Fetch last 5 jobs
                cur.execute("""
                    SELECT job_id, content_id, status, started_at, completed_at
                    FROM job_queue
                    ORDER BY job_id DESC
                    LIMIT 5;
                """)
                rows = cur.fetchall()

                print("Last 5 jobs in job_queue:")
                for row in rows:
                    print(row)

        print("✅ Connection successful and query executed!")

    except Exception as e:
        print("❌ Error:", e)

if __name__ == "__main__":
    main()
