import time
import httpx
import logging
import threading
import os
import base64
import struct
import psycopg2
import psycopg2.extras
from http.server import HTTPServer, BaseHTTPRequestHandler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Health Check Server ---
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")
    def log_message(self, format, *args):
        pass  # suppress noisy access logs

def run_health_server():
    port = int(os.environ.get('PORT', 8080))
    server = HTTPServer(("0.0.0.0", port), Handler)
    logger.info(f"Health check server listening on port {port}")
    server.serve_forever()

threading.Thread(target=run_health_server, daemon=True).start()
logger.info("Health check server started in background thread")

# --- Database Connection ---
def get_db():
    return psycopg2.connect(os.environ.get('DATABASE_URL'))

# --- Config ---
TIER3_URL = os.environ.get('TIER3_URL', 'https://seekreap-tier-3-private-10.onrender.com')
TIER4_URL = os.environ.get('TIER4_URL', 'https://seekreap-tier-4-orchestrator-1.onrender.com')
MAX_RETRIES = 3


# --- Chromaprint similarity ---
def chromaprint_similarity(fp1: str, fp2: str) -> float:
    """
    Compare two Chromaprint fingerprints (base64url strings).
    Returns similarity 0.0–1.0 (1.0 = identical).
    Uses bit-level Hamming distance on the decoded 32-bit integer arrays.
    """
    try:
        # Add padding and decode base64url
        def decode(fp):
            pad = (4 - len(fp) % 4) % 4
            return base64.urlsafe_b64decode(fp + '=' * pad)

        b1, b2 = decode(fp1), decode(fp2)
        # Trim to same length (multiples of 4 bytes = 32-bit ints)
        n = min(len(b1), len(b2)) & ~3
        if n == 0:
            return 0.0
        ints1 = struct.unpack(f'<{n//4}I', b1[:n])
        ints2 = struct.unpack(f'<{n//4}I', b2[:n])
        total_bits = n * 8
        differing = sum(bin(a ^ b).count('1') for a, b in zip(ints1, ints2))
        return 1.0 - (differing / total_bits)
    except Exception as e:
        logger.warning(f"Fingerprint comparison error: {e}")
        return 0.0


# --- Audio fingerprint from Tier-3 ---
def get_audio_fingerprint(content_url: str):
    """
    Call Tier-3 /internal/audio-fingerprint.
    Returns {"fingerprint": "...", "duration": 120.0} or {"error": "..."}
    """
    try:
        resp = httpx.post(
            f"{TIER3_URL}/internal/audio-fingerprint",
            json={"content_url": content_url},
            timeout=240.0  # ffmpeg pipeline can take ~2min
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"Audio fingerprint error for {content_url}: {e}")
        return {"error": str(e)}


# --- Look up existing fingerprints for similarity ---
def find_best_match(conn, fingerprint: str, exclude_submission_id: str = None):
    """
    Query fingerprints table for the most similar existing fingerprint.
    Returns (best_similarity, best_submission_id) or (0.0, None).
    """
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("""
            SELECT submission_id, audio_fingerprint
            FROM fingerprints
            WHERE audio_fingerprint IS NOT NULL
              AND submission_id != %s
            ORDER BY created_at DESC
            LIMIT 100
        """, (exclude_submission_id or '00000000-0000-0000-0000-000000000000',))
        rows = cur.fetchall()
        cur.close()

        best_sim = 0.0
        best_id = None
        for row in rows:
            sim = chromaprint_similarity(fingerprint, row['audio_fingerprint'])
            if sim > best_sim:
                best_sim = sim
                best_id = str(row['submission_id'])

        return best_sim, best_id
    except Exception as e:
        logger.error(f"Fingerprint lookup error: {e}")
        return 0.0, None


# --- Store fingerprint ---
def store_fingerprint(conn, submission_id, creator_id, content_url, fingerprint, duration, thumbnail_url):
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO fingerprints
                (submission_id, creator_id, content_url, audio_fingerprint,
                 audio_duration, thumbnail_url, fingerprint_version)
            VALUES (%s, %s, %s, %s, %s, %s, 'chromaprint-v1')
            ON CONFLICT DO NOTHING
        """, (submission_id, creator_id, content_url, fingerprint, duration, thumbnail_url))
        conn.commit()
        cur.close()
        logger.info(f"Stored fingerprint for submission {submission_id}")
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to store fingerprint: {e}")


# --- Store match if similarity above threshold ---
def store_match(conn, submission_id, matched_submission_id, similarity_score, fingerprint_version='chromaprint-v1'):
    """Write to content_matches when similarity >= 0.85."""
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO content_matches
                (submission_id, matched_submission_id, similarity_score,
                 match_type, fingerprint_version)
            VALUES (%s, %s, %s, 'audio', %s)
            ON CONFLICT (submission_id, matched_submission_id, match_type) DO UPDATE
                SET similarity_score = EXCLUDED.similarity_score,
                    detected_at = NOW()
        """, (submission_id, matched_submission_id, similarity_score, fingerprint_version))
        conn.commit()
        cur.close()
        logger.info(f"Match stored: {submission_id[:8]}... ~ {matched_submission_id[:8]}... score={similarity_score:.3f}")
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to store match: {e}")


# --- Finalize via Tier-4 ---
def update_tier4(submission_id, analysis):
    try:
        resp = httpx.post(
            f"{TIER4_URL}/api/finalize",
            json={"submission_id": submission_id, "analysis": analysis},
            timeout=60.0
        )
        resp.raise_for_status()
        logger.info(f"Tier-4 finalize: {resp.status_code} - {resp.text}")
        return True
    except Exception as e:
        logger.error(f"Tier-4 finalize error for {submission_id}: {e}")
        if hasattr(e, 'response') and e.response:
            logger.error(f"Response: {e.response.status_code} {e.response.text}")
        return False


# --- Process a single job ---
def process_job(job):
    job_id = job['job_id']
    submission_id = job['submission_id']
    attempts = job['attempts']
    logger.info(f"Processing job {job_id} for submission {submission_id} (attempt {attempts + 1})")

    conn = None
    cur = None
    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # Fetch submission details
        cur.execute("""
            SELECT content_url, content_hash, content_type,
                   creator_id, content_preview_url
            FROM submissions WHERE id = %s
        """, (submission_id,))
        sub = cur.fetchone()
        if not sub:
            logger.error(f"Submission {submission_id} not found")
            return False, "Submission not found"

        content_url = sub['content_url']
        creator_id = str(sub['creator_id'])
        thumbnail_url = sub['content_preview_url'] or ''

        # Step 1: Get audio fingerprint from Tier-3
        logger.info(f"Requesting audio fingerprint for {content_url}")
        fp_result = get_audio_fingerprint(content_url)

        if "error" in fp_result:
            logger.warning(f"Audio fingerprint failed: {fp_result['error']} — falling back to dummy score")
            audio_similarity = 0.0
            fingerprint = None
            duration = None
        else:
            fingerprint = fp_result.get("fingerprint")
            duration = fp_result.get("duration")
            logger.info(f"Got fingerprint (duration={duration}s)")

            # Step 2: Compare against existing fingerprints
            audio_similarity, matched_id = find_best_match(conn, fingerprint, submission_id)
            logger.info(f"Best audio match: similarity={audio_similarity:.3f} matched_id={matched_id}")

            # Step 2b: Store match record if above threshold
            MATCH_THRESHOLD = 0.85
            if audio_similarity >= MATCH_THRESHOLD and matched_id:
                store_match(conn, submission_id, matched_id, audio_similarity)
                logger.info(f"⚠️  MATCH DETECTED: similarity={audio_similarity:.3f} >= {MATCH_THRESHOLD}")

            # Step 3: Store fingerprint
            store_fingerprint(conn, submission_id, creator_id, content_url,
                              fingerprint, duration, thumbnail_url)

        # Step 4: Call Tier-3 /api/analyze with real similarity score
        try:
            analyze_resp = httpx.post(
                f"{TIER3_URL}/api/analyze",
                json={
                    "content_id": submission_id,
                    "content_hash": sub['content_hash'],
                    "content_type": sub['content_type'],
                    "content_data": {
                        "audio_similarity": round(audio_similarity, 4),
                        "visual_similarity": 0.0,
                        "flags": ["audio_match"] if audio_similarity > 0.8 else []
                    }
                },
                timeout=60.0
            )
            analyze_resp.raise_for_status()
            analysis = analyze_resp.json()
            logger.info(f"Analysis: score={analysis.get('risk_score')} level={analysis.get('risk_level')}")
        except Exception as e:
            logger.error(f"Tier-3 analyze error: {e}")
            return False, str(e)

        # Step 5: Finalize via Tier-4
        if "error" not in analysis:
            success = update_tier4(submission_id, analysis)
            return (True, None) if success else (False, "Tier-4 update failed")
        else:
            return False, analysis.get("error")

    except Exception as e:
        logger.error(f"process_job error: {e}")
        import traceback; traceback.print_exc()
        return False, str(e)
    finally:
        if cur: cur.close()
        if conn: conn.close()


# --- Main Worker Loop ---
if __name__ == "__main__":
    logger.info("Tier-5 worker started, polling PostgreSQL for jobs...")
    logger.info(f"TIER3_URL: {TIER3_URL}")
    logger.info(f"TIER4_URL: {TIER4_URL}")

    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        conn.close()
        logger.info("Database connection successful")
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        exit(1)

    while True:
        conn = None
        cur = None
        try:
            conn = get_db()
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

            cur.execute("""
                SELECT job_id, submission_id, attempts
                FROM job_queue
                WHERE status = 'pending'
                ORDER BY created_at ASC
                LIMIT 5
                FOR UPDATE SKIP LOCKED
            """)
            jobs = cur.fetchall()

            if jobs:
                for job in jobs:
                    job_id = job['job_id']
                    attempts = job['attempts']
                    logger.info(f"Found job {job_id} (attempt {attempts + 1}), marking as processing")

                    cur.execute("""
                        UPDATE job_queue
                        SET status = 'processing', attempts = attempts + 1
                        WHERE job_id = %s
                    """, (job_id,))
                    conn.commit()

                    success, error_msg = process_job(job)

                    if success:
                        cur.execute("UPDATE job_queue SET status = 'completed' WHERE job_id = %s", (job_id,))
                        logger.info(f"Job {job_id} completed successfully")
                    else:
                        new_attempts = attempts + 1
                        if new_attempts >= MAX_RETRIES:
                            cur.execute("UPDATE job_queue SET status = 'failed' WHERE job_id = %s", (job_id,))
                            logger.error(f"Job {job_id} failed permanently: {error_msg}")
                        else:
                            cur.execute("UPDATE job_queue SET status = 'pending' WHERE job_id = %s", (job_id,))
                            logger.warning(f"Job {job_id} failed (attempt {new_attempts}), will retry: {error_msg}")

                    conn.commit()
            else:
                logger.debug("No pending jobs, sleeping...")
                time.sleep(3)

        except Exception as e:
            logger.error(f"Main loop error: {e}")
            import traceback; traceback.print_exc()
            time.sleep(5)
        finally:
            if cur: cur.close()
            if conn: conn.close()
