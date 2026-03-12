"""
SeekReap Tier-5 — Track B audio fingerprinting.
Downloads YouTube audio via yt-dlp, computes chromaprint via fpcalc,
compares against fingerprints table, updates the row.
"""
import os, subprocess, tempfile, logging, json

logger = logging.getLogger(__name__)

DB_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://neondb_owner:npg_yX7aHMwIqQC4@ep-rapid-base-ai27r1sa-pooler.c-4.us-east-1.aws.neon.tech:5432/seekreap_neon_db?sslmode=require"
)

MATCH_THRESHOLD = 0.85


def _download_audio(content_url: str, out_dir: str) -> str:
    """Download first 120s of audio-only stream. Returns path to audio file."""
    out_template = os.path.join(out_dir, "audio.%(ext)s")
    cmd = [
        "yt-dlp",
        "--no-playlist",
        "--format", "worstaudio/bestaudio",   # smallest file = fastest download
        "--output", out_template,
        "--quiet",
        "--no-warnings",
        "--socket-timeout", "20",             # fail fast on stalled connections
        "--extractor-retries", "1",           # don't retry extractor on failure
        "--retries", "2",                     # 2 network retries max
        "--download-sections", "*0-120",      # only first 120s — enough for fpcalc
        "--no-part",                          # write directly, no .part temp files
        content_url,
    ]
    subprocess.run(cmd, check=True, timeout=90)   # bumped to 90s overall
    for f in os.listdir(out_dir):
        if f.startswith("audio."):
            return os.path.join(out_dir, f)
    raise FileNotFoundError("yt-dlp did not produce an audio file")


def _compute_chromaprint(audio_path: str):
    """Run fpcalc and return (fingerprint_str, duration_float)."""
    result = subprocess.run(
        ["fpcalc", "-json", audio_path],
        capture_output=True, text=True, timeout=30, check=True
    )
    data = json.loads(result.stdout)
    return data["fingerprint"], float(data["duration"])


def _bic_similarity(fp1: str, fp2: str) -> float:
    """
    Rough similarity between two chromaprint strings.
    Compares character overlap as a proxy (good enough for duplicate detection).
    """
    if not fp1 or not fp2:
        return 0.0
    min_len = min(len(fp1), len(fp2))
    if min_len == 0:
        return 0.0
    matches = sum(a == b for a, b in zip(fp1[:min_len], fp2[:min_len]))
    return round(matches / min_len, 4)


def run_audio_fingerprint(submission_id: str, creator_id: str, content_url: str) -> dict:
    """
    Main entry point called from Tier-5 worker.
    Downloads audio, computes chromaprint, compares against DB,
    updates fingerprints row for this submission.
    Returns result dict.
    """
    result = {
        "audio_fingerprint":      None,
        "audio_duration":         None,
        "audio_similarity_score": 0.0,
        "closest_audio_match_id": None,
        "audio_stored":           False,
        "error":                  None,
    }

    # 1. Download audio to temp dir
    try:
        with tempfile.TemporaryDirectory() as tmp:
            audio_path = _download_audio(content_url, tmp)

            # 2. Compute chromaprint
            fp_str, duration = _compute_chromaprint(audio_path)
            result["audio_fingerprint"] = fp_str
            result["audio_duration"]    = duration

    except subprocess.TimeoutExpired:
        result["error"] = "Audio download/fingerprint timed out"
        logger.warning("Audio timeout for %s", submission_id)
        return result
    except Exception as e:
        result["error"] = f"Audio processing failed: {e}"
        logger.warning("Audio processing error for %s: %s", submission_id, e)
        return result

    # 3. Compare + store in DB
    try:
        import psycopg2
        conn = psycopg2.connect(DB_URL)
        cur  = conn.cursor()

        import uuid as _uv
        try:
            _uv.UUID(submission_id)
            cur.execute(
                "SELECT id, content_url, audio_fingerprint FROM fingerprints "
                "WHERE audio_fingerprint IS NOT NULL AND submission_id::text != %s",
                (submission_id,)
            )
        except (ValueError, AttributeError):
            cur.execute(
                "SELECT id, content_url, audio_fingerprint FROM fingerprints "
                "WHERE audio_fingerprint IS NOT NULL"
            )

        rows = cur.fetchall()
        best_score = 0.0
        best_row   = None
        for row in rows:
            score = _bic_similarity(fp_str, row[2])
            if score > best_score:
                best_score = score
                best_row   = row

        if best_row and best_score >= MATCH_THRESHOLD:
            result["audio_similarity_score"] = best_score
            result["closest_audio_match_id"] = str(best_row[0])

        cur.execute(
            """
            UPDATE fingerprints
               SET audio_fingerprint = %s,
                   audio_duration    = %s
             WHERE submission_id = %s
            """,
            (fp_str, duration, submission_id)
        )
        if cur.rowcount == 0:
            cur.execute(
                """
                INSERT INTO fingerprints
                    (submission_id, creator_id, content_url, audio_fingerprint, audio_duration, fingerprint_version)
                VALUES (%s, %s, %s, %s, %s, 'audio-v1')
                ON CONFLICT DO NOTHING
                """,
                (submission_id, creator_id, content_url, fp_str, duration)
            )

        conn.commit()
        cur.close()
        conn.close()
        result["audio_stored"] = True
        logger.info("Audio fingerprint stored for %s (duration=%.1fs)", submission_id, duration)

    except Exception as e:
        logger.error("DB audio fingerprint failed for %s: %s", submission_id, e)
        result["error"] = f"DB error: {e}"

    return result
