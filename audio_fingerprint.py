"""
SeekReap Tier-5 — Track B audio fingerprinting.
Uses yt-dlp --get-url to fetch the direct stream URL, then ffmpeg -t 120
to download only the first 2 minutes. fpcalc only needs ~60s for a reliable fingerprint.
"""
import os, subprocess, tempfile, logging, json

logger = logging.getLogger(__name__)

DB_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://neondb_owner:npg_yX7aHMwIqQC4@ep-rapid-base-ai27r1sa-pooler.c-4.us-east-1.aws.neon.tech:5432/seekreap_neon_db?sslmode=require"
)

MATCH_THRESHOLD = 0.85


def _get_audio_stream_url(content_url: str) -> str:
    """Use yt-dlp --get-url to extract the direct audio stream URL (no download)."""
    result = subprocess.run(
        [
            "yt-dlp",
            "--no-playlist",
            "--format", "worstaudio/bestaudio",
            "--get-url",
            "--no-warnings",
            "--socket-timeout", "15",
            "--extractor-retries", "1",
            content_url,
        ],
        capture_output=True, text=True, timeout=30
    )
    if result.returncode != 0:
        raise RuntimeError(f"yt-dlp --get-url failed: {result.stderr.strip()[:200]}")
    stream_url = result.stdout.strip().splitlines()[0]
    if not stream_url:
        raise RuntimeError("yt-dlp returned empty stream URL")
    logger.info("Got stream URL (len=%d)", len(stream_url))
    return stream_url


def _download_audio_ffmpeg(stream_url: str, out_path: str) -> None:
    """Use ffmpeg to download exactly 120s of audio from the direct stream URL."""
    subprocess.run(
        [
            "ffmpeg",
            "-y",                    # overwrite output
            "-t", "120",             # stop after 120 seconds — hard cutoff
            "-i", stream_url,        # direct stream URL from yt-dlp
            "-vn",                   # no video
            "-acodec", "copy",       # copy audio stream (no re-encode = fast)
            "-loglevel", "error",
            out_path,
        ],
        check=True, timeout=60       # ffmpeg with -t 120 should finish in well under 60s
    )


def _compute_chromaprint(audio_path: str):
    """Run fpcalc and return (fingerprint_str, duration_float)."""
    result = subprocess.run(
        ["fpcalc", "-json", audio_path],
        capture_output=True, text=True, timeout=30, check=True
    )
    data = json.loads(result.stdout)
    return data["fingerprint"], float(data["duration"])


def _bic_similarity(fp1: str, fp2: str) -> float:
    if not fp1 or not fp2:
        return 0.0
    min_len = min(len(fp1), len(fp2))
    if min_len == 0:
        return 0.0
    matches = sum(a == b for a, b in zip(fp1[:min_len], fp2[:min_len]))
    return round(matches / min_len, 4)


def run_audio_fingerprint(submission_id: str, creator_id: str, content_url: str) -> dict:
    result = {
        "audio_fingerprint":      None,
        "audio_duration":         None,
        "audio_similarity_score": 0.0,
        "closest_audio_match_id": None,
        "audio_stored":           False,
        "error":                  None,
    }

    try:
        with tempfile.TemporaryDirectory() as tmp:
            out_path = os.path.join(tmp, "audio.m4a")

            # Step 1: get direct stream URL (fast — no download, just metadata)
            logger.info("Fetching stream URL for %s", submission_id)
            stream_url = _get_audio_stream_url(content_url)

            # Step 2: download first 120s via ffmpeg
            logger.info("Downloading 120s of audio for %s", submission_id)
            _download_audio_ffmpeg(stream_url, out_path)

            # Step 3: compute chromaprint
            logger.info("Computing chromaprint for %s", submission_id)
            fp_str, duration = _compute_chromaprint(out_path)
            result["audio_fingerprint"] = fp_str
            result["audio_duration"]    = duration
            logger.info("Chromaprint done for %s: duration=%.1fs fp_len=%d",
                        submission_id, duration, len(fp_str))

    except subprocess.TimeoutExpired as e:
        result["error"] = f"Audio step timed out: {e}"
        logger.warning("Audio timeout for %s: %s", submission_id, e)
        return result
    except Exception as e:
        result["error"] = f"Audio processing failed: {e}"
        logger.warning("Audio processing error for %s: %s", submission_id, e)
        return result

    # Step 4: compare + store in DB
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
        best_score, best_row = 0.0, None
        for row in rows:
            score = _bic_similarity(fp_str, row[2])
            if score > best_score:
                best_score, best_row = score, row

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
        logger.info("Audio fingerprint stored ✅ for %s", submission_id)

    except Exception as e:
        logger.error("DB audio fingerprint failed for %s: %s", submission_id, e)
        result["error"] = f"DB error: {e}"

    return result
