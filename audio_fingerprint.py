"""
SeekReap Tier-5 — Track B audio fingerprinting.
Gets direct stream URL via Tier-3 proxy (Tier-3 can reach YouTube, Tier-5 cannot).
Then uses ffmpeg -t 120 to download only the first 2 minutes for fpcalc.
"""
import os, subprocess, tempfile, logging, json, urllib.request, urllib.error

logger = logging.getLogger(__name__)

DB_URL     = os.environ.get("DATABASE_URL",
    "postgresql://neondb_owner:npg_yX7aHMwIqQC4@ep-rapid-base-ai27r1sa-pooler.c-4.us-east-1.aws.neon.tech:5432/seekreap_neon_db?sslmode=require")
TIER3_URL  = os.environ.get("TIER3_URL",
    "https://seekreap-tier3-tif2gmgi4q-uc.a.run.app")

MATCH_THRESHOLD = 0.85


def _get_identity_token() -> str:
    """Fetch GCP identity token for calling private Tier-3 service."""
    meta_url = (
        "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts"
        "/default/identity?audience=" + TIER3_URL
    )
    req = urllib.request.Request(meta_url, headers={"Metadata-Flavor": "Google"})
    with urllib.request.urlopen(req, timeout=5) as resp:
        return resp.read().decode()


def _get_audio_stream_url(content_url: str) -> str:
    """Call Tier-3's /internal/stream-url to get a direct audio stream URL."""
    endpoint = f"{TIER3_URL}/internal/stream-url"
    payload  = json.dumps({"content_url": content_url}).encode()

    try:
        token = _get_identity_token()
        auth_header = f"Bearer {token}"
    except Exception as e:
        logger.warning("Could not get identity token: %s — trying unauthenticated", e)
        auth_header = None

    req = urllib.request.Request(
        endpoint,
        data=payload,
        headers={
            "Content-Type": "application/json",
            **({"Authorization": auth_header} if auth_header else {}),
        },
        method="POST"
    )
    with urllib.request.urlopen(req, timeout=35) as resp:
        data = json.loads(resp.read().decode())

    if "error" in data:
        raise RuntimeError(f"Tier-3 stream-url error: {data['error']}")
    stream_url = data.get("stream_url", "")
    if not stream_url:
        raise RuntimeError("Tier-3 returned empty stream_url")
    logger.info("Got stream URL from Tier-3 (len=%d)", len(stream_url))
    return stream_url


def _download_audio_ffmpeg(stream_url: str, out_path: str) -> None:
    """Use ffmpeg to download exactly 120s of audio from the direct stream URL."""
    subprocess.run(
        [
            "ffmpeg", "-y",
            "-t", "120",
            "-i", stream_url,
            "-vn",
            "-acodec", "copy",
            "-loglevel", "error",
            out_path,
        ],
        check=True, timeout=60
    )


def _compute_chromaprint(audio_path: str):
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

            logger.info("Requesting stream URL from Tier-3 for %s", submission_id)
            stream_url = _get_audio_stream_url(content_url)

            logger.info("Downloading 120s via ffmpeg for %s", submission_id)
            _download_audio_ffmpeg(stream_url, out_path)

            logger.info("Computing chromaprint for %s", submission_id)
            fp_str, duration = _compute_chromaprint(out_path)
            result["audio_fingerprint"] = fp_str
            result["audio_duration"]    = duration
            logger.info("Chromaprint done for %s: duration=%.1fs", submission_id, duration)

    except subprocess.TimeoutExpired as e:
        result["error"] = f"Audio step timed out: {e}"
        logger.warning("Audio timeout for %s: %s", submission_id, e)
        return result
    except Exception as e:
        result["error"] = f"Audio processing failed: {e}"
        logger.warning("Audio processing error for %s: %s", submission_id, e)
        return result

    # Store in DB
    try:
        import psycopg2, uuid as _uv
        conn = psycopg2.connect(DB_URL)
        cur  = conn.cursor()

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
            "UPDATE fingerprints SET audio_fingerprint=%s, audio_duration=%s WHERE submission_id=%s",
            (fp_str, duration, submission_id)
        )
        if cur.rowcount == 0:
            cur.execute(
                """INSERT INTO fingerprints
                    (submission_id, creator_id, content_url, audio_fingerprint, audio_duration, fingerprint_version)
                   VALUES (%s,%s,%s,%s,%s,'audio-v1') ON CONFLICT DO NOTHING""",
                (submission_id, creator_id, content_url, fp_str, duration)
            )

        conn.commit(); cur.close(); conn.close()
        result["audio_stored"] = True
        logger.info("Audio fingerprint stored ✅ for %s", submission_id)

    except Exception as e:
        logger.error("DB audio fingerprint failed for %s: %s", submission_id, e)
        result["error"] = f"DB error: {e}"

    return result
