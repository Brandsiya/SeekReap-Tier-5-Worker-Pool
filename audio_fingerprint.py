"""
SeekReap Tier-5 — Track B audio fingerprinting.
Delegates the full pipeline to Tier-3 (which has YouTube/googlevideo egress).
Tier-5 only handles DB storage.
"""
import os, json, logging, urllib.request
import psycopg2, uuid as _uv

logger = logging.getLogger(__name__)

DB_URL    = os.environ.get("DATABASE_URL",
    "postgresql://neondb_owner:npg_yX7aHMwIqQC4@ep-rapid-base-ai27r1sa-pooler.c-4.us-east-1.aws.neon.tech:5432/seekreap_neon_db?sslmode=require")
TIER3_URL = os.environ.get("TIER3_URL",
    "https://seekreap-tier3-tif2gmgi4q-uc.a.run.app")

MATCH_THRESHOLD = 0.85


def _get_identity_token() -> str:
    meta_url = (
        "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts"
        "/default/identity?audience=" + TIER3_URL
    )
    req = urllib.request.Request(meta_url, headers={"Metadata-Flavor": "Google"})
    with urllib.request.urlopen(req, timeout=5) as resp:
        return resp.read().decode()


def _call_tier3_fingerprint(content_url: str) -> dict:
    endpoint = f"{TIER3_URL}/internal/audio-fingerprint"
    payload  = json.dumps({"content_url": content_url}).encode()
    try:
        token = _get_identity_token()
        auth_header = f"Bearer {token}"
    except Exception as e:
        logger.warning("No identity token: %s — trying unauthenticated", e)
        auth_header = None
    req = urllib.request.Request(
        endpoint, data=payload,
        headers={
            "Content-Type": "application/json",
            **({"Authorization": auth_header} if auth_header else {}),
        },
        method="POST"
    )
    # Tier-3 pipeline: 25s yt-dlp + 180s ffmpeg + 30s fpcalc = up to ~235s
    with urllib.request.urlopen(req, timeout=260) as resp:
        return json.loads(resp.read().decode())


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

    logger.info("Calling Tier-3 audio fingerprint pipeline for %s", submission_id)
    try:
        data = _call_tier3_fingerprint(content_url)
    except Exception as e:
        result["error"] = f"Tier-3 call failed: {e}"
        logger.warning("Tier-3 audio call failed for %s: %s", submission_id, e)
        return result

    if "error" in data:
        result["error"] = f"Tier-3 pipeline error: {data['error']}"
        logger.warning("Tier-3 returned error for %s: %s", submission_id, data["error"])
        return result

    fp_str   = data.get("fingerprint", "")
    duration = float(data.get("duration", 0))
    result["audio_fingerprint"] = fp_str
    result["audio_duration"]    = duration
    logger.info("Got fingerprint from Tier-3 for %s: duration=%.1fs", submission_id, duration)

    try:
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
