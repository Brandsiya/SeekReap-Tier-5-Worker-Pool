#!/bin/bash
set -euo pipefail

CONTENT_URL="$1"

curl -s -X POST https://seekreap-tier-3-private-10.onrender.com/internal/audio-fingerprint \
  -H "Content-Type: application/json" \
  -d "{\"content_url\": \"${CONTENT_URL}\", \"yt_dlp_args\": \"--cookies $YT_COOKIES\"}"
