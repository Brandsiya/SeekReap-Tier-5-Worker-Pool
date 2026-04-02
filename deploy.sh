#!/bin/bash
# One-command Fly deployment for SeekReap Tier 5 Worker Pool

APP_NAME="seekreap-tier5-worker"
REDIS_URL="https://darling-sunbird-82526.upstash.io"

# Ensure flyctl is available
export PATH="$HOME/.fly/bin:$PATH"

# Login (only needed if not logged in)
echo "Logging into Fly..."
flyctl auth login --copy-to-browser

# Create app if it doesn't exist
if ! flyctl apps list | grep -q "$APP_NAME"; then
    echo "Creating Fly app: $APP_NAME"
    flyctl apps create "$APP_NAME"
fi

# Create fly.toml if missing
if [ ! -f fly.toml ]; then
    echo "Creating fly.toml for $APP_NAME"
    flyctl launch --name "$APP_NAME" --no-deploy --copy-config
fi

# Set secrets
echo "Setting REDIS_URL secret"
flyctl secrets set REDIS_URL="$REDIS_URL" -a "$APP_NAME"

# Deploy
echo "Deploying app..."
flyctl deploy -a "$APP_NAME"

echo "✅ Deployment complete. Monitor at:"
echo "https://fly.io/apps/$APP_NAME/monitoring"
