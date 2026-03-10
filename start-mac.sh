#!/bin/bash
# WhatsConect quick start for macOS
# Usage: chmod +x start-mac.sh && ./start-mac.sh

set -euo pipefail

cd "$(dirname "$0")"

if [ ! -x "go-backend/go-backend" ] && [ ! -x "go-backend/go-backend-mac-arm64" ] && [ ! -x "go-backend/go-backend-mac-amd64" ]; then
    echo "Go backend is not ready yet. Running setup first..."
    bash ./setup-mac.sh
    exit 0
fi

echo "Starting WhatsConect..."
npm start
