#!/bin/bash
# WhatsConect – Quick Start (Mac)
# Run this after setup-mac.sh to start the app.
# Usage: chmod +x start-mac.sh && ./start-mac.sh

set -e
cd "$(dirname "$0")"

if [ ! -f "go-backend/go-backend" ]; then
    echo "⚠️  Go backend not built yet. Running setup first..."
    bash setup-mac.sh
    exit 0
fi

echo "🚀 Starting WhatsConect..."
npm start
