#!/bin/bash
# WhatsConect – Mac Setup Script
# Run this once to install Node, get the Go backend, and launch standard app.
# Usage: chmod +x setup-mac.sh && ./setup-mac.sh

set -e

echo "======================================"
echo "  WhatsConect – Mac Auto-Setup"
echo "======================================"

# 1. Check and Auto-Install Node.js
if ! command -v node &> /dev/null; then
    if [ "${CI:-}" = "true" ]; then
        echo "❌ Node.js is missing in CI. Install it in the workflow first."
        exit 1
    fi

    echo "⚠️  Node.js not found! Downloading Node.js installer..."
    curl -L -o node-installer.pkg "https://nodejs.org/dist/v20.11.1/node-v20.11.1.pkg"
    echo "Please complete the installer window."
    open -W node-installer.pkg
    rm -f node-installer.pkg
    export PATH="/usr/local/bin:$PATH"

    if ! command -v node &> /dev/null; then
        echo "❌ Node.js installation failed or was cancelled."
        exit 1
    fi
fi
echo "✅ Node.js ready: $(node --version)"

# 2. Setup Go Backend
echo ""
cd go-backend || { echo "❌ go-backend folder missing"; exit 1; }

ARCH=$(uname -m)
if [ "$ARCH" = "arm64" ]; then
    BIN_NAME="go-backend-mac-arm64"
    echo "🔍 Apple Silicon Mac detected."
else
    BIN_NAME="go-backend-mac-amd64"
    echo "🔍 Intel Mac detected."
fi

if command -v go &> /dev/null; then
    echo "✅ Go compiler found. Compiling backend locally..."
    go build -o go-backend main.go
else
    echo "⬇️  Fetching pre-built Go backend from GitHub..."
    DOWNLOAD_URL="https://raw.githubusercontent.com/SyamimiDhaniyah/wa-template-sender-new/main/go-backend/$BIN_NAME"
    curl -sL "$DOWNLOAD_URL" -o "$BIN_NAME"
    
    if [ ! -f "$BIN_NAME" ] || [ ! -s "$BIN_NAME" ]; then
        echo "❌ Failed to download pre-built binary."
        exit 1
    fi
    cp "$BIN_NAME" go-backend
    chmod +x go-backend
fi
cd ..
echo "✅ Go backend ready"

# 3. Install npm dependencies
echo ""
echo "📦 Installing internal app dependencies..."
npm install --silent

# 4. Launch the app
echo ""
echo "🚀 Everything installed! Starting WhatsConect..."
npm start
