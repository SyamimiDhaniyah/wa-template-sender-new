#!/bin/bash
# WhatsConect – Mac Setup Script
# Run this once to install dependencies and get the Go backend for macOS.
# Usage: chmod +x setup-mac.sh && ./setup-mac.sh

set -e

echo "======================================"
echo "  WhatsConect – Mac Setup"
echo "======================================"

# 1. Check Node.js
if ! command -v node &> /dev/null; then
    echo ""
    echo "❌ Node.js not found."
    echo "   Please install Node.js from https://nodejs.org (LTS version)"
    echo "   Then run this script again."
    exit 1
fi
echo "✅ Node.js: $(node --version)"

# 2. Setup Go Backend
echo ""
cd go-backend || { echo "❌ go-backend folder missing"; exit 1; }

# Determine Mac architecture
ARCH=$(uname -m)
if [ "$ARCH" = "arm64" ]; then
    BIN_NAME="go-backend-mac-arm64"
    echo "🔍 Detected Apple Silicon Mac (M1/M2/M3)..."
else
    BIN_NAME="go-backend-mac-amd64"
    echo "🔍 Detected Intel Mac..."
fi

if command -v go &> /dev/null; then
    echo "✅ Go compiler found. Compiling backend locally..."
    go build -o go-backend main.go
else
    echo "⚠️  Go compiler not found. Fetching pre-built binary..."
    
    # URL to raw binary on GitHub (from the latest release or main branch)
    # Since we push binaries to the repo via GitHub Actions, we can just download the raw file!
    DOWNLOAD_URL="https://raw.githubusercontent.com/SyamimiDhaniyah/wa-template-sender-new/main/go-backend/$BIN_NAME"
    
    echo "⬇️  Downloading $BIN_NAME from GitHub..."
    curl -sL "$DOWNLOAD_URL" -o "$BIN_NAME"
    
    if [ ! -f "$BIN_NAME" ] || [ ! -s "$BIN_NAME" ]; then
        echo "❌ Failed to download pre-built binary."
        echo "   Please check your internet connection and try again."
        exit 1
    fi
    
    echo "✅ Download complete."
    cp "$BIN_NAME" go-backend
    chmod +x go-backend
fi
cd ..
echo "✅ Go backend ready: go-backend/go-backend"

# 3. Install npm dependencies
echo ""
echo "📦 Installing npm dependencies..."
npm install

# 4. Launch the app
echo ""
echo "🚀 Starting WhatsConect..."
npm start
