#!/bin/bash
# WhatsConect – Mac Setup Script
# Run this once to install dependencies and build the Go backend for macOS.
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

# 2. Check Go
if ! command -v go &> /dev/null; then
    echo ""
    echo "❌ Go not found."
    echo "   Please install Go from https://go.dev/dl/"
    echo "   Then run this script again."
    exit 1
fi
echo "✅ Go: $(go version)"

# 3. Install npm dependencies
echo ""
echo "📦 Installing npm dependencies..."
npm install

# 4. Build the Go backend for macOS
echo ""
echo "🔨 Building Go backend for macOS..."
cd go-backend
go build -o go-backend main.go
cd ..
echo "✅ Go backend built: go-backend/go-backend"

# 5. Launch the app
echo ""
echo "🚀 Starting WhatsConect..."
npm start
