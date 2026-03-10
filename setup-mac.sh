#!/bin/bash
# WhatsConect Mac setup
# Usage: chmod +x setup-mac.sh && ./setup-mac.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR"
GO_BACKEND_DIR="$PROJECT_DIR/go-backend"
GO_MOD_FILE="$GO_BACKEND_DIR/go.mod"
NODE_PKG_URL="https://nodejs.org/dist/v20.11.1/node-v20.11.1.pkg"

cd "$PROJECT_DIR"

echo "======================================"
echo "  WhatsConect Mac Auto-Setup"
echo "======================================"

if [ ! -d "$GO_BACKEND_DIR" ]; then
    echo "ERROR: go-backend folder is missing."
    exit 1
fi

if [ ! -f "$GO_MOD_FILE" ]; then
    echo "ERROR: $GO_MOD_FILE is missing."
    exit 1
fi

MAC_ARCH="$(uname -m)"
if [ "$MAC_ARCH" = "arm64" ]; then
    GO_ARCH="arm64"
    PREBUILT_NAME="go-backend-mac-arm64"
    GO_PKG_ARCH="arm64"
    echo "Detected Apple Silicon Mac."
else
    GO_ARCH="amd64"
    PREBUILT_NAME="go-backend-mac-amd64"
    GO_PKG_ARCH="amd64"
    echo "Detected Intel Mac."
fi

GO_VERSION="$(awk '/^go / { print $2; exit }' "$GO_MOD_FILE")"
GO_PKG_FILE="go${GO_VERSION}.darwin-${GO_PKG_ARCH}.pkg"
GO_PKG_URL="https://go.dev/dl/${GO_PKG_FILE}"

refresh_path() {
    export PATH="/usr/local/go/bin:/opt/homebrew/bin:/usr/local/bin:$PATH"
    hash -r 2>/dev/null || true
}

install_node_if_needed() {
    if command -v node >/dev/null 2>&1; then
        echo "Node.js ready: $(node --version)"
        return
    fi

    if [ "${CI:-}" = "true" ]; then
        echo "ERROR: Node.js is missing in CI. Install it in the workflow first."
        exit 1
    fi

    echo "Node.js not found. Downloading installer..."
    curl -fL -o node-installer.pkg "$NODE_PKG_URL"
    echo "Complete the Node.js installer, then this script will continue."
    open -W node-installer.pkg
    rm -f node-installer.pkg
    refresh_path

    if ! command -v node >/dev/null 2>&1; then
        echo "ERROR: Node.js installation failed or was cancelled."
        exit 1
    fi

    echo "Node.js ready: $(node --version)"
}

install_go_if_needed() {
    if command -v go >/dev/null 2>&1; then
        echo "Go ready: $(go version)"
        return
    fi

    if [ -f "$GO_BACKEND_DIR/$PREBUILT_NAME" ]; then
        echo "Go is not installed. Using bundled prebuilt backend: $PREBUILT_NAME"
        return
    fi

    if [ "${CI:-}" = "true" ]; then
        echo "ERROR: Go is missing in CI and no prebuilt backend is bundled."
        exit 1
    fi

    echo "Go is not installed. Downloading ${GO_PKG_FILE} from go.dev..."
    curl -fL -o go-installer.pkg "$GO_PKG_URL"
    echo "Complete the Go installer, then this script will continue."
    open -W go-installer.pkg
    rm -f go-installer.pkg
    refresh_path

    if ! command -v go >/dev/null 2>&1; then
        echo "ERROR: Go installation failed or was cancelled."
        echo "Install Go ${GO_VERSION} from https://go.dev/dl/ and run this script again."
        exit 1
    fi

    echo "Go ready: $(go version)"
}

prepare_backend() {
    echo ""
    echo "Preparing Go backend..."
    cd "$GO_BACKEND_DIR"

    if command -v go >/dev/null 2>&1; then
        echo "Compiling backend locally for darwin/${GO_ARCH}..."
        GOOS=darwin GOARCH="$GO_ARCH" go build -o go-backend main.go
        chmod +x go-backend
        cd "$PROJECT_DIR"
        echo "Go backend ready."
        return
    fi

    if [ -f "$PREBUILT_NAME" ]; then
        cp "$PREBUILT_NAME" go-backend
        chmod +x go-backend
        cd "$PROJECT_DIR"
        echo "Go backend ready from bundled prebuilt binary."
        return
    fi

    echo "ERROR: Unable to prepare backend. Go is missing and no prebuilt binary was found."
    exit 1
}

install_dependencies() {
    echo ""
    echo "Installing app dependencies..."

    if [ -f package-lock.json ] && [ "${CI:-}" = "true" ]; then
        npm ci --silent
        return
    fi

    npm install --silent
}

install_node_if_needed
refresh_path
install_go_if_needed
refresh_path
prepare_backend
install_dependencies

echo ""
if [ "${CI:-}" = "true" ]; then
    echo "CI detected. Skipping 'npm start'."
    echo "Setup script completed successfully."
    exit 0
fi

echo "Starting WhatsConect..."
npm start
