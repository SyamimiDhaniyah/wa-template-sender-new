#!/bin/bash
# Double-click this file if macOS blocks WhatsConect on first launch.

set -euo pipefail

APP_PATH="/Applications/WhatsConect.app"

if [ ! -d "$APP_PATH" ]; then
  osascript -e 'display dialog "WhatsConect.app was not found in Applications. Please install it from the DMG first." buttons {"OK"} default button "OK" with icon caution' >/dev/null 2>&1 || true
  echo "WhatsConect.app was not found in /Applications."
  echo "Install the app from the DMG first, then run this launcher again."
  exit 1
fi

xattr -dr com.apple.quarantine "$APP_PATH" || true
open "$APP_PATH"
