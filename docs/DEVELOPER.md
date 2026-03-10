# Developer Documentation

This document describes the WhatsApp integration end-to-end.

The **WhatsApp engine** is 100% WhatsMeow (Go). All Baileys code has been removed.

---

## 1. Runtime Architecture

### Main process (`main.js` / Node.js)
- Owns the Go Sidecar lifecycle (`child_process.spawn`).
- In **packaged builds**, resolves the Go binary from `app.asar.unpacked/go-backend/` via `process.resourcesPath`.
- In **dev builds**, resolves from `__dirname/go-backend/`.
- Proxies UI IPC requests to the local Go HTTP server at `http://127.0.0.1:12345`.
- Manages message/chat cache via `electron-store` (persisted JSON).

### Preload (`preload.js`)
- Exposes `window.api` methods and event listeners via `contextBridge`.
- Renderer never communicates with the Go sidecar or WhatsApp directly.

### Renderer (`renderer/renderer.js`)
- Renders chats/messages and handles composer input.
- Sends user actions through IPC (`waSendChatMessage`, `waSendPresence`, `waMarkChatRead`, etc.).
- Keeps request sequencing guards (`waChatsReqSeq`, `waMessagesReqSeq`) to avoid stale updates.

### Backend Process (`go-backend/` / WhatsMeow)
- Handles WhatsApp protocol connection via WhatsMeow.
- Manages profile auth SQLite databases at `%APPDATA%/whatsconect/wa_profiles/<profileId>/store.db`.
- Exposes local HTTP endpoints consumed by `main.js`.
- Emits `chatSync` / `historySync` / `status` / `presence` JSON events to stdout, consumed by `main.js`.

---

## 2. Go Backend API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/status` | Connection status (connected, loggedIn, pushName, jid) |
| GET | `/api/login/qr` | Start QR login, returns QR code string |
| GET | `/api/login/pair?phone=...` | Start phone pairing, returns 8-digit code |
| GET | `/api/contacts` | All contacts |
| GET | `/api/chats` | Chat settings |
| GET | `/api/messages` | Recent messages |
| POST | `/api/send` | Send message (text or image) |
| POST | `/api/presence` | Set typing/online presence |
| POST | `/api/read` | Mark message as read |
| POST | `/api/onwhatsapp` | Check if phones are on WhatsApp |
| POST | `/api/logout` | Log out and clear session |
| POST | `/api/media/download` | Download media bytes for a stored message |

---

## 3. Connection Lifecycle

### Handshake modes
1. Renderer calls `wa:handshake` with `method: "qr"` or `"pairing"`.
2. Main passes this to the Go backend.
3. Go backend streams the QR or pairing code back.
4. Main emits `wa:qr` or `wa:pairingCode` to the renderer.

### Status Reporting
The `wa:status` IPC event includes a `text` field (e.g., `"Connected"`, `"Logged Out"`, `"Not connected"`) so the UI accurately reflects backend state.

---

## 4. Profiles

Profiles represent individual WhatsApp sessions.

- **Default profile**: `p_default` named **"Dentabay"** (auto-created on first run or reset).
- **Migration**: On upgrade, any non-user-renamed `p_default` profile is automatically renamed to `"Dentabay"`.
- Profile actions: Create, Rename, Delete, Terminate (clear auth only).
- On delete/terminate of the active profile, calls `disconnectActiveProfileSocket()` to cleanly stop the session.

---

## 5. Sending Messages

Renderer calls `waSendChatMessage` → Main → HTTP POST to `go-backend /api/send`.

Go backend handles:
- JID normalization (`@s.whatsapp.net` / `@lid` / `@g.us` groups)
- Image upload via `client.Upload(ctx, data, whatsmeow.MediaImage)`
- After successful send, emits a `chatSync` event to stdout so `main.js` immediately persists the message

### Anti-Ban Protections
- **Daily limit**: 200 messages per profile per day (tracked by `main.js`)
- **Pacing**: Min/max delay between batch messages
- Limit warnings are shown in the UI before blocking further sends

---

## 6. Media / Image Preview

Image thumbnails are extracted from the `imageMessage.jpegThumbnail` field in WhatsMeow's protojson output (serialized as base64 strings). The `bytesToBase64` function handles both raw bytes and base64 strings.

For full-resolution preview:
- `ensureLocalImageForStoredMessage` calls `/api/media/download` with the stored proto message.
- Go backend calls `client.DownloadAny(ctx, &protoMsg)` and returns base64 bytes.
- Result is saved as a local file in `%APPDATA%/whatsconect/wa_media/images/`.

---

## 7. Device Branding

In `go-backend/main.go`, before creating the WhatsApp client:

```go
store.DeviceProps.PlatformType = waCompanionReg.DeviceProps_CHROME.Enum()
store.DeviceProps.Os = &osName // "WhatsConect"
```

This makes WhatsApp Linked Devices show the connection as **Chrome** instead of "Other device".

> Note: The device name is registered at scan/pairing time. An existing session needs to be re-linked to reflect this change.

---

## 8. Development Commands

```bash
# Install dependencies
npm install

# Run in development
npm start

# Build installer
npm run dist
# Output: dist/WhatsConect Setup x.x.x.exe

# Rebuild Go backend only
cd go-backend
go build -o go-backend.exe main.go
```
