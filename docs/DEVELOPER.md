# Developer Documentation

This document describes the WhatsApp integration end-to-end, which uses **WhatsMeow (Go Sidecar)** for:
- connection and authentication
- receive and sync
- send (single and batch)
- read state
- typing/presence
- media download and preview
- profile/contact enrichment

All frontend references below are in `main.js`, `preload.js`, and `renderer/renderer.js`. The backend engine is housed in `go-backend/`.

## 1. Runtime Architecture

### Main process (`main.js` / Node.js)
- Owns the Go Sidecar lifecycle (`child_process.spawn`).
- Proxies UI requests to the local Go HTTP/WS server.

### Preload (`preload.js`)
- Exposes `window.api` methods and event listeners through `contextBridge`.
- Renderer never talks to the Go sidecar or WhatsApp directly.

### Renderer (`renderer/renderer.js`)
- Renders chats/messages.
- Sends user actions through IPC (`waSendChatMessage`, `waSendPresence`, `waMarkChatRead`, etc.).
- Keeps request sequencing guards (`waChatsReqSeq`, `waMessagesReqSeq`) to avoid stale updates.

### Backend Process (`go-backend/` / WhatsMeow)
- Handles the actual WhatsApp protocol connection via WhatsMeow.
- Manages profile auth SQLite databases.
- Normalizes and stores contacts/chats/messages in memory and SQLite.
- Exposes local endpoints for the Electron `main.js` to communicate with.

## 2. Connection Lifecycle
The Go sidecar handles the connection to WhatsApp. 

### Handshake modes
1. Renderer calls `wa:handshake` with `method: "qr"` or `"pairing"`.
2. Main passes this to the Go backend.
3. Go backend streams the QR or pairing code back through WebSocket.
4. Main emits `wa:qr` or `wa:pairingCode` to the UI.

## 3. Event Subscriptions & Data Model
The Go sidecar subscribes to all WhatsApp events (messages, presence, etc.) and updates its local SQLite/Memory cache. 
When data changes, the Go backend emits an invalidation signal through WebSocket to Node.js, which emits `wa:chatSync` to the Renderer.
The Renderer then fetches the full current state via IPC (which asks the Go backend for the JSON payload).

## 4. Sending Messages
Renderer handles composer input and calls `waSendChatMessage`.
Main receives it and makes an HTTP POST request to the Go backend.
The Go backend handles the `@s.whatsapp.net` / `@lid` formatting and dispatches via WhatsMeow.

Batch sending works similarly but the Go backend manages the pacing controls (pattern/min/max delay) natively.

## 15. Development Commands

Run:
```bash
npm install
npm start
```

Build:
```bash
npm run dist
```
