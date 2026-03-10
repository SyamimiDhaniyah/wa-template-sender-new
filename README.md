# WhatsConect

Desktop WhatsApp automation tool for patient appointment follow-ups and marketing.

## Features
- Connect WhatsApp via **QR code** or **Pairing Code**
- Multiple WhatsApp sessions via **Profiles** (create, rename, delete, terminate)
- Template library with placeholders: `{name} {date} {time} {topic} {branch} {doctor}`
- Batch sending with smart pacing (min/max seconds delay) and skip-already-sent per template
- CSV import with column mapping and variable merge
- **Image & Media Support**: Send and preview images in chat
- **Chat History**: View and sync recent conversations
- **Anti-Ban Protections**: Daily sending limits and smart pacing to protect your WhatsApp account
- **Device Branding**: Appears as Chrome in WhatsApp Linked Devices

## Tech Stack
- **Frontend**: Electron + HTML/CSS/JS
- **WhatsApp Engine**: [WhatsMeow](https://github.com/tulir/whatsmeow) (Go sidecar)
- **Storage**: `electron-store` (JSON) + Go SQLite (auth/session)

## Run (Development)
```bash
npm install
npm start
```

## Build Installer
```bash
npm run dist
```
Output: `dist/WhatsConect Setup x.x.x.exe`

## Notes
- Uses `whatsmeow` packaged as a local Go sidecar (`go-backend/go-backend.exe`).
- Auth state is managed natively via Go SQLite databases within the backend process.
- The Go sidecar runs on `http://127.0.0.1:12345` locally.
- In packaged builds, the Go executable is extracted to `app.asar.unpacked/go-backend/`.

## Developer Docs
See [`docs/DEVELOPER.md`](docs/DEVELOPER.md) for detailed WhatsMeow integration architecture and operational flows.
