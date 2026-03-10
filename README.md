# WhatsConect

Desktop WhatsApp template sender for patient follow-ups.

## Features
- Connect WhatsApp via **QR** or **Pairing Code** (user-triggered handshake)
- Multiple WhatsApp sessions via **Profiles**
- Template library with placeholders: `{name} {date} {time} {topic} {branch} {doctor}`
- Batch sending with pacing (min/max seconds) and skip-already-sent per template
- CSV import with column mapping + variables merge

## Run
```bash
npm install
npm start
```

## Build installer
```bash
npm run dist
```

## Notes
## Notes
- Uses `whatsmeow` packaged in a local Go sidecar.
- Auth state is managed natively via Go SQLite databases within the backend process.

## Developer Docs
- See `docs/DEVELOPER.md` for detailed WhatsMeow integration architecture and operational flows.
