# WA Template Sender

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
- Uses Baileys (`@whiskeysockets/baileys`).
- Auth state uses `useMultiFileAuthState` for now (ok for desktop tool, not meant for server-scale).

## Developer Docs
- See `docs/DEVELOPER.md` for architecture and WhatsApp flow details (receive/send/read/typing/reset history).
