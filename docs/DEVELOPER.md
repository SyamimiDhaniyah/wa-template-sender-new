# Developer Documentation

This document describes the WhatsApp integration end-to-end, including how Baileys is set up and used for:
- connection and authentication
- receive and sync
- send (single and batch)
- read state
- typing/presence
- media download and preview
- profile/contact enrichment

All implementation references below are in `main.js`, `preload.js`, and `renderer/renderer.js`.

## 1. Runtime Architecture

### Main process (`main.js`)
- Owns Baileys socket lifecycle.
- Manages profile auth files (`useMultiFileAuthState`).
- Normalizes and stores contacts/chats/messages in in-memory caches.
- Persists caches to `electron-store`.
- Exposes WhatsApp IPC handlers (`ipcMain.handle(...)`).

### Preload (`preload.js`)
- Exposes `window.api` methods and event listeners through `contextBridge`.
- Renderer never talks to Baileys directly.

### Renderer (`renderer/renderer.js`)
- Renders chats/messages.
- Sends user actions through IPC (`waSendChatMessage`, `waSendPresence`, `waMarkChatRead`, etc.).
- Keeps request sequencing guards (`waChatsReqSeq`, `waMessagesReqSeq`) to avoid stale updates.

## 2. Baileys Setup

### 2.1 Dependency and import mode

- Package: `@whiskeysockets/baileys`.
- Loaded through dynamic ESM import from CommonJS in `getBaileys()`.
- `getBaileys()` resolves and caches:
  - `makeWASocket`
  - `useMultiFileAuthState`
  - `DisconnectReason`
  - `Browsers`
  - `fetchLatestBaileysVersion`
  - `downloadMediaMessage`

Why dynamic import is used:
- Current Baileys versions are ESM-first.
- App codebase remains CommonJS in Electron main.

### 2.2 Socket creation

Socket is created in `connectWA(method, attemptId)` with:
- `auth: state` from `useMultiFileAuthState(authDir)`
- `printQRInTerminal: false`
- `logger: log` (pino)
- `browser: Browsers.windows("Google Chrome")` fallback tuple if unavailable
- `syncFullHistory: true`
- `version: waVersion` when latest version fetch succeeds (`getLatestWaVersionSafe(...)`)

Credentials are persisted through:
- `sock.ev.on("creds.update", saveCreds)`

### 2.3 Profile-based auth

Each profile has independent auth files:
- `getProfileAuthDir(profileId)` -> `<userData>/wa_profiles/<profileId>/auth`
- Auth state is not shared between profiles.
- Active profile selection is controlled by `app:setActiveProfile`.

## 3. Connection Lifecycle

Core entry points:
- `startHandshake(payload)` for explicit user handshake.
- `autoReconnectActiveProfile()` for startup/profile-switch reconnect.
- `getConnectionState()` for renderer status checks.

Concurrency and stale-event safety:
- `connectSetupPromise` serializes connect setup.
- `handshakeAttemptId` invalidates late events from old sockets.
- `stopSocket()` removes listeners, closes socket, and clears in-flight maps/timers.

### 3.1 Handshake modes

### QR mode
1. Renderer calls `wa:handshake` with `method: "qr"`.
2. `connectWA(...)` emits `wa:status` "Connecting...".
3. On `connection.update` with QR payload, main emits:
   - `wa:qr` (data URL)
   - `wa:status` "Scan QR in WhatsApp"

### Pairing code mode
1. Renderer calls `wa:handshake` with `method: "pairing"` and phone number.
2. Phone is normalized by `normalizeE164NoPlus`.
3. On connection init, main calls `sock.requestPairingCode(...)`.
4. Main emits:
   - `wa:pairingCode`
   - `wa:status` "Enter pairing code in WhatsApp"

### 3.2 Open and close handling

When connection opens:
- `isConnected = true`
- profile metadata updated (`updateConnectedProfileMeta(...)`)
- contact name sync kicked (`syncContactNamesForProfile(..., force: true, isInitialSync: true)`)
- history warmup kicked (`warmRecentHistoryForProfile(..., force: true)`)
- renderer notified through `wa:status` and `wa:chatSync`

When connection closes:
- status emitted to renderer with disconnect reason
- one-time auto recovery only for `restartRequired`
- no infinite retry loops by design
- invalid sessions flagged for cleanup on next handshake

## 4. Event Subscriptions (Baileys -> Internal Store)

Installed in `connectWA(...)`:
- `messaging-history.set`
- `contacts.upsert`
- `contacts.update`
- `chats.upsert`
- `chats.set`
- `chats.update`
- `messages.upsert`
- `messages.set`
- `messages.update`
- `presence.update`
- `connection.update`

Event processing strategy:
- Normalize incoming payloads.
- Upsert contacts/chats/messages.
- Canonicalize chat IDs.
- Emit `wa:chatSync` only as a lightweight invalidation signal.
- Renderer then fetches full current state via IPC.

## 5. Data Model and Canonicalization

Contact cache:
- `waContactsByProfileMem[profileId][msisdn]`
- Includes: `jid`, `lid`, `name`, `notify`, `verifiedName`, `imgUrl`, timestamps.

Chat/message cache:
- `waChatByProfileMem[profileId]`
- `chatsByJid`
- `messagesByChat`

Critical JID logic:
- `canonicalizeChatJidForProfile(profileId, chatJid)`
- `findContactByJidForProfile(...)`
- LID (`...@lid`) to PN (`...@s.whatsapp.net`) reconciliation prevents split threads and dropped lookups.

Store limits and persistence:
- max messages per chat: `WA_CHAT_MAX_MESSAGES_PER_CHAT`
- max stored chats: `WA_CHAT_MAX_STORED_CHATS`
- debounce persist: `WA_CHAT_PERSIST_DEBOUNCE_MS`

## 6. Receiving Messages (Detailed Flow)

Main flow:
1. Baileys event -> `upsertMessagesForProfile(...)` or `applyMessageUpdatesForProfile(...)`.
2. Each raw message passes through `normalizeMessageRecord(profileId, rawMessage)`.
3. Message key normalized by `normalizeMessageKey(...)`.
4. Chat JID canonicalized by `canonicalizeChatJidForProfile(...)`.
5. Message summary extracted (text/media/reaction/contact/poll/system).
6. Message merged into `messagesByChat`.
7. Chat summary updated (`lastMessageTimestampMs`, preview, unread).
8. `scheduleWaChatSync(profileId, reason)` emits incremental change signal.

Renderer flow:
1. `onWaChatSync` triggers `scheduleWaSyncRefresh(...)`.
2. Renderer invokes `wa:getRecentChats`.
3. If active chat exists, renderer invokes `wa:getChatMessages`.
4. UI rerenders chat list and message pane.

## 7. Sending Messages

### 7.1 Single chat composer

Renderer:
- `sendWaComposerMessage()` handles text + queued attachments.
- Calls `waSendChatMessage({ chatJid, text, attachment, quotedKey? })`.

Main (`sendChatMessage(payload)`):
- Validates socket connection and target JID.
- Supports payloads:
  - text
  - image (+ caption)
  - video (+ caption)
  - audio
  - document (+ caption)
- Uses `sock.sendMessage(chatJid, messagePayload, sendOptions)`.
- On success, immediately upserts returned message into local store for instant UI.
- For local image attachments, stores local preview data for fast rendering.

### 7.2 Batch sending

Main handlers:
- `wa:sendBatch`
- `wa:sendPreparedBatch`

Features:
- pacing controls (pattern/min/max delay)
- skip already sent (template-based keying)
- optional AI rewrite pipeline
- progress events to renderer: `batch:progress`
- recipient name hinting into contact cache (`rememberRecipientNameForProfile`)

## 8. Read State (Unread -> Read)

Renderer:
- `refreshWaMessages(...)` can call `waMarkChatRead` on open chat.

Main (`markChatReadForProfile(...)`):
- marks local unread items
- attempts network update in fallback order:
  - `sock.readMessages(...)`
  - `sock.sendReceipt(..., "read")`
  - `sock.chatModify({ markRead: true, ... })`
- forces local `unreadCount = 0`
- emits `wa:chatSync`

## 9. Typing and Presence

### 9.1 Outgoing typing signal

Renderer typing engine:
- `handleWaComposerInputTyping()`
- `startWaOutgoingTyping(chatJid)`
- `scheduleWaOutgoingTypingPause(chatJid)`
- `stopWaOutgoingTyping({ sendPaused })`

Behavior:
- start `composing` when input becomes active
- send periodic heartbeat while typing
- send `paused` on blur, send, tab/chat switch, or idle timeout

IPC path:
- renderer: `waSendPresence({ chatJid, type })`
- main: `wa:sendPresence` -> `sendChatPresence(...)`
- network: `sock.sendPresenceUpdate(type, chatJid)`

Compatibility path:
- `wa:setTyping` -> `sendChatTypingPresence(...)`

### 9.2 Incoming typing signal

Main:
- `sock.ev.on("presence.update", ...)`
- normalizes sender and allowed presence values
- emits `wa:presence` with participant map

Renderer:
- `applyWaPresenceUpdate(payload)`
- stores per-chat presence with expiry timers
- header displays `typing...` / `recording audio...` via `renderWaConversationHead()`

## 10. Media Handling

### Attach and send
- File picker via `wa:pickAttachment`.
- MIME and media kind resolved by:
  - `getMimeTypeForPath(...)`
  - `attachmentKindFromMimeOrPath(...)`

### Download media
- IPC: `wa:downloadMedia` -> `downloadChatMedia(payload)`
- Uses Baileys `downloadMediaMessage(...)`
- Saves through native save dialog.

### Image preview and cache
- IPC: `wa:resolveImagePreview` -> `resolveImagePreview(payload)`
- Tries local stored image path first.
- Falls back to Baileys download if missing (`ensureLocalImageForStoredMessage(...)`).
- Stores compact thumbnail data URLs and local media paths.

## 11. Contact Names and Profile Photos

Name sync:
- `syncContactNamesForProfile(...)` with app-state resync and cooldown/backoff.
- Promotes better labels over fallback numeric/JID labels (`choosePreferredIdentityLabel`).

Photo enrichment:
- `enrichContactPhotosForProfile(...)` fetches missing profile pictures.
- Throttled by:
  - per-profile in-flight guard
  - rate-limit backoff
  - min time between checks
  - bounded concurrency
- One startup backfill pass is triggered through `wa:getRecentChats`.

## 12. IPC Contract (WhatsApp)

Renderer -> Main (`ipcRenderer.invoke`):
- `wa:handshake`
- `wa:autoReconnect`
- `wa:getConnectionState`
- `wa:getContacts`
- `wa:getRecentChats`
- `wa:getChatMessages`
- `wa:markChatRead`
- `wa:sendPresence`
- `wa:setTyping`
- `wa:sendChatMessage`
- `wa:resetChatHistory` (kept as backend endpoint, no current header button)
- `wa:pickAttachment`
- `wa:downloadMedia`
- `wa:resolveImagePreview`
- `wa:sendBatch`
- `wa:sendPreparedBatch`

Main -> Renderer (`webContents.send`):
- `wa:status`
- `wa:qr`
- `wa:pairingCode`
- `wa:chatSync`
- `wa:presence`
- `batch:progress`

## 13. Startup and Operational Sequence

Typical startup path:
1. `app.whenReady()` loads profiles and persisted caches.
2. Window created.
3. On first `did-finish-load`, `autoReconnectActiveProfile()` attempts connection if session exists.
4. Renderer runs `loadInitialDataAfterLogin()` and calls `wa:getRecentChats`.
5. Chat list/messages hydrate; background enrich/sync processes continue.

## 14. Troubleshooting

If incoming messages are missing:
- verify `messages.upsert` / `messages.set` listeners are firing
- verify `normalizeMessageRecord(...)` is not filtering unexpectedly
- verify chat JID canonicalization for LID/PN mapping

If outgoing send works but UI lags:
- inspect chat/message cache size limits
- inspect `wa:chatSync` frequency and renderer refresh loops
- inspect heavy background tasks (history warmup, photo enrichment)

If typing indicators do not appear:
- confirm renderer sends `wa:sendPresence` on input activity
- confirm main accepts type in `WA_ALLOWED_OUTGOING_CHAT_PRESENCE`
- confirm socket connected and chat JID valid

If profile photo backfill is incomplete:
- check rate-limit backoff and photo fetch cooldown options
- verify chat contacts are seeded into contact cache
- verify `profilePictureUrl` failures in logs

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
