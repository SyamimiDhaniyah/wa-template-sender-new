# Developer Documentation

This document explains how the WhatsApp integration works in this Electron app, with focus on Baileys (`@whiskeysockets/baileys`), message receive/send flow, read handling, and typing presence.

## 1. High-level Architecture

- `main.js`
  - Electron main process.
  - Owns Baileys socket lifecycle, auth state, chat/message/contact caches, and IPC handlers.
- `preload.js`
  - Safe bridge between renderer and main (`contextBridge` + `ipcRenderer.invoke/on`).
- `renderer/renderer.js`
  - UI state and user interaction.
  - Calls IPC methods for WhatsApp actions and renders chats/messages/presence.

Data path (normal case):
- Baileys event in `main.js` -> normalize/store -> emit `wa:chatSync` / `wa:presence`
- renderer listens -> requests latest chats/messages -> renders UI

## 2. Baileys Lifecycle and Connection

Core functions in `main.js`:
- `startHandshake(payload)`
- `autoReconnectActiveProfile()`
- `connectWA(method, attemptId)`
- `getConnectionState()`

Important behavior:
- Uses `useMultiFileAuthState` per profile auth directory.
- Uses `syncFullHistory: true` when creating socket.
- Sends connection status to renderer via `wa:status`.
- Supports QR and pairing-code flows.

## 3. Internal Stores

In-memory stores in `main.js`:
- Contacts cache by profile (`waContactsByProfileMem`)
- Chat/message cache by profile (`waChatByProfileMem`)
  - `chatsByJid`
  - `messagesByChat`

Persistence:
- Both are persisted via `electron-store`.

Key design point:
- Chat JID canonicalization is profile-aware (`canonicalizeChatJidForProfile`).
- This prevents duplicate/missing threads when WA uses LID JIDs (`...@lid`) while contact mapping has phone JID (`...@s.whatsapp.net`).

## 4. Receiving Messages (Baileys -> UI)

Baileys listeners in `connectWA(...)`:
- `messaging-history.set`
- `messages.upsert`
- `messages.set`
- `messages.update`
- `presence.update`

Message pipeline:
1. Event payload enters `upsertMessagesForProfile(profileId, rows)` (or `applyMessageUpdatesForProfile`).
2. Each message is normalized by `normalizeMessageRecord(profileId, rawMessage)`.
3. JID is canonicalized (`canonicalizeChatJidForProfile`) so incoming LID messages are not dropped/split.
4. Message is inserted/updated in `messagesByChat`.
5. Chat summary is updated (`lastMessageTimestampMs`, preview, unread).
6. `scheduleWaChatSync(profileId, reason)` notifies renderer.

Renderer side:
- On `wa:chatSync`, renderer calls `refreshWaChats(...)` and `refreshWaMessages(...)` as needed.

## 5. Sending Messages

### 5.1 Chat composer send

Renderer:
- `sendWaComposerMessage()` in `renderer/renderer.js`
- Calls `window.api.waSendChatMessage({ chatJid, text, attachment })`

Main:
- IPC: `wa:sendChatMessage`
- Handler: `sendChatMessage(payload)` in `main.js`

`sendChatMessage` behavior:
- Validates connected socket and target chat.
- Builds proper Baileys payload for:
  - text
  - image/video/audio/document attachments
  - caption where supported
- Sends via `sock.sendMessage(...)`
- Immediately upserts the sent message into local store for fast UI reflection.

### 5.2 Batch send

- `wa:sendBatch` / `wa:sendPreparedBatch` in `main.js`
- Uses `sendText(msisdn, text)` with pacing and progress events (`batch:progress`).

## 6. Read State Management

Renderer:
- `refreshWaMessages(...)` optionally calls `waMarkChatRead` when the active chat is open.

Main:
- IPC: `wa:markChatRead`
- Handler: `markChatReadForProfile(profileId, chatJid)`

Read strategy in `markChatReadForProfile`:
- Marks unread incoming messages as read in local cache.
- Attempts network read operations with fallbacks:
  - `sock.readMessages(...)` when available
  - `sock.sendReceipt(..., "read")` fallback
  - `sock.chatModify({ markRead: true, ... })` fallback
- Forces chat `unreadCount` to `0` locally and emits sync.

## 7. Typing Presence

### 7.1 Outgoing typing (you are typing)

Renderer:
- `handleWaComposerInputTyping()`
- `startWaOutgoingTyping(chatJid)`
- `stopWaOutgoingTyping({ sendPaused })`

Behavior:
- On composer input with non-empty text -> send `composing`.
- On pause/blur/send/chat switch -> send `paused`.
- Heartbeat keeps `composing` alive while still typing.

IPC:
- `waSendPresence(payload)` in preload -> `wa:sendPresence` in main.
- Main uses `sendChatPresence(payload)` -> `sock.sendPresenceUpdate(type, chatJid)`.

There is also compatibility endpoint:
- `waSetTyping` -> `wa:setTyping` -> `sendChatTypingPresence`.

### 7.2 Incoming typing (other party typing)

Main:
- `sock.ev.on("presence.update", ...)`
- Normalizes presence and emits `wa:presence` to renderer.

Renderer:
- `window.api.onWaPresence(...)`
- `setWaPresenceFromEvent(payload)` updates per-chat presence state.
- `renderWaConversationHead()` displays typing/recording text in header meta.

## 8. Per-chat History Reset and Fresh Reload

Renderer:
- Header button `Reset` -> `resetActiveWaChatHistory()`
- Calls `waResetChatHistory({ chatJid, maxPages, perPage })`

Main:
- IPC: `wa:resetChatHistory`
- Handler: `resetChatHistoryForProfile(profileId, chatJid, options)`

What reset does:
1. Clears cached messages for that chat only.
2. Triggers app-state resync (when connected).
3. Requests message history pages for that chat (`fetchMessageHistory` loop).
4. Emits sync so renderer refreshes chat/messages.

## 9. IPC Surface (WA-related)

Renderer -> main invokes:
- `wa:handshake`
- `wa:autoReconnect`
- `wa:getConnectionState`
- `wa:getRecentChats`
- `wa:getChatMessages`
- `wa:markChatRead`
- `wa:sendPresence`
- `wa:setTyping`
- `wa:sendChatMessage`
- `wa:resetChatHistory`
- `wa:pickAttachment`

Main -> renderer events:
- `wa:status`
- `wa:qr`
- `wa:pairingCode`
- `wa:chatSync`
- `wa:presence`

## 10. Troubleshooting Notes

If incoming messages are missing but outgoing works:
- Check JID handling/canonicalization first.
- New WA traffic may include LID addresses; they must not be dropped.
- Ensure incoming events (`messages.upsert` / `messages.set`) are reaching `upsertMessagesForProfile`.

If chat list updates but message pane is stale:
- Verify request sequence guards in renderer (`waMessagesReqSeq`, `waChatsReqSeq`) are not being bypassed.

If typing indicator not visible remotely:
- Confirm renderer input triggers `wa:sendPresence` and main sends `sock.sendPresenceUpdate("composing"/"paused")`.
- Ensure chat JID is valid and socket is connected.

## 11. Development Commands

Run app:
```bash
npm install
npm start
```

Build installer:
```bash
npm run dist
```
