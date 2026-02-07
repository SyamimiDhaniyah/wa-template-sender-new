const { app, BrowserWindow, ipcMain, dialog } = require("electron");
const path = require("path");
const fs = require("fs");
const qrcode = require("qrcode");
const Store = require("electron-store");
const pino = require("pino");

// Baileys is ESM-only in newer versions; load it via dynamic import from CommonJS
let _baileys = null;
async function getBaileys() {
  if (_baileys) return _baileys;
  const mod = await import("@whiskeysockets/baileys");
  const makeWASocket = mod.makeWASocket || mod.default;
  if (typeof makeWASocket !== "function") {
    throw new Error("Baileys makeWASocket export not found");
  }
  _baileys = {
    makeWASocket,
    useMultiFileAuthState: mod.useMultiFileAuthState,
    DisconnectReason: mod.DisconnectReason,
    Browsers: mod.Browsers,
    fetchLatestBaileysVersion: mod.fetchLatestBaileysVersion
  };
  return _baileys;
}

const log = pino({ level: "info" });

let win = null;
let sock = null;
let isConnecting = false;
let isConnected = false;

// Used to ignore late events from old sockets
let handshakeAttemptId = 0;

const store = new Store();

const userDataDir = app.getPath("userData");
const profilesRootDir = path.join(userDataDir, "wa_profiles");

const dataDir = path.join(__dirname, "data");
const templatesFile = path.join(dataDir, "templates.json");

function ensureDir(p) {
  if (!fs.existsSync(p)) fs.mkdirSync(p, { recursive: true });
}

function isValidTemplateVarName(name) {
  return /^[A-Za-z_][A-Za-z0-9_]*$/.test(String(name || ""));
}

function extractTemplateVars(body) {
  const set = new Set();
  const re = /\{(\w+)\}/g;
  const text = String(body || "");
  let m;
  while ((m = re.exec(text))) {
    if (isValidTemplateVarName(m[1])) set.add(m[1]);
  }
  return Array.from(set);
}

function normalizeTemplateRecord(raw, idx) {
  const t = raw && typeof raw === "object" ? raw : {};
  const body = String(t.body || "");

  const vars = new Set();
  for (const v of Array.isArray(t.variables) ? t.variables : []) {
    if (isValidTemplateVarName(v)) vars.add(String(v));
  }
  for (const v of extractTemplateVars(body)) vars.add(v);

  const sendPolicy = t.sendPolicy === "multiple" ? "multiple" : "once";
  const fallbackId = "t_" + String(idx + 1);

  return {
    id: String(t.id || fallbackId),
    name: String(t.name || "Untitled"),
    body,
    variables: Array.from(vars),
    sendPolicy
  };
}

function normalizeTemplatesList(input) {
  const list = Array.isArray(input) ? input : [];
  return list.map((t, idx) => normalizeTemplateRecord(t, idx));
}

function ensureDataFiles() {
  ensureDir(dataDir);
  if (!fs.existsSync(templatesFile)) {
    fs.writeFileSync(
      templatesFile,
      JSON.stringify(
        [
          {
            id: "t1",
            name: "Follow up quotation",
            body: "Hi {name}, saya nak follow up pasal quotation {topic}. Awak free bila untuk saya explain ringkas?",
            variables: ["name", "topic"],
            sendPolicy: "once"
          },
          {
            id: "t2",
            name: "Appointment reminder",
            body: "Hi {name}, reminder appointment awak pada {date} jam {time}. Jika nak reschedule, reply ya.",
            variables: ["name", "date", "time"],
            sendPolicy: "once"
          }
        ],
        null,
        2
      ),
      "utf-8"
    );
  }
}

function readTemplates() {
  ensureDataFiles();
  const raw = JSON.parse(fs.readFileSync(templatesFile, "utf-8"));
  const normalized = normalizeTemplatesList(raw);
  return normalized;
}

function saveTemplates(templates) {
  ensureDataFiles();
  const normalized = normalizeTemplatesList(templates);
  fs.writeFileSync(templatesFile, JSON.stringify(normalized, null, 2), "utf-8");
}

function nowIsoShort() {
  const d = new Date();
  const pad = (n) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(
    d.getMinutes()
  )}:${pad(d.getSeconds())}`;
}

function normalizeJidForContact(jid) {
  return String(jid || "").split(":")[0].trim();
}

function msisdnFromUserJid(jid) {
  const j = normalizeJidForContact(jid);
  if (!j.endsWith("@s.whatsapp.net")) return "";
  const raw = j.slice(0, j.indexOf("@"));
  if (!/^\d{6,}$/.test(raw)) return "";
  return raw;
}

function normalizeMsisdn(input) {
  let s = String(input || "").trim();
  s = s.replace(/\s+/g, "").replace(/-/g, "");
  if (s.startsWith("+")) s = s.slice(1);
  if (s.startsWith("0")) s = "60" + s.slice(1);
  s = s.replace(/\D/g, "");
  if (!s || s.length < 8) throw new Error("Invalid phone number");
  return s;
}

function renderTemplate(body, vars) {
  return String(body || "").replace(/\{(\w+)\}/g, (_, key) => {
    const v = vars && Object.prototype.hasOwnProperty.call(vars, key) ? String(vars[key]) : "";
    return v;
  });
}

function delayMsFromPattern(patternName, minSec, maxSec, idx) {
  const min = Math.max(1, Number(minSec || 7));
  const max = Math.max(min, Number(maxSec || 10));

  // random delay within range, inclusive
  if (patternName === "random") {
    const sec = min + Math.floor(Math.random() * (max - min + 1));
    return sec * 1000;
  }

  // deterministic cycle within range (useful for testing)
  if (patternName === "cycle") {
    const span = max - min + 1;
    const sec = min + (idx % span);
    return sec * 1000;
  }

  // fixed average
  const avg = Math.round((min + max) / 2);
  return avg * 1000;
}

/* --------------------------- Profiles (saved WhatsApp numbers) ---------------------------- */
function loadProfiles() {
  ensureDir(profilesRootDir);
  const profiles = store.get("profiles");
  if (Array.isArray(profiles) && profiles.length > 0) return profiles;
  const defaultProfile = { id: "p_default", name: "Default WhatsApp" };
  store.set("profiles", [defaultProfile]);
  store.set("activeProfileId", defaultProfile.id);
  return [defaultProfile];
}

function getActiveProfileId() {
  const profiles = loadProfiles();
  const activeId = store.get("activeProfileId");
  if (activeId && profiles.some((p) => p.id === activeId)) return activeId;
  store.set("activeProfileId", profiles[0].id);
  return profiles[0].id;
}

function getProfileAuthDir(profileId) {
  ensureDir(profilesRootDir);
  return path.join(profilesRootDir, profileId, "auth");
}

function setActiveProfileId(profileId) {
  const profiles = loadProfiles();
  if (!profiles.some((p) => p.id === profileId)) throw new Error("Profile not found");
  store.set("activeProfileId", profileId);
}

function createProfile(name) {
  const profiles = loadProfiles();
  const id = "p_" + Math.random().toString(16).slice(2) + "_" + Date.now().toString(16);
  const profile = { id, name: String(name || "New Profile").trim() || "New Profile" };
  profiles.push(profile);
  store.set("profiles", profiles);
  ensureDir(getProfileAuthDir(id));
  return profile;
}

function deleteProfile(profileId) {
  const profiles = loadProfiles();
  if (profiles.length <= 1) throw new Error("Cannot delete the last profile");
  const nextProfiles = profiles.filter((p) => p.id !== profileId);
  if (nextProfiles.length === profiles.length) throw new Error("Profile not found");
  store.set("profiles", nextProfiles);
  const activeId = getActiveProfileId();
  if (activeId === profileId) {
    store.set("activeProfileId", nextProfiles[0].id);
  }
  const profileDir = path.join(profilesRootDir, profileId);
  try {
    fs.rmSync(profileDir, { recursive: true, force: true });
  } catch (e) {
    // ignore
  }
  return true;
}

/* --------------------------- Sent Log key: templateId + "|" + msisdn -> lastSentAt ---------------------------- */
function sentKey(templateId, msisdn) {
  return `${String(templateId || "")}|${String(msisdn || "")}`;
}

function getSentLog() {
  const v = store.get("sentLog");
  if (v && typeof v === "object") return v;
  store.set("sentLog", {});
  return {};
}

function wasSent(templateId, msisdn) {
  const logObj = getSentLog();
  return !!logObj[sentKey(templateId, msisdn)];
}

function markSent(templateId, msisdn) {
  const logObj = getSentLog();
  logObj[sentKey(templateId, msisdn)] = nowIsoShort();
  store.set("sentLog", logObj);
}

function clearSentForTemplate(templateId) {
  const logObj = getSentLog();
  const prefix = `${String(templateId || "")}|`;
  for (const k of Object.keys(logObj)) {
    if (k.startsWith(prefix)) delete logObj[k];
  }
  store.set("sentLog", logObj);
  return true;
}

/* --------------------------- WhatsApp Contacts Cache (per profile) ---------------------------- */
function getContactsStoreObj() {
  const v = store.get("waContactsByProfile");
  if (v && typeof v === "object") return v;
  store.set("waContactsByProfile", {});
  return {};
}

function sanitizeContactName(v) {
  return String(v || "").trim();
}

function upsertContactsForProfile(profileId, contacts) {
  if (!profileId || !Array.isArray(contacts) || contacts.length === 0) return 0;

  const root = getContactsStoreObj();
  const byPhone = root[profileId] && typeof root[profileId] === "object" ? { ...root[profileId] } : {};
  let changed = 0;

  for (const c of contacts) {
    const jid = normalizeJidForContact(c?.jid || c?.id || "");
    const msisdn = msisdnFromUserJid(jid);
    if (!msisdn) continue;

    const existing = byPhone[msisdn] && typeof byPhone[msisdn] === "object" ? byPhone[msisdn] : {};
    const nameCandidate =
      sanitizeContactName(c?.name) ||
      sanitizeContactName(c?.notify) ||
      sanitizeContactName(c?.verifiedName) ||
      sanitizeContactName(existing?.name);

    const next = {
      msisdn,
      jid: jid || existing?.jid || `${msisdn}@s.whatsapp.net`,
      name: nameCandidate,
      updatedAt: nowIsoShort()
    };

    const prev = byPhone[msisdn];
    const same =
      prev &&
      prev.msisdn === next.msisdn &&
      prev.jid === next.jid &&
      String(prev.name || "") === String(next.name || "");

    if (!same) {
      byPhone[msisdn] = next;
      changed++;
    }
  }

  if (changed > 0) {
    root[profileId] = byPhone;
    store.set("waContactsByProfile", root);
  }

  return changed;
}

function upsertChatsAsContactsForProfile(profileId, chats) {
  if (!profileId || !Array.isArray(chats) || chats.length === 0) return 0;

  const contacts = [];
  for (const chat of chats) {
    const jid = normalizeJidForContact(chat?.id || chat?.jid || "");
    const msisdn = msisdnFromUserJid(jid);
    if (!msisdn) continue;

    const name =
      sanitizeContactName(chat?.name) ||
      sanitizeContactName(chat?.notify) ||
      sanitizeContactName(chat?.pushName) ||
      "";
    contacts.push({ id: jid, jid, name, notify: name });
  }

  return upsertContactsForProfile(profileId, contacts);
}

function getContactsForProfile(profileId) {
  const root = getContactsStoreObj();
  const byPhone = root[profileId] && typeof root[profileId] === "object" ? root[profileId] : {};
  const rows = Object.values(byPhone).filter((x) => x && typeof x === "object");

  rows.sort((a, b) => {
    const an = String(a?.name || "").toLowerCase();
    const bn = String(b?.name || "").toLowerCase();
    if (an && bn) return an.localeCompare(bn);
    if (an) return -1;
    if (bn) return 1;
    return String(a?.msisdn || "").localeCompare(String(b?.msisdn || ""));
  });

  return rows;
}

/* --------------------------- AI Rewrite (via external backend) ---------------------------- */
const DEFAULT_AI_REWRITE_CONFIG = {
  enabled: false,
  endpoint: "https://xqoc-ewo0-x3u2.s2.xano.io/api:lY50ALPv/LLM",
  authToken: "",
  prompt: "{message}",
  timeoutMs: 30000,
  fallbackToOriginal: true
};

function normalizeAiRewriteConfig(input) {
  const cfg = input && typeof input === "object" ? input : {};
  const timeoutRaw = Number(cfg.timeoutMs);
  const timeoutMs = Number.isFinite(timeoutRaw) ? Math.min(120000, Math.max(3000, Math.round(timeoutRaw))) : 30000;
  const endpoint = String(cfg.endpoint || "").trim() || DEFAULT_AI_REWRITE_CONFIG.endpoint;

  return {
    enabled: !!cfg.enabled,
    endpoint,
    authToken: String(cfg.authToken || "").trim(),
    prompt: String(cfg.prompt || DEFAULT_AI_REWRITE_CONFIG.prompt).trim(),
    timeoutMs,
    fallbackToOriginal: cfg.fallbackToOriginal !== false
  };
}

function getAiRewriteConfig() {
  const raw = store.get("aiRewriteConfig");
  const cfg = normalizeAiRewriteConfig({ ...DEFAULT_AI_REWRITE_CONFIG, ...(raw || {}) });
  return cfg;
}

function setAiRewriteConfig(input) {
  const cfg = normalizeAiRewriteConfig({ ...getAiRewriteConfig(), ...(input || {}) });
  store.set("aiRewriteConfig", cfg);
  return cfg;
}

function extractTextFromAnyContent(content) {
  if (typeof content === "string") return content.trim();
  if (Array.isArray(content)) {
    const parts = [];
    for (const item of content) {
      if (typeof item === "string") {
        if (item.trim()) parts.push(item.trim());
        continue;
      }
      if (item && typeof item === "object") {
        const txt = String(item.text || item.content || "").trim();
        if (txt) parts.push(txt);
      }
    }
    return parts.join("\n").trim();
  }
  return "";
}

function extractAssistantTextFromApiResponse(data) {
  if (!data) return "";

  if (typeof data === "string") return data.trim();

  // Xano custom shape
  const xanoReply = String(data?.reply || data?.data?.reply || "").trim();
  if (xanoReply) return xanoReply;

  // Chat Completions style (OpenAI/DeepSeek compatible)
  const chatContent = data?.choices?.[0]?.message?.content;
  const fromChatContent = extractTextFromAnyContent(chatContent);
  if (fromChatContent) return fromChatContent;

  const chatText = String(data?.choices?.[0]?.text || "").trim();
  if (chatText) return chatText;

  // Responses API / custom wrappers
  const outputText = String(data?.output_text || data?.response?.output_text || "").trim();
  if (outputText) return outputText;

  const nested = data?.output?.[0]?.content?.[0]?.text;
  const nestedText = String(nested || "").trim();
  if (nestedText) return nestedText;

  return "";
}

async function rewriteMessageViaBackend({
  endpoint,
  authToken,
  prompt,
  message,
  variables,
  msisdn,
  templateId,
  timeoutMs
}) {
  if (!endpoint) throw new Error("AI rewrite endpoint is not set");
  const finalPrompt = renderTemplate(String(prompt || "{message}"), {
    ...(variables && typeof variables === "object" ? variables : {}),
    message: String(message || ""),
    msisdn: String(msisdn || ""),
    phone: String(msisdn || ""),
    templateId: String(templateId || "")
  }).trim();
  if (!finalPrompt) throw new Error("AI rewrite prompt is empty");

  const controller = new AbortController();
  const timeoutValue = Number.isFinite(Number(timeoutMs)) ? Number(timeoutMs) : 30000;
  const timeout = setTimeout(() => controller.abort(), timeoutValue);
  try {
    const headers = { "Content-Type": "application/json" };
    if (authToken) {
      headers.Authorization = authToken.startsWith("Bearer ") ? authToken : `Bearer ${authToken}`;
    }

    // Xano contract: one input field named "prompt"
    const payload = { prompt: finalPrompt };

    const res = await fetch(endpoint, {
      method: "POST",
      headers,
      body: JSON.stringify(payload),
      signal: controller.signal
    });

    const raw = await res.text();
    let data = null;
    try {
      data = raw ? JSON.parse(raw) : {};
    } catch (e) {
      throw new Error(`AI backend returned non-JSON response (${res.status})`);
    }

    if (!res.ok) {
      const msg =
        extractAssistantTextFromApiResponse(data) ||
        String(data?.error || "").trim() ||
        String(data?.error?.message || data?.message || data?.error || "").trim() ||
        `HTTP ${res.status}`;
      throw new Error(`AI backend error: ${msg}`);
    }

    if (String(data?.status || "").toLowerCase() === "error") {
      const msg =
        String(data?.message || data?.error || "").trim() ||
        "Xano returned error status";
      throw new Error(`AI backend error: ${msg}`);
    }

    const rewritten = extractAssistantTextFromApiResponse(data);
    if (!rewritten) {
      throw new Error("AI backend response missing rewritten message text");
    }

    return rewritten;
  } catch (e) {
    if (e?.name === "AbortError") {
      throw new Error("AI rewrite timeout");
    }
    throw e;
  } finally {
    clearTimeout(timeout);
  }
}

/* --------------------------- CSV Import Basic CSV parser that supports commas and quotes. ---------------------------- */
function parseCsvText(text) {
  const rows = [];
  let row = [];
  let field = "";
  let inQuotes = false;

  const pushField = () => {
    row.push(field);
    field = "";
  };
  const pushRow = () => {
    rows.push(row);
    row = [];
  };

  for (let i = 0; i < text.length; i++) {
    const c = text[i];

    if (inQuotes) {
      if (c === '"') {
        const next = text[i + 1];
        if (next === '"') {
          field += '"';
          i++;
        } else {
          inQuotes = false;
        }
      } else {
        field += c;
      }
      continue;
    }

    if (c === '"') {
      inQuotes = true;
      continue;
    }

    if (c === ",") {
      pushField();
      continue;
    }

    if (c === "\r") continue;
    if (c === "\n") {
      pushField();
      pushRow();
      continue;
    }

    field += c;
  }

  pushField();
  if (row.length > 1 || (row.length === 1 && row[0] !== "")) pushRow();

  // Trim each field
  return rows.map((r) => r.map((x) => String(x || "").trim()));
}

function mapCsvToRecipients(csvRows, mapping) {
  // mapping: { hasHeader, phoneCol, varCols: {name: colIndex, topic: colIndex ...}}
  const hasHeader = !!mapping.hasHeader;
  const phoneCol = Number(mapping.phoneCol);
  const varCols = mapping.varCols && typeof mapping.varCols === "object" ? mapping.varCols : {};
  const startIndex = hasHeader ? 1 : 0;
  const recipients = [];
  const varsByPhone = {};

  for (let i = startIndex; i < csvRows.length; i++) {
    const r = csvRows[i];
    const rawPhone = r[phoneCol] || "";
    if (!rawPhone) continue;

    const phoneNorm = normalizeMsisdn(rawPhone);
    recipients.push(phoneNorm);

    const vars = {};
    for (const key of Object.keys(varCols)) {
      const idx = Number(varCols[key]);
      if (!Number.isFinite(idx)) continue;
      const val = r[idx];
      if (val !== undefined && val !== null && String(val).trim() !== "") {
        vars[key] = String(val).trim();
      }
    }

    if (Object.keys(vars).length > 0) {
      varsByPhone[phoneNorm] = vars;
    }
  }

  return { recipients, varsByPhone };
}

/* --------------------------- WhatsApp Connection ---------------------------- */
let handshakeState = {
  method: "qr", // 'qr' | 'pairing'
  phoneNumber: "", // E.164 digits, no plus
  pairingRequested: false
};

function normalizeE164NoPlus(input) {
  let s = String(input || "").trim();
  s = s.replace(/\s+/g, "").replace(/-/g, "");
  if (s.startsWith("+")) s = s.slice(1);
  // Malaysia convenience
  if (s.startsWith("0")) s = "60" + s.slice(1);
  s = s.replace(/\D/g, "");
  if (!s || s.length < 8) throw new Error("Invalid phone number");
  return s;
}

async function stopSocket() {
  const s = sock;
  sock = null;
  if (!s) return;

  try {
    try {
      s.ev?.removeAllListeners?.();
    } catch (e) {
      // ignore
    }
    try {
      s.ws?.close?.();
    } catch (e) {
      // ignore
    }
    try {
      s.end?.();
    } catch (e) {
      // ignore
    }
  } finally {
    isConnected = false;
  }
}

function clearAuthDir(authDir) {
  try {
    fs.rmSync(authDir, { recursive: true, force: true });
  } catch (e) {
    // ignore
  }
  ensureDir(authDir);
}

function hasRegisteredSession(profileId) {
  try {
    const authDir = getProfileAuthDir(profileId);
    const credsPath = path.join(authDir, "creds.json");
    if (!fs.existsSync(credsPath)) return false;
    const creds = JSON.parse(fs.readFileSync(credsPath, "utf-8"));
    return !!creds?.registered;
  } catch (e) {
    return false;
  }
}

async function connectWA(method, attemptId) {
  if (isConnecting) return;
  isConnecting = true;

  try {
    const activeProfileId = getActiveProfileId();
    const authDir = getProfileAuthDir(activeProfileId);
    ensureDir(authDir);

    const {
      makeWASocket,
      useMultiFileAuthState,
      DisconnectReason,
      Browsers,
      fetchLatestBaileysVersion
    } = await getBaileys();
    const { state, saveCreds } = await useMultiFileAuthState(authDir);

    // Always use a realistic browser signature (helps both QR and pairing)
    const browser =
      Browsers && typeof Browsers.windows === "function"
        ? Browsers.windows("Google Chrome")
        : ["Windows", "Chrome", "124.0.0.0"];

    // UI: show something immediately
    win?.webContents.send("wa:status", {
      connected: false,
      text: "Connecting...",
      profileId: activeProfileId
    });

    // Ensure only one live socket
    await stopSocket();

    let waVersion;
    try {
      const latest = await fetchLatestBaileysVersion();
      if (Array.isArray(latest?.version) && latest.version.length === 3) {
        waVersion = latest.version;
      }
    } catch (e) {
      log.warn({ err: e }, "Unable to fetch latest WA Web version, continuing with Baileys default");
    }

    sock = makeWASocket({
      auth: state,
      printQRInTerminal: false,
      logger: log,
      browser,
      syncFullHistory: true,
      ...(waVersion ? { version: waVersion } : {})
    });

    sock.ev.on("creds.update", saveCreds);
    sock.ev.on("messaging-history.set", (history) => {
      upsertContactsForProfile(activeProfileId, history?.contacts || []);
      upsertChatsAsContactsForProfile(activeProfileId, history?.chats || []);
    });
    sock.ev.on("contacts.upsert", (contacts) => {
      upsertContactsForProfile(activeProfileId, contacts || []);
    });
    sock.ev.on("contacts.update", (contacts) => {
      upsertContactsForProfile(activeProfileId, contacts || []);
    });
    sock.ev.on("chats.upsert", (chats) => {
      upsertChatsAsContactsForProfile(activeProfileId, chats || []);
    });
    sock.ev.on("chats.update", (chats) => {
      upsertChatsAsContactsForProfile(activeProfileId, chats || []);
    });

    // Limit auto recovery loops per handshake attempt
    let autoRecoverCount = 0;

    sock.ev.on("connection.update", async (update) => {
      // Ignore late events from old attempts
      if (attemptId !== handshakeAttemptId) return;

      const { connection, lastDisconnect, qr } = update;

      // QR flow
      if (handshakeState.method === "qr" && qr) {
        const dataUrl = await qrcode.toDataURL(qr);
        win?.webContents.send("wa:qr", dataUrl);
        win?.webContents.send("wa:status", {
          connected: false,
          text: "Scan QR in WhatsApp",
          profileId: activeProfileId
        });
      }

      // Pairing code flow
      if (handshakeState.method === "pairing" && !handshakeState.pairingRequested) {
        const alreadyRegistered = !!sock?.authState?.creds?.registered;
        if (alreadyRegistered) {
          handshakeState.pairingRequested = true;
          win?.webContents.send("wa:status", {
            connected: false,
            text: "Already registered on this profile. Reset session if you want to pair again.",
            profileId: activeProfileId
          });
        } else if (connection === "connecting" || !!qr) {
          handshakeState.pairingRequested = true;

          try {
            if (!handshakeState.phoneNumber) throw new Error("Missing phone number for pairing mode");
            const code = await sock.requestPairingCode(handshakeState.phoneNumber);
            win?.webContents.send("wa:pairingCode", code);
            win?.webContents.send("wa:status", {
              connected: false,
              text: "Enter pairing code in WhatsApp",
              profileId: activeProfileId
            });
          } catch (e) {
            handshakeState.pairingRequested = false;
            win?.webContents.send("wa:status", {
              connected: false,
              text: `Pairing failed: ${String(e?.message || e)}`,
              profileId: activeProfileId
            });
          }
        }
      }

      if (connection === "open") {
        isConnected = true;
        win?.webContents.send("wa:status", {
          connected: true,
          text: "Connected",
          profileId: activeProfileId
        });
      }

      if (connection === "close") {
        isConnected = false;
        const code = lastDisconnect?.error?.output?.statusCode;
        const reason = DisconnectReason?.[code] || code || "unknown";

        win?.webContents.send("wa:status", {
          connected: false,
          text: `Disconnected (${reason})`,
          profileId: activeProfileId
        });

        // IMPORTANT: WhatsApp often forces a disconnect after scan/pair to restart the socket.
        // We must reconnect for restartRequired (515).
        if (code === DisconnectReason.restartRequired) {
          if (autoRecoverCount < 3) {
            autoRecoverCount++;
            await stopSocket();
            setTimeout(() => {
              connectWA(handshakeState.method, attemptId).catch(() => {});
            }, 700);
          }
          return;
        }

        // Recover from transient transport/server issues.
        const shouldRetry =
          code === DisconnectReason.connectionClosed ||
          code === DisconnectReason.connectionLost ||
          code === DisconnectReason.timedOut ||
          code === DisconnectReason.unavailableService ||
          code === 405;
        if (shouldRetry) {
          if (autoRecoverCount < 3) {
            autoRecoverCount++;
            await stopSocket();
            setTimeout(() => {
              connectWA(handshakeState.method, attemptId).catch(() => {});
            }, 1000);
          }
          return;
        }

        // If the session is invalid, clear auth and present fresh QR / pairing.
        const needsFreshAuth =
          code === DisconnectReason.loggedOut ||
          code === DisconnectReason.badSession ||
          code === DisconnectReason.multideviceMismatch ||
          code === DisconnectReason.forbidden;

        if (needsFreshAuth) {
          if (autoRecoverCount < 2) {
            autoRecoverCount++;
            await stopSocket();
            clearAuthDir(authDir);
            handshakeState.pairingRequested = false;
            setTimeout(() => {
              connectWA(handshakeState.method, attemptId).catch(() => {});
            }, 700);
          } else {
            win?.webContents.send("wa:status", {
              connected: false,
              text: "Session invalid. Please reset the profile session and handshake again.",
              profileId: activeProfileId
            });
          }
        }
      }
    });
  } finally {
    isConnecting = false;
  }
}

async function startHandshake(payload) {
  const method = payload?.method === "pairing" ? "pairing" : "qr";
  const phoneNumber = payload?.phoneNumber ? normalizeE164NoPlus(payload.phoneNumber) : "";

  handshakeAttemptId++;
  handshakeState = { method, phoneNumber, pairingRequested: false };

  // Close any existing socket cleanly
  await stopSocket();

  await connectWA(method, handshakeAttemptId);
  return { ok: true, method };
}

async function autoReconnectActiveProfile() {
  const activeProfileId = getActiveProfileId();
  if (!hasRegisteredSession(activeProfileId)) {
    win?.webContents.send("wa:status", {
      connected: false,
      text: "Not connected",
      profileId: activeProfileId
    });
    return { ok: false, skipped: true };
  }

  handshakeAttemptId++;
  handshakeState = { method: "qr", phoneNumber: "", pairingRequested: false };
  await stopSocket();
  await connectWA("qr", handshakeAttemptId);
  return { ok: true, started: true };
}

async function sendText(msisdn, text) {
  if (!sock) throw new Error("WhatsApp not connected");
  const jid = `${msisdn}@s.whatsapp.net`;
  await sock.sendMessage(jid, { text: String(text || "") });
}

/* --------------------------- IPC ---------------------------- */
ipcMain.handle("app:getTemplates", async () => readTemplates());

ipcMain.handle("app:saveTemplates", async (_evt, templates) => {
  saveTemplates(Array.isArray(templates) ? templates : []);
  return { ok: true };
});

ipcMain.handle("app:getAiRewriteConfig", async () => {
  return { ok: true, config: getAiRewriteConfig() };
});

ipcMain.handle("app:saveAiRewriteConfig", async (_evt, config) => {
  const saved = setAiRewriteConfig(config);
  return { ok: true, config: saved };
});

ipcMain.handle("app:getProfiles", async () => {
  const profiles = loadProfiles();
  const activeProfileId = getActiveProfileId();
  return { profiles, activeProfileId };
});

ipcMain.handle("app:createProfile", async (_evt, name) => {
  const profile = createProfile(name);
  return { ok: true, profile };
});

ipcMain.handle("app:deleteProfile", async (_evt, profileId) => {
  const ok = deleteProfile(profileId);
  return { ok };
});

ipcMain.handle("app:setActiveProfile", async (_evt, profileId) => {
  setActiveProfileId(profileId);
  await autoReconnectActiveProfile();
  return { ok: true, activeProfileId: getActiveProfileId() };
});

ipcMain.handle("wa:handshake", async (_evt, payload) => {
  return await startHandshake(payload);
});

ipcMain.handle("wa:autoReconnect", async () => {
  return await autoReconnectActiveProfile();
});

ipcMain.handle("wa:getContacts", async () => {
  const profileId = getActiveProfileId();
  const contacts = getContactsForProfile(profileId);
  return {
    ok: true,
    connected: !!isConnected,
    profileId,
    contacts,
    count: contacts.length
  };
});

ipcMain.handle("wa:clearSentForTemplate", async (_evt, templateId) => {
  clearSentForTemplate(templateId);
  return { ok: true };
});

ipcMain.handle("app:openCsvDialogAndParse", async (_evt, mapping) => {
  const res = await dialog.showOpenDialog({
    title: "Select recipients CSV",
    properties: ["openFile"],
    filters: [{ name: "CSV", extensions: ["csv"] }]
  });

  if (res.canceled || !res.filePaths || res.filePaths.length === 0) {
    return { ok: false, canceled: true };
  }

  const filePath = res.filePaths[0];
  const text = fs.readFileSync(filePath, "utf-8");
  const rows = parseCsvText(text);

  if (!rows || rows.length === 0) {
    return { ok: false, error: "CSV file is empty" };
  }

  const mapped = mapCsvToRecipients(rows, mapping || { hasHeader: true, phoneCol: 0, varCols: {} });

  return {
    ok: true,
    filePath,
    sampleHeader: rows[0] || [],
    rowCount: rows.length,
    recipients: mapped.recipients,
    varsByPhone: mapped.varsByPhone
  };
});

ipcMain.handle("wa:sendBatch", async (_evt, payload) => {
  const {
    templateId,
    templateBody,
    recipients,
    varsByPhone,
    aiRewrite,
    pacing = { pattern: "cycle", minSec: 7, maxSec: 10 },
    safety = { maxRecipients: 200 },
    skipAlreadySent = true
  } = payload || {};

  if (!templateId) throw new Error("Missing templateId");
  if (!Array.isArray(recipients) || recipients.length === 0) throw new Error("No recipients");

  const maxRecipients = Math.max(1, Number(safety?.maxRecipients ?? 200));
  if (recipients.length > maxRecipients) throw new Error(`Too many recipients. Limit is ${maxRecipients}.`);

  const pattern = pacing.pattern || "cycle";
  const minSec = pacing.minSec ?? 7;
  const maxSec = pacing.maxSec ?? 10;
  const aiCfg = normalizeAiRewriteConfig({ ...getAiRewriteConfig(), ...(aiRewrite || {}) });
  if (aiCfg.enabled && !aiCfg.endpoint) {
    throw new Error("AI rewrite is enabled but backend endpoint is empty");
  }
  if (aiCfg.enabled && !aiCfg.prompt) {
    throw new Error("AI rewrite is enabled but prompt is empty");
  }

  let sent = 0;
  let failed = 0;
  let skipped = 0;

  for (let i = 0; i < recipients.length; i++) {
    const rawPhone = recipients[i];
    let msisdn = "";

    try {
      msisdn = normalizeMsisdn(rawPhone);
    } catch (e) {
      failed++;
      win?.webContents.send("batch:progress", {
        ts: nowIsoShort(),
        index: i + 1,
        total: recipients.length,
        phone: String(rawPhone),
        status: "failed",
        error: "Invalid phone number"
      });
      continue;
    }

    if (skipAlreadySent && wasSent(templateId, msisdn)) {
      skipped++;
      win?.webContents.send("batch:progress", {
        ts: nowIsoShort(),
        index: i + 1,
        total: recipients.length,
        phone: msisdn,
        status: "skipped",
        error: "Already sent (this template)"
      });

      if (i < recipients.length - 1) {
        const ms = delayMsFromPattern(pattern, minSec, maxSec, i);
        await new Promise((r) => setTimeout(r, ms));
      }
      continue;
    }

    try {
      const vars = (varsByPhone && (varsByPhone[rawPhone] || varsByPhone[msisdn])) || {};
      const baseText = renderTemplate(templateBody, vars);
      let text = baseText;

      if (aiCfg.enabled) {
        const promptVars = { ...(vars || {}), msisdn, phone: msisdn, templateId };
        const promptText = renderTemplate(aiCfg.prompt, promptVars);

        try {
          text = await rewriteMessageViaBackend({
            endpoint: aiCfg.endpoint,
            authToken: aiCfg.authToken,
            prompt: promptText,
            message: baseText,
            variables: vars,
            msisdn,
            templateId,
            timeoutMs: aiCfg.timeoutMs
          });
        } catch (aiErr) {
          if (!aiCfg.fallbackToOriginal) {
            throw new Error(`AI rewrite failed: ${String(aiErr?.message || aiErr)}`);
          }

          text = baseText;
          win?.webContents.send("batch:progress", {
            ts: nowIsoShort(),
            index: i + 1,
            total: recipients.length,
            phone: msisdn,
            status: "sending",
            error: `AI fallback: ${String(aiErr?.message || aiErr)}`
          });
        }
      }

      win?.webContents.send("batch:progress", {
        ts: nowIsoShort(),
        index: i + 1,
        total: recipients.length,
        phone: msisdn,
        status: "sending"
      });

      await sendText(msisdn, text);
      markSent(templateId, msisdn);
      sent++;

      win?.webContents.send("batch:progress", {
        ts: nowIsoShort(),
        index: i + 1,
        total: recipients.length,
        phone: msisdn,
        status: "sent"
      });
    } catch (e) {
      failed++;
      win?.webContents.send("batch:progress", {
        ts: nowIsoShort(),
        index: i + 1,
        total: recipients.length,
        phone: msisdn,
        status: "failed",
        error: String(e?.message || e)
      });
    }

    if (i < recipients.length - 1) {
      const ms = delayMsFromPattern(pattern, minSec, maxSec, i);
      await new Promise((r) => setTimeout(r, ms));
    }
  }

  return { ok: true, sent, failed, skipped };
});

/* --------------------------- Window ---------------------------- */
function createWindow() {
  win = new BrowserWindow({
    width: 1180,
    height: 800,
    minWidth: 1060,
    minHeight: 720,
    backgroundColor: "#0b0f19",
    webPreferences: {
      preload: path.join(__dirname, "preload.js"),
      contextIsolation: true,
      nodeIntegration: false
    }
  });

  win.loadFile(path.join(__dirname, "renderer", "index.html"));
}

app.whenReady().then(() => {
  ensureDataFiles();
  loadProfiles();
  createWindow();
});

app.on("window-all-closed", () => {
  if (process.platform !== "darwin") app.quit();
});
