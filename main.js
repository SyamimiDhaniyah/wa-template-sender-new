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
  _baileys = {
    makeWASocket: mod.default,
    useMultiFileAuthState: mod.useMultiFileAuthState,
    DisconnectReason: mod.DisconnectReason,
    Browsers: mod.Browsers
  };
  return _baileys;
}

const log = pino({ level: "info" });

let win = null;
let sock = null;
let isConnecting = false;

const store = new Store();

const userDataDir = app.getPath("userData");
const profilesRootDir = path.join(userDataDir, "wa_profiles");

const dataDir = path.join(__dirname, "data");
const templatesFile = path.join(dataDir, "templates.json");

function ensureDir(p) {
  if (!fs.existsSync(p)) fs.mkdirSync(p, { recursive: true });
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
            body: "Hi {name}, saya nak follow up pasal quotation {topic}. Awak free bila untuk saya explain ringkas?"
          },
          {
            id: "t2",
            name: "Appointment reminder",
            body: "Hi {name}, reminder appointment awak pada {date} jam {time}. Jika nak reschedule, reply ya."
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
  return JSON.parse(fs.readFileSync(templatesFile, "utf-8"));
}

function saveTemplates(templates) {
  ensureDataFiles();
  fs.writeFileSync(templatesFile, JSON.stringify(templates, null, 2), "utf-8");
}

function nowIsoShort() {
  const d = new Date();
  const pad = (n) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(
    d.getMinutes()
  )}:${pad(d.getSeconds())}`;
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

async function connectWA(method) {
  if (isConnecting) return;
  isConnecting = true;

  try {
    const activeProfileId = getActiveProfileId();
    const authDir = getProfileAuthDir(activeProfileId);
    ensureDir(authDir);

    const { makeWASocket, useMultiFileAuthState, DisconnectReason, Browsers } = await getBaileys();

    const { state, saveCreds } = await useMultiFileAuthState(authDir);

    // Pairing-code login is more strict about browser config
    const browser =
      method === "pairing" && Browsers && typeof Browsers.windows === "function"
        ? Browsers.windows("Google Chrome")
        : ["WA Template Sender", "Chrome", "1.0.0"];

    sock = makeWASocket({ auth: state, printQRInTerminal: false, logger: log, browser });

    sock.ev.on("creds.update", saveCreds);

    sock.ev.on("connection.update", async (update) => {
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
        if (connection === "connecting" || !!qr) {
          handshakeState.pairingRequested = true;
          try {
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
        win?.webContents.send("wa:status", { connected: true, text: "Connected", profileId: activeProfileId });
      }

      if (connection === "close") {
        const code = lastDisconnect?.error?.output?.statusCode;
        const reason = code || "unknown";
        win?.webContents.send("wa:status", {
          connected: false,
          text: `Disconnected (${reason})`,
          profileId: activeProfileId
        });

        // Do NOT auto-retry handshakes. User must click Handshake again.
        // This prevents infinite loops when WhatsApp rejects the session.
        const shouldReconnect = false;
        if (shouldReconnect) {
          setTimeout(() => connectWA(handshakeState.method).catch(() => {}), 1500);
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

  handshakeState = { method, phoneNumber, pairingRequested: false };

  // drop existing socket (forces new handshake)
  sock = null;

  await connectWA(method);
  return { ok: true, method };
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
  // Disconnect current socket so user can reconnect with selected profile
  sock = null;
  return { ok: true, activeProfileId: getActiveProfileId() };
});

ipcMain.handle("wa:handshake", async (_evt, payload) => {
  return await startHandshake(payload);
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
      const text = renderTemplate(templateBody, vars);

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
