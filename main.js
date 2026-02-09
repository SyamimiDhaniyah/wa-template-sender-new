const { app, BrowserWindow, ipcMain, dialog, nativeImage } = require("electron");
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
    fetchLatestBaileysVersion: mod.fetchLatestBaileysVersion,
    downloadMediaMessage: mod.downloadMediaMessage
  };
  return _baileys;
}

const log = pino({ level: "info" });

let win = null;
let sock = null;
let isConnecting = false;
let isConnected = false;
let connectSetupPromise = null;
let waContactsByProfileMem = {};
const invalidSessionProfiles = new Set();
const restartRecoveryDoneByAttempt = new Set();
let lastFetchedWaVersion = null;
let lastFetchedWaVersionAt = 0;
const contactNameSyncByProfile = new Map();
const lastContactNameSyncAtByProfile = new Map();

// Used to ignore late events from old sockets
let handshakeAttemptId = 0;
let startupReconnectScheduled = false;
const WA_CHAT_MAX_MESSAGES_PER_CHAT = 320;
const WA_HISTORY_LOOKBACK_DAYS = 7;
const WA_CHAT_STORE_KEY = "waChatByProfile";
const waChatByProfileMem = {};
const waChatSyncTimers = new Map();
const waHistoryWarmupInFlightByProfile = new Map();
const waHistoryWarmupInFlightByChat = new Map();
const waImageAutoSaveInFlight = new Set();
let waChatPersistTimer = null;

const store = new Store();

const userDataDir = app.getPath("userData");
const profilesRootDir = path.join(userDataDir, "wa_profiles");

const dataDir = path.join(__dirname, "data");
const templatesFile = path.join(dataDir, "templates.json");
const CLINIC_API_BASE = "https://xqoc-ewo0-x3u2.s2.xano.io";
const CLINIC_TZ = "Asia/Kuala_Lumpur";
const CLINIC_AUTH_SESSION_KEY = "clinicAuthSession";
const CLINIC_SETTINGS_KEY = "clinicSettings";
const CLINIC_APPOINTMENT_TEMPLATES_KEY = "clinicAppointmentTemplates";
const CLINIC_API_KEY_AUTH = "api:s4bMNy03";
const CLINIC_API_KEY_DATA = "api:lY50ALPv";

const DEFAULT_CLINIC_SETTINGS = {
  timezone: CLINIC_TZ,
  gapMinSec: 7,
  gapMaxSec: 45,
  marketingMonthsAgoDefault: 6
};

const DEFAULT_APPOINTMENT_TEMPLATES = {
  remindAppointment: {
    bahasa: [
      "Assalamualaikum/Selamat sejahtera {name}. Anda ada temujanji di {branch}.",
      "",
      "Tarikh: {date} ({weekday})",
      "Masa: {time}",
      "",
      "Anda digalakkan hadir 15 minit awal dan tidak lewat lebih dari 10 minit dari waktu temujanji. Jika lewat, temujanji mungkin akan terpaksa dibatalkan jika terdapat kekangan waktu di klinik.",
      "",
      "Maklumkan kepada kami sekiranya anda tidak dapat menghadiri temujanji ye.",
      "",
      "Lokasi kami : {branch} {address}",
      "No Tel: {branch_phone}",
      "Google Map: {google_direction}",
      "Waze: {waze_direction}",
      "",
      "Jumpa nanti."
    ].join("\n"),
    english: [
      "Hello {title_en} {name}. Your appointment at {branch} is confirmed.",
      "",
      "Date: {date} ({weekday})",
      "Time: {time}",
      "",
      "You are encouraged to arrive 15 minutes early and not later than 10 minutes past the appointment time. If delayed, the appointment may need to be canceled due to time constraints at the clinic.",
      "",
      "Please inform us if you are unable to attend the appointment.",
      "",
      "Our location : {branch} {address}",
      "No Tel: {branch_phone}",
      "Google Map: {google_direction}",
      "Waze: {waze_direction}",
      "",
      "See you."
    ].join("\n")
  },
  followUp: {
    bahasa: [
      "Assalamualaikum/Selamat sejahtera {title_bm} {name}.",
      "",
      "Saya staf dari {branch}.",
      "",
      "Kami nak follow up selepas sesi rawatan semalam. Macam mana keadaan gigi selepas buat rawatan semalam ye? Ada rasa sakit / tak selesa ke?"
    ].join("\n"),
    english: [
      "Hello {title_en} {name}.",
      "",
      "I'm from {branch}.",
      "",
      "We would like to follow up after yesterday's treatment session. How is the condition of your teeth after the treatment yesterday? Do you feel any pain or discomfort?"
    ].join("\n")
  },
  requestReview: {
    bahasa: [
      "Kalau {title_bm} kelapangan, minta jasa baik untuk bagi kami review di bawah untuk cawangan {branch} dan {dentist} ye.",
      "",
      "Google: {google_review_link}",
      "Facebook: https://bit.ly/3rYsqvy",
      "",
      "Review tau, ianya akan sangat membantu kami hehe.",
      "",
      "Terima kasih."
    ].join("\n"),
    english: [
      "If you have the time, we kindly request that you provide us with a review for {branch} and {dentist} through the following links:",
      "",
      "Google: {google_review_link}",
      "Facebook: https://bit.ly/3rYsqvy",
      "",
      "We will really appreciate it as the review will really help us.",
      "",
      "Thank you in advance."
    ].join("\n")
  }
};

function ensureDir(p) {
  if (!fs.existsSync(p)) fs.mkdirSync(p, { recursive: true });
}

function cleanString(v) {
  return String(v || "").trim();
}

function clampInt(raw, min, max, fallback) {
  const n = Number(raw);
  if (!Number.isFinite(n)) return fallback;
  const rounded = Math.round(n);
  return Math.min(max, Math.max(min, rounded));
}

function normalizeClinicSettings(input) {
  const src = input && typeof input === "object" ? input : {};
  const gapMinSec = clampInt(src.gapMinSec, 7, 45, DEFAULT_CLINIC_SETTINGS.gapMinSec);
  const gapMaxCandidate = clampInt(src.gapMaxSec, 7, 45, DEFAULT_CLINIC_SETTINGS.gapMaxSec);
  const gapMaxSec = Math.max(gapMinSec, gapMaxCandidate);
  const marketingMonthsAgoDefault = clampInt(src.marketingMonthsAgoDefault, 1, 24, 6);

  return {
    timezone: CLINIC_TZ,
    gapMinSec,
    gapMaxSec,
    marketingMonthsAgoDefault
  };
}

function getClinicSettings() {
  const raw = store.get(CLINIC_SETTINGS_KEY);
  return normalizeClinicSettings({ ...DEFAULT_CLINIC_SETTINGS, ...(raw || {}) });
}

function saveClinicSettings(input) {
  const next = normalizeClinicSettings({ ...getClinicSettings(), ...(input || {}) });
  store.set(CLINIC_SETTINGS_KEY, next);
  return next;
}

function normalizeAuthUser(raw) {
  const src = raw && typeof raw === "object" ? raw : {};
  const out = {};
  const keys = ["name", "email", "nickname", "Gender", "Role", "Branch", "dept", "Access"];
  for (const key of keys) {
    if (!Object.prototype.hasOwnProperty.call(src, key)) continue;
    const val = src[key];
    if (Array.isArray(val)) out[key] = val.map((x) => cleanString(x)).filter(Boolean);
    else if (val === null || val === undefined) out[key] = "";
    else out[key] = cleanString(val);
  }
  return out;
}

function normalizeAuthSession(raw) {
  const src = raw && typeof raw === "object" ? raw : {};
  const authToken = cleanString(src.authToken);
  return {
    authToken,
    user: normalizeAuthUser(src.user),
    loggedInAt: cleanString(src.loggedInAt)
  };
}

function getAuthSession() {
  const raw = store.get(CLINIC_AUTH_SESSION_KEY);
  return normalizeAuthSession(raw || {});
}

function saveAuthSession(authToken, user) {
  const session = normalizeAuthSession({
    authToken,
    user,
    loggedInAt: nowIsoShort()
  });
  store.set(CLINIC_AUTH_SESSION_KEY, session);
  return session;
}

function clearAuthSession() {
  store.set(CLINIC_AUTH_SESSION_KEY, { authToken: "", user: {}, loggedInAt: "" });
}

function normalizeAppointmentTemplateEntry(rawEntry, fallback) {
  const src = rawEntry && typeof rawEntry === "object" ? rawEntry : {};
  return {
    bahasa: cleanString(src.bahasa) || fallback.bahasa,
    english: cleanString(src.english) || fallback.english
  };
}

function normalizeAppointmentTemplates(input) {
  const src = input && typeof input === "object" ? input : {};
  return {
    remindAppointment: normalizeAppointmentTemplateEntry(
      src.remindAppointment,
      DEFAULT_APPOINTMENT_TEMPLATES.remindAppointment
    ),
    followUp: normalizeAppointmentTemplateEntry(src.followUp, DEFAULT_APPOINTMENT_TEMPLATES.followUp),
    requestReview: normalizeAppointmentTemplateEntry(src.requestReview, DEFAULT_APPOINTMENT_TEMPLATES.requestReview)
  };
}

function getAppointmentTemplates() {
  const raw = store.get(CLINIC_APPOINTMENT_TEMPLATES_KEY);
  return normalizeAppointmentTemplates(raw || {});
}

function saveAppointmentTemplates(input) {
  const next = normalizeAppointmentTemplates({ ...getAppointmentTemplates(), ...(input || {}) });
  store.set(CLINIC_APPOINTMENT_TEMPLATES_KEY, next);
  return next;
}

function clinicAuthHeaders(authToken, contentTypeJson = true) {
  const headers = {};
  if (contentTypeJson) headers["Content-Type"] = "application/json";
  const token = cleanString(authToken);
  if (token) headers.Authorization = token.startsWith("Bearer ") ? token : `Bearer ${token}`;
  return headers;
}

async function clinicFetchJson(url, options) {
  const opts = options && typeof options === "object" ? options : {};
  const res = await fetch(url, opts);
  const raw = await res.text();
  let data = {};
  try {
    data = raw ? JSON.parse(raw) : {};
  } catch (e) {
    throw new Error(`API returned non-JSON response (${res.status})`);
  }

  if (!res.ok) {
    const msg =
      cleanString(data?.message) ||
      cleanString(data?.error?.message) ||
      cleanString(data?.error) ||
      `HTTP ${res.status}`;
    const err = new Error(msg);
    err.statusCode = res.status;
    err.apiCode = cleanString(data?.code);
    err.url = url;
    throw err;
  }

  if (String(data?.status || "").toLowerCase() === "error") {
    const msg = cleanString(data?.message) || cleanString(data?.error) || "API returned error status";
    const err = new Error(msg);
    err.statusCode = res.status;
    err.apiCode = cleanString(data?.code);
    err.url = url;
    throw err;
  }

  return data;
}

function clinicIsRouteNotFoundError(err) {
  const msg = String(err?.message || "");
  const apiCode = String(err?.apiCode || "");
  const statusCode = Number(err?.statusCode || 0);
  return statusCode === 404 || apiCode === "ERROR_CODE_NOT_FOUND" || /Unable to locate request/i.test(msg);
}

async function clinicFetchJsonWithFallback(urls, options) {
  const list = Array.isArray(urls) ? urls.filter(Boolean) : [urls].filter(Boolean);
  if (list.length === 0) throw new Error("No API endpoint configured");

  let lastErr = null;
  for (const url of list) {
    try {
      return await clinicFetchJson(url, options);
    } catch (err) {
      if (!clinicIsRouteNotFoundError(err)) throw err;
      lastErr = err;
    }
  }

  const tried = list.join(", ");
  throw new Error(`Unable to locate request. Tried: ${tried}`);
}

async function clinicLogin(email, password) {
  const endpoint = `${CLINIC_API_BASE}/api:s4bMNy03/auth/login`;
  const payload = {
    email: cleanString(email),
    password: cleanString(password)
  };
  if (!payload.email || !payload.password) throw new Error("Email and password are required");

  const data = await clinicFetchJson(endpoint, {
    method: "POST",
    headers: clinicAuthHeaders("", true),
    body: JSON.stringify(payload)
  });

  const token = cleanString(data?.authToken || data?.token);
  if (!token) throw new Error("Login succeeded but token is missing");
  return token;
}

async function clinicGetMe(authToken) {
  const endpoint = `${CLINIC_API_BASE}/api:lY50ALPv/auth/me`;
  const data = await clinicFetchJson(endpoint, {
    method: "GET",
    headers: clinicAuthHeaders(authToken, false)
  });
  return normalizeAuthUser(data || {});
}

function normalizeBranchRecord(raw) {
  const src = raw && typeof raw === "object" ? raw : {};
  return {
    created_at: Number(src.created_at || 0) || 0,
    label: cleanString(src.label),
    address: cleanString(src.address),
    google_direction: cleanString(src.google_direction),
    waze_direction: cleanString(src.waze_direction),
    branch_phone: cleanString(src.branch_phone),
    Region: cleanString(src.Region),
    Google_Review: cleanString(src.Google_Review)
  };
}

async function clinicGetBranchList(authToken) {
  const endpoint = `${CLINIC_API_BASE}/api:lY50ALPv/branch_list`;
  const data = await clinicFetchJson(endpoint, {
    method: "GET",
    headers: clinicAuthHeaders(authToken, false)
  });
  return Array.isArray(data) ? data.map(normalizeBranchRecord).filter((x) => x.label) : [];
}

function normalizeAppointmentRecord(raw) {
  const src = raw && typeof raw === "object" ? raw : {};
  return {
    id: Number(src.id || 0) || 0,
    created_at: Number(src.created_at || 0) || 0,
    Appt_Date: Number(src.Appt_Date || 0) || 0,
    Appt_Start_Time: Number(src.Appt_Start_Time || 0) || 0,
    Appt_End_Time: Number(src.Appt_End_Time || 0) || 0,
    Branch_Name: cleanString(src.Branch_Name),
    Dentist_Name: cleanString(src.Dentist_Name),
    Patient_Name: cleanString(src.Patient_Name),
    Patient_Phone_No: cleanString(src.Patient_Phone_No),
    Treatment: cleanString(src.Treatment),
    Status: src.Status === true,
    ic_number: cleanString(src.ic_number)
  };
}

async function clinicGetAppointmentList(authToken, payload) {
  const src = payload && typeof payload === "object" ? payload : {};
  const branch = cleanString(src.branch);
  const date = Number(src.date || 0);
  if (!branch) throw new Error("Branch is required");
  if (!Number.isFinite(date) || date <= 0) throw new Error("Date timestamp is required");

  const qs = new URLSearchParams({
    branch,
    date: String(Math.round(date))
  });
  const endpoint = `${CLINIC_API_BASE}/api:lY50ALPv/appt_list?${qs.toString()}`;
  const data = await clinicFetchJson(endpoint, {
    method: "GET",
    headers: clinicAuthHeaders(authToken, false)
  });

  return Array.isArray(data) ? data.map(normalizeAppointmentRecord).filter((x) => x.id) : [];
}

function normalizePatientRecord(raw) {
  const src = raw && typeof raw === "object" ? raw : {};
  return {
    name: cleanString(src.name),
    ic_number: cleanString(src.ic_number),
    phone: cleanString(src.phone),
    gender: cleanString(src.gender).toLowerCase(),
    dob: cleanString(src.dob),
    age: Number(src.age || 0) || 0,
    address: cleanString(src.address),
    postcode: cleanString(src.postcode)
  };
}

async function clinicGetPatient(authToken, icNumber) {
  const endpoint = `${CLINIC_API_BASE}/api:lY50ALPv/patient`;
  const ic = cleanString(icNumber);
  if (!ic) throw new Error("IC number is required");
  const data = await clinicFetchJson(endpoint, {
    method: "POST",
    headers: clinicAuthHeaders(authToken, true),
    body: JSON.stringify({ ic_number: ic })
  });
  return normalizePatientRecord(data || {});
}

function normalizePastPatientRecord(raw) {
  const src = raw && typeof raw === "object" ? raw : {};
  return {
    Appt_Date: Number(src.Appt_Date || 0) || 0,
    Dentist_Name: cleanString(src.Dentist_Name),
    Patient_Name: cleanString(src.Patient_Name),
    Patient_Phone_No: cleanString(src.Patient_Phone_No)
  };
}

async function clinicGetPastPatients(authToken, payload) {
  const src = payload && typeof payload === "object" ? payload : {};
  const branch = cleanString(src.branch);
  const startDay = Number(src.start_day || 0);
  const endDay = Number(src.end_day || 0);
  if (!branch) throw new Error("Branch is required");
  if (!Number.isFinite(startDay) || startDay <= 0) throw new Error("start_day timestamp is required");
  if (!Number.isFinite(endDay) || endDay <= 0) throw new Error("end_day timestamp is required");

  const qs = new URLSearchParams({
    branch,
    start_day: String(Math.round(startDay)),
    end_day: String(Math.round(endDay))
  });
  const endpoint = `${CLINIC_API_BASE}/api:lY50ALPv/past_patient?${qs.toString()}`;
  const data = await clinicFetchJson(endpoint, {
    method: "GET",
    headers: clinicAuthHeaders(authToken, false)
  });

  return Array.isArray(data) ? data.map(normalizePastPatientRecord).filter((x) => x.Patient_Phone_No) : [];
}

async function getLatestWaVersionSafe(fetchLatestBaileysVersion) {
  const now = Date.now();
  // Reuse last known good version for 6 hours.
  if (lastFetchedWaVersion && now - lastFetchedWaVersionAt < 6 * 60 * 60 * 1000) {
    return lastFetchedWaVersion;
  }
  if (typeof fetchLatestBaileysVersion !== "function") return null;

  const timeoutMs = 4000;
  try {
    const timed = Promise.race([
      fetchLatestBaileysVersion(),
      new Promise((_, reject) => setTimeout(() => reject(new Error("version_fetch_timeout")), timeoutMs))
    ]);
    const latest = await timed;
    const version = Array.isArray(latest?.version) && latest.version.length === 3 ? latest.version : null;
    if (version) {
      lastFetchedWaVersion = version;
      lastFetchedWaVersionAt = now;
      return version;
    }
  } catch (e) {
    log.warn({ err: e }, "Failed to fetch latest WA Web version, using library default");
  }
  return null;
}

function clearPersistedContactsCache() {
  try {
    if (typeof store.delete === "function") store.delete("waContactsByProfile");
    else store.set("waContactsByProfile", {});
  } catch (e) {
    store.set("waContactsByProfile", {});
  }
  waContactsByProfileMem = {};
}

function normalizeContactsCacheRoot(raw) {
  const root = raw && typeof raw === "object" ? raw : {};
  const out = {};

  for (const [profileId, byPhoneRaw] of Object.entries(root)) {
    if (!profileId || !byPhoneRaw || typeof byPhoneRaw !== "object") continue;
    const byPhone = {};

    for (const [phoneKey, contactRaw] of Object.entries(byPhoneRaw)) {
      if (!contactRaw || typeof contactRaw !== "object") continue;
      let msisdn = "";
      try {
        msisdn = normalizeMsisdn(contactRaw.msisdn || phoneKey || "");
      } catch (e) {
        continue;
      }
      if (!msisdn) continue;

      const jid = normalizeJidForContact(contactRaw.jid || `${msisdn}@s.whatsapp.net`);
      byPhone[msisdn] = {
        msisdn,
        jid: jid || `${msisdn}@s.whatsapp.net`,
        lid: sanitizeContactName(contactRaw.lid),
        name: sanitizeContactName(contactRaw.name),
        notify: sanitizeContactName(contactRaw.notify),
        verifiedName: sanitizeContactName(contactRaw.verifiedName),
        imgUrl: contactRaw.imgUrl ? String(contactRaw.imgUrl).trim() || null : null,
        status: sanitizeContactName(contactRaw.status),
        photoCheckedAt: String(contactRaw.photoCheckedAt || "").trim() || null,
        updatedAt: String(contactRaw.updatedAt || "").trim() || nowIsoShort()
      };
    }

    if (Object.keys(byPhone).length > 0) out[profileId] = byPhone;
  }

  return out;
}

function persistContactsCache() {
  try {
    store.set("waContactsByProfile", waContactsByProfileMem);
  } catch (e) {
    log.warn({ err: e }, "Failed to persist WA contacts cache");
  }
}

function loadPersistedContactsCache() {
  try {
    waContactsByProfileMem = normalizeContactsCacheRoot(store.get("waContactsByProfile"));
  } catch (e) {
    waContactsByProfileMem = {};
  }
}

function clearContactsCacheForProfile(profileId) {
  if (!profileId) return;
  if (!waContactsByProfileMem || typeof waContactsByProfileMem !== "object") return;
  if (!Object.prototype.hasOwnProperty.call(waContactsByProfileMem, profileId)) return;
  delete waContactsByProfileMem[profileId];
  persistContactsCache();
}

function normalizePersistedChatSummary(raw, fallbackJid) {
  const src = raw && typeof raw === "object" ? raw : {};
  const jid = normalizeChatJid(src.jid || fallbackJid || "");
  if (!jid) return null;
  return {
    jid,
    name: cleanString(src.name || ""),
    lastMessageTimestampMs: Math.max(0, Number(src.lastMessageTimestampMs || 0) || 0),
    lastMessagePreview: String(src.lastMessagePreview || ""),
    lastMessageType: cleanString(src.lastMessageType || ""),
    lastMessageFromMe: src.lastMessageFromMe === true,
    unreadCount: Math.max(0, Number(src.unreadCount || 0) || 0),
    archived: src.archived === true,
    pinned: src.pinned === true,
    updatedAt: cleanString(src.updatedAt || nowIsoShort())
  };
}

function normalizePersistedChatMedia(raw) {
  const src = raw && typeof raw === "object" ? raw : {};
  const kind = cleanString(src.kind || "");
  const mimeType = cleanString(src.mimeType || "");
  const fileName = cleanString(src.fileName || "");
  const fileLength = Math.max(0, Number(src.fileLength || 0) || 0);
  const thumbnailDataUrl = cleanString(src.thumbnailDataUrl || "");
  const localPath = cleanString(src.localPath || "");
  if (!kind && !mimeType && !fileName && !fileLength && !thumbnailDataUrl && !localPath) return null;
  return {
    kind,
    mimeType,
    fileName,
    fileLength,
    thumbnailDataUrl,
    localPath
  };
}

function normalizePersistedChatMessage(raw, fallbackChatJid) {
  const src = raw && typeof raw === "object" ? raw : {};
  const key = normalizeMessageKey(src.key || {}, fallbackChatJid);
  const chatJid = normalizeChatJid(src.chatJid || key.remoteJid || fallbackChatJid || "");
  if (!chatJid || !key.id) return null;

  const timestampMs = Math.max(0, Number(src.timestampMs || 0) || 0);
  const type = cleanString(src.type || "unknown");
  const text = String(src.text || "");
  const preview = String(src.preview || text || "");
  const fromMe = src.fromMe === true || key.fromMe === true;
  const pushName = cleanString(src.pushName || "");
  const media = normalizePersistedChatMedia(src.media);
  const hasMedia = src.hasMedia === true || !!media;

  return {
    key: {
      remoteJid: chatJid,
      id: key.id,
      fromMe,
      participant: cleanString(key.participant || "")
    },
    hash: messageKeyHash({
      remoteJid: chatJid,
      id: key.id,
      fromMe,
      participant: cleanString(key.participant || "")
    }),
    chatJid,
    fromMe,
    pushName,
    timestampMs,
    type,
    text,
    preview,
    hasMedia,
    media,
    status: Math.max(0, Number(src.status || 0) || 0),
    rawMessage: null
  };
}

function normalizeWaChatCacheRoot(raw) {
  const srcRoot = raw && typeof raw === "object" ? raw : {};
  const out = {};

  for (const [profileId, profileRaw] of Object.entries(srcRoot)) {
    if (!profileId) continue;
    const profileObj = profileRaw && typeof profileRaw === "object" ? profileRaw : {};
    const chatsRaw = profileObj.chatsByJid && typeof profileObj.chatsByJid === "object" ? profileObj.chatsByJid : {};
    const messagesRaw =
      profileObj.messagesByChat && typeof profileObj.messagesByChat === "object" ? profileObj.messagesByChat : {};

    const chatsByJid = {};
    for (const [jidKey, chatRaw] of Object.entries(chatsRaw)) {
      const chat = normalizePersistedChatSummary(chatRaw, jidKey);
      if (!chat) continue;
      chatsByJid[chat.jid] = chat;
    }

    const messagesByChat = {};
    for (const [jidKey, listRaw] of Object.entries(messagesRaw)) {
      const chatJid = normalizeChatJid(jidKey);
      if (!chatJid || !Array.isArray(listRaw)) continue;
      const normalizedList = listRaw
        .map((m) => normalizePersistedChatMessage(m, chatJid))
        .filter((m) => !!m)
        .sort((a, b) => Number(a.timestampMs || 0) - Number(b.timestampMs || 0))
        .slice(-WA_CHAT_MAX_MESSAGES_PER_CHAT);
      if (normalizedList.length > 0) messagesByChat[chatJid] = normalizedList;
    }

    for (const [chatJid, list] of Object.entries(messagesByChat)) {
      if (Object.prototype.hasOwnProperty.call(chatsByJid, chatJid)) continue;
      const latest = Array.isArray(list) && list.length > 0 ? list[list.length - 1] : null;
      chatsByJid[chatJid] = {
        jid: chatJid,
        name: "",
        lastMessageTimestampMs: Number(latest?.timestampMs || 0) || 0,
        lastMessagePreview: String(latest?.preview || ""),
        lastMessageType: cleanString(latest?.type || ""),
        lastMessageFromMe: latest?.fromMe === true,
        unreadCount: 0,
        archived: false,
        pinned: false,
        updatedAt: nowIsoShort()
      };
    }

    if (Object.keys(chatsByJid).length > 0 || Object.keys(messagesByChat).length > 0) {
      out[profileId] = {
        chatsByJid,
        messagesByChat
      };
    }
  }

  return out;
}

function persistWaChatCache() {
  try {
    const root = {};
    for (const [profileId, profileState] of Object.entries(waChatByProfileMem || {})) {
      if (!profileId || !profileState || typeof profileState !== "object") continue;
      const chats = profileState.chatsByJid && typeof profileState.chatsByJid === "object" ? profileState.chatsByJid : {};
      const messages =
        profileState.messagesByChat && typeof profileState.messagesByChat === "object"
          ? profileState.messagesByChat
          : {};

      const chatsByJid = {};
      for (const [jid, chatRaw] of Object.entries(chats)) {
        const chat = normalizePersistedChatSummary(chatRaw, jid);
        if (!chat) continue;
        chatsByJid[chat.jid] = chat;
      }

      const messagesByChat = {};
      for (const [jid, listRaw] of Object.entries(messages)) {
        const chatJid = normalizeChatJid(jid);
        if (!chatJid || !Array.isArray(listRaw)) continue;
        const compact = listRaw
          .map((msg) => normalizePersistedChatMessage(msg, chatJid))
          .filter((msg) => !!msg)
          .sort((a, b) => Number(a.timestampMs || 0) - Number(b.timestampMs || 0))
          .slice(-WA_CHAT_MAX_MESSAGES_PER_CHAT)
          .map((msg) => ({
            ...msg,
            rawMessage: null
          }));
        if (compact.length > 0) messagesByChat[chatJid] = compact;
      }

      for (const [chatJid, list] of Object.entries(messagesByChat)) {
        if (Object.prototype.hasOwnProperty.call(chatsByJid, chatJid)) continue;
        const latest = Array.isArray(list) && list.length > 0 ? list[list.length - 1] : null;
        chatsByJid[chatJid] = {
          jid: chatJid,
          name: "",
          lastMessageTimestampMs: Number(latest?.timestampMs || 0) || 0,
          lastMessagePreview: String(latest?.preview || ""),
          lastMessageType: cleanString(latest?.type || ""),
          lastMessageFromMe: latest?.fromMe === true,
          unreadCount: 0,
          archived: false,
          pinned: false,
          updatedAt: nowIsoShort()
        };
      }

      if (Object.keys(chatsByJid).length > 0 || Object.keys(messagesByChat).length > 0) {
        root[profileId] = {
          chatsByJid,
          messagesByChat
        };
      }
    }
    store.set(WA_CHAT_STORE_KEY, root);
  } catch (e) {
    log.warn({ err: e }, "Failed to persist WA chat cache");
  }
}

function schedulePersistWaChatCache() {
  if (waChatPersistTimer) return;
  waChatPersistTimer = setTimeout(() => {
    waChatPersistTimer = null;
    persistWaChatCache();
  }, 260);
}

function loadPersistedWaChatCache() {
  try {
    const raw = store.get(WA_CHAT_STORE_KEY);
    const normalized = normalizeWaChatCacheRoot(raw);
    for (const key of Object.keys(waChatByProfileMem)) delete waChatByProfileMem[key];
    for (const [profileId, profileState] of Object.entries(normalized)) {
      waChatByProfileMem[profileId] = profileState;
    }
  } catch (e) {
    for (const key of Object.keys(waChatByProfileMem)) delete waChatByProfileMem[key];
  }
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

function getTemplateExportBundle() {
  return {
    exported_at: nowIsoShort(),
    timezone: CLINIC_TZ,
    marketingTemplates: readTemplates(),
    appointmentTemplates: getAppointmentTemplates()
  };
}

function importTemplateBundle(raw) {
  if (Array.isArray(raw)) {
    saveTemplates(raw);
    return {
      marketingCount: normalizeTemplatesList(raw).length,
      appointmentUpdated: false
    };
  }

  const src = raw && typeof raw === "object" ? raw : {};
  const marketingTemplates = Array.isArray(src.marketingTemplates)
    ? src.marketingTemplates
    : Array.isArray(src.templates)
      ? src.templates
      : null;
  const appointmentTemplates =
    src.appointmentTemplates && typeof src.appointmentTemplates === "object" ? src.appointmentTemplates : null;

  let marketingCount = readTemplates().length;
  let appointmentUpdated = false;

  if (marketingTemplates) {
    const normalizedMarketing = normalizeTemplatesList(marketingTemplates);
    saveTemplates(normalizedMarketing);
    marketingCount = normalizedMarketing.length;
  }

  if (appointmentTemplates) {
    saveAppointmentTemplates(appointmentTemplates);
    appointmentUpdated = true;
  }

  return { marketingCount, appointmentUpdated };
}

function nowIsoShort() {
  const d = new Date();
  const pad = (n) => String(n).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(
    d.getMinutes()
  )}:${pad(d.getSeconds())}`;
}

function normalizeJidForContact(jid) {
  const raw = String(jid || "").trim();
  if (!raw) return "";

  const atIndex = raw.indexOf("@");
  if (atIndex < 0) return raw;

  const userPart = raw.slice(0, atIndex);
  const serverPart = raw.slice(atIndex + 1);
  if (!serverPart) return raw;

  // Baileys can emit device-qualified JIDs like user:device@server.
  // Keep the server part, strip only the device segment.
  const normalizedUser = userPart.split(":")[0];
  if (!normalizedUser) return raw;
  return `${normalizedUser}@${serverPart}`;
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
function normalizeProfileRecord(raw, fallbackId) {
  const p = raw && typeof raw === "object" ? raw : {};
  const id = String(p.id || fallbackId || "").trim() || "p_" + Math.random().toString(16).slice(2);
  const name = String(p.name || "WhatsApp Profile").trim() || "WhatsApp Profile";
  return {
    id,
    name,
    customName: p.customName === true,
    waJid: String(p.waJid || "").trim(),
    waMsisdn: String(p.waMsisdn || "").trim(),
    waName: String(p.waName || "").trim(),
    lastConnectedAt: String(p.lastConnectedAt || "").trim()
  };
}

function saveProfiles(profiles) {
  const list = Array.isArray(profiles) ? profiles : [];
  const normalized = list.map((p, idx) => normalizeProfileRecord(p, `p_${idx + 1}`));
  store.set("profiles", normalized);
  return normalized;
}

function loadProfiles() {
  ensureDir(profilesRootDir);
  const profiles = store.get("profiles");
  if (Array.isArray(profiles) && profiles.length > 0) {
    return saveProfiles(profiles);
  }
  const defaultProfile = normalizeProfileRecord({ id: "p_default", name: "Default WhatsApp", customName: false });
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
  const trimmed = String(name || "").trim();
  const profile = normalizeProfileRecord({
    id,
    name: trimmed || "New Profile",
    customName: !!trimmed
  });
  profiles.push(profile);
  saveProfiles(profiles);
  ensureDir(getProfileAuthDir(id));
  return profile;
}

function renameProfile(profileId, newName) {
  const nextName = String(newName || "").trim();
  if (!nextName) throw new Error("Profile name cannot be empty");
  const profiles = loadProfiles();
  const idx = profiles.findIndex((p) => p.id === profileId);
  if (idx < 0) throw new Error("Profile not found");

  profiles[idx] = {
    ...profiles[idx],
    name: nextName,
    customName: true
  };
  saveProfiles(profiles);
  return profiles[idx];
}

async function deleteProfile(profileId) {
  const profiles = loadProfiles();
  const activeId = getActiveProfileId();
  const isActive = activeId === profileId;

  const idx = profiles.findIndex((p) => p.id === profileId);
  if (idx < 0) throw new Error("Profile not found");

  if (isActive) {
    handshakeAttemptId++;
    handshakeState = { method: "qr", phoneNumber: "", pairingRequested: false };
    await waitForConnectSetupIdle();
    await stopSocket();
    isConnecting = false;
  }

  const nextProfiles = profiles.filter((p) => p.id !== profileId);

  const profileDir = path.join(profilesRootDir, profileId);
  try {
    fs.rmSync(profileDir, { recursive: true, force: true });
  } catch (e) {
    // ignore
  }
  invalidSessionProfiles.delete(profileId);
  clearContactsCacheForProfile(profileId);
  clearWaChatStateForProfile(profileId);
  clearWaChatSyncTimer(profileId);

  // Never leave app without at least one profile
  if (nextProfiles.length === 0) {
    const fallback = normalizeProfileRecord({
      id: "p_default",
      name: "Default WhatsApp",
      customName: false
    });
    saveProfiles([fallback]);
    store.set("activeProfileId", fallback.id);
    ensureDir(getProfileAuthDir(fallback.id));

    win?.webContents.send("wa:status", {
      connected: false,
      text: "Not connected",
      profileId: fallback.id
    });
    return { ok: true, activeProfileId: fallback.id, replaced: true };
  }

  saveProfiles(nextProfiles);
  if (isActive) {
    const nextActiveId = nextProfiles[0].id;
    store.set("activeProfileId", nextActiveId);
    await autoReconnectActiveProfile();
    return { ok: true, activeProfileId: nextActiveId, replaced: false };
  }

  return { ok: true, activeProfileId: getActiveProfileId(), replaced: false };
}

async function terminateProfileSession(profileId) {
  const profiles = loadProfiles();
  const idx = profiles.findIndex((p) => p.id === profileId);
  if (idx < 0) throw new Error("Profile not found");

  const activeProfileId = getActiveProfileId();
  const isActive = activeProfileId === profileId;
  const authDir = getProfileAuthDir(profileId);

  if (isActive) {
    handshakeAttemptId++;
    handshakeState = { method: "qr", phoneNumber: "", pairingRequested: false };
    await waitForConnectSetupIdle();
    await stopSocket();
    isConnecting = false;
  }

  clearAuthDir(authDir);
  invalidSessionProfiles.delete(profileId);
  clearContactsCacheForProfile(profileId);
  clearWaChatStateForProfile(profileId);
  clearWaChatSyncTimer(profileId);

  profiles[idx] = normalizeProfileRecord(
    {
      ...profiles[idx],
      waJid: "",
      waMsisdn: "",
      waName: "",
      lastConnectedAt: ""
    },
    profiles[idx].id
  );
  saveProfiles(profiles);

  if (isActive) {
    win?.webContents.send("wa:status", {
      connected: false,
      text: "Session terminated. Handshake required.",
      profileId
    });
  }

  return { ok: true, profileId, activeProfileId };
}

function updateConnectedProfileMeta(profileId, waUser) {
  const profiles = loadProfiles();
  const idx = profiles.findIndex((p) => p.id === profileId);
  if (idx < 0) return null;

  const current = profiles[idx];
  const jid = normalizeJidForContact(waUser?.id || current.waJid || "");
  const msisdn = msisdnFromUserJid(jid);
  const waName = sanitizeContactName(waUser?.name || waUser?.notify || waUser?.verifiedName || current.waName || "");

  const next = {
    ...current,
    waJid: jid || current.waJid || "",
    waMsisdn: msisdn || current.waMsisdn || "",
    waName: waName || current.waName || "",
    lastConnectedAt: nowIsoShort()
  };

  if (!next.customName) {
    const autoName = next.waName || (next.waMsisdn ? `WhatsApp ${next.waMsisdn}` : "");
    if (autoName) next.name = autoName;
  }

  profiles[idx] = normalizeProfileRecord(next, next.id);
  saveProfiles(profiles);
  return profiles[idx];
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

/* --------------------------- WhatsApp Contacts (persisted cache + live sync) ---------------------------- */
function getContactsStoreObj() {
  return waContactsByProfileMem;
}

function sanitizeContactName(v) {
  return String(v || "").trim();
}

function getContactDisplayName(c) {
  return (
    sanitizeContactName(c?.name) ||
    sanitizeContactName(c?.notify) ||
    sanitizeContactName(c?.verifiedName) ||
    ""
  );
}

function firstNonEmptyString(values) {
  for (const v of Array.isArray(values) ? values : []) {
    const s = sanitizeContactName(v);
    if (s) return s;
  }
  return "";
}

function msisdnFromContactAddress(value) {
  const norm = normalizeJidForContact(value);
  const fromJid = msisdnFromUserJid(norm);
  if (fromJid) return fromJid;
  if (/^\d{6,}$/.test(norm)) return norm;
  return "";
}

function upsertContactsForProfile(profileId, contacts) {
  if (!profileId || !Array.isArray(contacts) || contacts.length === 0) return 0;

  const root = getContactsStoreObj();
  const byPhone = root[profileId] && typeof root[profileId] === "object" ? { ...root[profileId] } : {};
  let changed = 0;

  for (const c of contacts) {
    const jidSource = c?.jid || c?.id || c?.pnJid || "";
    const jid = normalizeJidForContact(jidSource);
    const msisdn = msisdnFromContactAddress(jidSource);
    if (!msisdn) continue;

    const existing = byPhone[msisdn] && typeof byPhone[msisdn] === "object" ? byPhone[msisdn] : {};
    const nameCandidate = firstNonEmptyString([
      c?.name,
      c?.fullName,
      c?.firstName,
      c?.shortName,
      c?.short,
      c?.username,
      existing?.name
    ]);
    const notify = firstNonEmptyString([c?.notify, c?.pushName, c?.pushname, existing?.notify]);
    const verifiedName = firstNonEmptyString([c?.verifiedName, c?.vname, existing?.verifiedName]);
    const lid = firstNonEmptyString([c?.lid, c?.lidJid, existing?.lid]);
    const imgUrl =
      c?.imgUrl === null || c?.imgUrl === undefined
        ? existing?.imgUrl ?? null
        : String(c.imgUrl || "").trim() || null;
    const status = sanitizeContactName(c?.status) || sanitizeContactName(existing?.status);

    const next = {
      msisdn,
      jid: jid || existing?.jid || `${msisdn}@s.whatsapp.net`,
      lid,
      name: nameCandidate,
      notify,
      verifiedName,
      imgUrl,
      status,
      photoCheckedAt: existing?.photoCheckedAt || null,
      updatedAt: nowIsoShort()
    };

    const prev = byPhone[msisdn];
    const same =
      prev &&
      prev.msisdn === next.msisdn &&
      prev.jid === next.jid &&
      String(prev.lid || "") === String(next.lid || "") &&
      String(prev.name || "") === String(next.name || "") &&
      String(prev.notify || "") === String(next.notify || "") &&
      String(prev.verifiedName || "") === String(next.verifiedName || "") &&
      String(prev.imgUrl || "") === String(next.imgUrl || "") &&
      String(prev.status || "") === String(next.status || "");

    if (!same) {
      byPhone[msisdn] = next;
      changed++;
    }
  }

  if (changed > 0) {
    root[profileId] = byPhone;
    persistContactsCache();
  }

  return changed;
}

function upsertChatsAsContactsForProfile(profileId, chats) {
  if (!profileId || !Array.isArray(chats) || chats.length === 0) return 0;

  const contacts = [];
  for (const chat of chats) {
    const jidSource = chat?.id || chat?.jid || "";
    const jid = normalizeJidForContact(jidSource);
    const msisdn = msisdnFromContactAddress(jidSource);
    if (!msisdn) continue;

    const name = firstNonEmptyString([chat?.name, chat?.notify, chat?.pushName]);
    const notify = firstNonEmptyString([chat?.notify, chat?.pushName, name]);
    contacts.push({
      id: jid || `${msisdn}@s.whatsapp.net`,
      jid: jid || `${msisdn}@s.whatsapp.net`,
      lid: firstNonEmptyString([chat?.lid, chat?.lidJid]),
      name,
      notify,
      verifiedName: sanitizeContactName(chat?.verifiedName)
    });
  }

  return upsertContactsForProfile(profileId, contacts);
}

function upsertMessagesAsContactsForProfile(profileId, messages) {
  if (!profileId || !Array.isArray(messages) || messages.length === 0) return 0;

  const contacts = [];
  for (const msg of messages) {
    const key = msg?.key && typeof msg.key === "object" ? msg.key : {};
    const jidSource = key.participant || key.remoteJid || "";
    const jid = normalizeJidForContact(jidSource);
    const msisdn = msisdnFromContactAddress(jidSource);
    if (!msisdn) continue;

    const notify = firstNonEmptyString([msg?.pushName, msg?.notifyName, msg?.notify, msg?.participantName]);
    const verifiedName = firstNonEmptyString([msg?.verifiedBizName, msg?.verifiedName]);
    if (!notify && !verifiedName) continue;

    contacts.push({
      id: jid || `${msisdn}@s.whatsapp.net`,
      jid: jid || `${msisdn}@s.whatsapp.net`,
      notify,
      verifiedName
    });
  }

  return upsertContactsForProfile(profileId, contacts);
}

function getContactsForProfile(profileId) {
  const root = getContactsStoreObj();
  const byPhone = root[profileId] && typeof root[profileId] === "object" ? root[profileId] : {};
  const rows = Object.values(byPhone).filter((x) => x && typeof x === "object");

  rows.sort((a, b) => {
    const an = getContactDisplayName(a).toLowerCase();
    const bn = getContactDisplayName(b).toLowerCase();
    if (an && bn) return an.localeCompare(bn);
    if (an) return -1;
    if (bn) return 1;
    return String(a?.msisdn || "").localeCompare(String(b?.msisdn || ""));
  });

  return rows;
}

function contactHasAnyName(contact) {
  return !!getContactDisplayName(contact);
}

async function syncContactNamesForProfile(profileId, options) {
  const opts = options && typeof options === "object" ? options : {};
  const force = !!opts.force;
  const isInitialSync = opts.isInitialSync === true;
  const cooldownMs = Number.isFinite(Number(opts.cooldownMs))
    ? Math.max(15 * 1000, Number(opts.cooldownMs))
    : 5 * 60 * 1000;

  if (!profileId || !isConnected || !sock || typeof sock.resyncAppState !== "function") {
    return { ok: false, skipped: true, reason: "not_available" };
  }

  const now = Date.now();
  const lastSyncAt = Number(lastContactNameSyncAtByProfile.get(profileId) || 0);
  if (!force && now - lastSyncAt < cooldownMs) {
    return { ok: true, skipped: true, reason: "cooldown" };
  }

  const inFlight = contactNameSyncByProfile.get(profileId);
  if (inFlight) return await inFlight;

  const syncTask = (async () => {
    try {
      await sock.resyncAppState(
        ["critical_unblock_low", "critical_block", "regular_high", "regular_low", "regular"],
        isInitialSync
      );
      lastContactNameSyncAtByProfile.set(profileId, Date.now());
      return { ok: true, synced: true, profileId };
    } catch (e) {
      log.warn({ err: e, profileId }, "Contact name sync failed");
      return { ok: false, profileId, error: String(e?.message || e) };
    } finally {
      contactNameSyncByProfile.delete(profileId);
    }
  })();

  contactNameSyncByProfile.set(profileId, syncTask);
  return await syncTask;
}

function shouldRefreshContactPhoto(contact, minMinutesBetweenChecks) {
  if (!contact || typeof contact !== "object") return false;
  if (contact.imgUrl) return false;
  if (!contact.jid || !String(contact.jid).endsWith("@s.whatsapp.net")) return false;
  const mins = Math.max(1, Number(minMinutesBetweenChecks || 180));
  const last = Date.parse(String(contact.photoCheckedAt || ""));
  if (!Number.isFinite(last)) return true;
  return Date.now() - last >= mins * 60 * 1000;
}

async function enrichContactPhotosForProfile(profileId, options) {
  const opts = options && typeof options === "object" ? options : {};
  if (!isConnected || !sock) return getContactsForProfile(profileId);
  if (!profileId) return [];

  const maxPhotoFetchRaw = Number(opts.maxPhotoFetch);
  const maxPhotoFetch = Number.isFinite(maxPhotoFetchRaw) ? Math.max(1, Math.min(200, maxPhotoFetchRaw)) : 40;
  const minMinutesRaw = Number(opts.minMinutesBetweenPhotoChecks);
  const minMinutesBetweenPhotoChecks = Number.isFinite(minMinutesRaw)
    ? Math.max(1, Math.min(1440, minMinutesRaw))
    : 180;
  const concurrencyRaw = Number(opts.photoFetchConcurrency);
  const photoFetchConcurrency = Number.isFinite(concurrencyRaw) ? Math.max(1, Math.min(8, concurrencyRaw)) : 3;

  const root = getContactsStoreObj();
  const byPhone = root[profileId] && typeof root[profileId] === "object" ? { ...root[profileId] } : {};
  const contacts = Object.values(byPhone).filter((x) => x && typeof x === "object");
  const targets = contacts
    .filter((c) => shouldRefreshContactPhoto(c, minMinutesBetweenPhotoChecks))
    .slice(0, maxPhotoFetch);

  if (targets.length === 0) return getContactsForProfile(profileId);

  let index = 0;
  let changed = 0;
  async function worker() {
    while (index < targets.length) {
      const currIndex = index++;
      const item = targets[currIndex];
      const now = nowIsoShort();

      try {
        const photo = await sock.profilePictureUrl(item.jid, "image");
        const prev = byPhone[item.msisdn] && typeof byPhone[item.msisdn] === "object" ? byPhone[item.msisdn] : item;
        const next = {
          ...prev,
          imgUrl: photo ? String(photo) : null,
          photoCheckedAt: now,
          updatedAt: now
        };

        const same =
          String(prev.imgUrl || "") === String(next.imgUrl || "") &&
          String(prev.photoCheckedAt || "") === String(next.photoCheckedAt || "");
        if (!same) {
          byPhone[item.msisdn] = next;
          changed++;
        }
      } catch (e) {
        const prev = byPhone[item.msisdn] && typeof byPhone[item.msisdn] === "object" ? byPhone[item.msisdn] : item;
        byPhone[item.msisdn] = { ...prev, photoCheckedAt: now };
        changed++;
      }
    }
  }

  const workers = [];
  for (let i = 0; i < photoFetchConcurrency; i++) workers.push(worker());
  await Promise.all(workers);

  if (changed > 0) {
    root[profileId] = byPhone;
    persistContactsCache();
  }

  return getContactsForProfile(profileId);
}

/* --------------------------- WhatsApp Chat Store (recent chats + messages) ---------------------------- */
function getWaChatStoreObj() {
  return waChatByProfileMem;
}

function clearWaChatStateForProfile(profileId) {
  if (!profileId) return;
  const root = getWaChatStoreObj();
  if (!Object.prototype.hasOwnProperty.call(root, profileId)) return;
  delete root[profileId];
  schedulePersistWaChatCache();
}

function ensureWaChatStateForProfile(profileId) {
  const root = getWaChatStoreObj();
  if (!root[profileId] || typeof root[profileId] !== "object") {
    root[profileId] = {
      chatsByJid: {},
      messagesByChat: {}
    };
  }
  if (!root[profileId].chatsByJid || typeof root[profileId].chatsByJid !== "object") {
    root[profileId].chatsByJid = {};
  }
  if (!root[profileId].messagesByChat || typeof root[profileId].messagesByChat !== "object") {
    root[profileId].messagesByChat = {};
  }
  return root[profileId];
}

function scheduleWaChatSync(profileId, reason) {
  if (!profileId) return;
  const key = String(profileId);
  const oldTimer = waChatSyncTimers.get(key);
  if (oldTimer) clearTimeout(oldTimer);
  const timer = setTimeout(() => {
    waChatSyncTimers.delete(key);
    win?.webContents.send("wa:chatSync", {
      profileId: key,
      reason: cleanString(reason || "update") || "update",
      ts: Date.now()
    });
  }, 120);
  waChatSyncTimers.set(key, timer);
}

function clearWaChatSyncTimer(profileId) {
  const key = String(profileId || "");
  if (!key) return;
  const timer = waChatSyncTimers.get(key);
  if (!timer) return;
  clearTimeout(timer);
  waChatSyncTimers.delete(key);
}

function normalizeEpochMs(raw, fallback = 0) {
  if (raw === null || raw === undefined) return fallback;

  if (typeof raw === "number") {
    if (!Number.isFinite(raw)) return fallback;
    if (raw > 1e12) return Math.round(raw);
    return Math.round(raw * 1000);
  }

  if (typeof raw === "bigint") {
    const n = Number(raw);
    return Number.isFinite(n) ? normalizeEpochMs(n, fallback) : fallback;
  }

  if (typeof raw === "string") {
    const s = raw.trim();
    if (!s) return fallback;
    const n = Number(s);
    return Number.isFinite(n) ? normalizeEpochMs(n, fallback) : fallback;
  }

  if (raw && typeof raw === "object") {
    if (typeof raw.toNumber === "function") {
      try {
        return normalizeEpochMs(raw.toNumber(), fallback);
      } catch (e) {
        // ignore
      }
    }

    if (typeof raw.low === "number") {
      return normalizeEpochMs(raw.low, fallback);
    }
    if (typeof raw.seconds === "number") {
      return normalizeEpochMs(raw.seconds, fallback);
    }
    if (typeof raw.value === "number") {
      return normalizeEpochMs(raw.value, fallback);
    }
  }

  return fallback;
}

function isIgnoredChatJid(jid) {
  const chatJid = normalizeJidForContact(jid);
  if (!chatJid) return true;
  if (!chatJid.includes("@")) return true;
  if (chatJid === "status@broadcast") return true;
  if (chatJid.endsWith("@broadcast")) return true;
  if (chatJid.endsWith("@lid")) return true;
  return false;
}

function normalizeChatJid(input) {
  const jid = normalizeJidForContact(input);
  if (!jid) return "";
  if (!jid.includes("@") && /^\d{6,}$/.test(jid)) {
    return `${jid}@s.whatsapp.net`;
  }
  return isIgnoredChatJid(jid) ? "" : jid;
}

function normalizeSendTargetJid(input) {
  const raw = cleanString(input);
  if (!raw) return "";
  if (raw.includes("@")) return normalizeChatJid(raw);
  try {
    const msisdn = normalizeMsisdn(raw);
    return `${msisdn}@s.whatsapp.net`;
  } catch (e) {
    return "";
  }
}

function bytesToBase64(value) {
  if (!value) return "";
  try {
    if (Buffer.isBuffer(value)) return value.toString("base64");
    if (value instanceof Uint8Array) return Buffer.from(value).toString("base64");
    if (Array.isArray(value)) return Buffer.from(value).toString("base64");
  } catch (e) {
    return "";
  }
  return "";
}

function unwrapMessageContent(messageContent) {
  let content = messageContent && typeof messageContent === "object" ? messageContent : {};
  for (let i = 0; i < 8; i++) {
    if (!content || typeof content !== "object") break;
    if (content.ephemeralMessage?.message) {
      content = content.ephemeralMessage.message;
      continue;
    }
    if (content.viewOnceMessage?.message) {
      content = content.viewOnceMessage.message;
      continue;
    }
    if (content.viewOnceMessageV2?.message) {
      content = content.viewOnceMessageV2.message;
      continue;
    }
    if (content.viewOnceMessageV2Extension?.message) {
      content = content.viewOnceMessageV2Extension.message;
      continue;
    }
    if (content.documentWithCaptionMessage?.message) {
      content = content.documentWithCaptionMessage.message;
      continue;
    }
    if (content.editedMessage?.message) {
      content = content.editedMessage.message;
      continue;
    }
    break;
  }
  return content && typeof content === "object" ? content : {};
}

function summarizeMessagePayload(rawMessage) {
  const messageNode = rawMessage && typeof rawMessage === "object" ? rawMessage : {};
  const content = unwrapMessageContent(messageNode.message || {});
  const textOrEmpty = (v) => cleanString(v || "");

  if (!content || Object.keys(content).length === 0) {
    return {
      type: "unknown",
      text: "",
      preview: "",
      hasMedia: false,
      media: null,
      skip: true
    };
  }

  if (textOrEmpty(content.conversation)) {
    const text = textOrEmpty(content.conversation);
    return { type: "text", text, preview: text, hasMedia: false, media: null, skip: false };
  }

  if (textOrEmpty(content.extendedTextMessage?.text)) {
    const text = textOrEmpty(content.extendedTextMessage.text);
    return { type: "text", text, preview: text, hasMedia: false, media: null, skip: false };
  }

  if (content.imageMessage && typeof content.imageMessage === "object") {
    const caption = textOrEmpty(content.imageMessage.caption);
    const thumb = bytesToBase64(content.imageMessage.jpegThumbnail);
    const localThumb = textOrEmpty(messageNode.__localThumbnailDataUrl);
    const localPath = textOrEmpty(messageNode.__localImagePath);
    const thumbDataUrl = thumb
      ? `data:image/jpeg;base64,${thumb}`
      : localThumb.startsWith("data:image/") ? localThumb : "";
    return {
      type: "image",
      text: caption,
      preview: caption || "[Image]",
      hasMedia: true,
      media: {
        kind: "image",
        mimeType: textOrEmpty(content.imageMessage.mimetype) || "image/jpeg",
        fileName: textOrEmpty(content.imageMessage.fileName),
        fileLength: Number(content.imageMessage.fileLength || 0) || 0,
        thumbnailDataUrl: thumbDataUrl,
        localPath
      },
      skip: false
    };
  }

  if (content.videoMessage && typeof content.videoMessage === "object") {
    const caption = textOrEmpty(content.videoMessage.caption);
    const thumb = bytesToBase64(content.videoMessage.jpegThumbnail);
    return {
      type: "video",
      text: caption,
      preview: caption || "[Video]",
      hasMedia: true,
      media: {
        kind: "video",
        mimeType: textOrEmpty(content.videoMessage.mimetype) || "video/mp4",
        fileName: textOrEmpty(content.videoMessage.fileName),
        fileLength: Number(content.videoMessage.fileLength || 0) || 0,
        thumbnailDataUrl: thumb ? `data:image/jpeg;base64,${thumb}` : ""
      },
      skip: false
    };
  }

  if (content.documentMessage && typeof content.documentMessage === "object") {
    const fileName = textOrEmpty(content.documentMessage.fileName);
    const caption = textOrEmpty(content.documentMessage.caption);
    return {
      type: "document",
      text: caption,
      preview: caption || fileName || "[Document]",
      hasMedia: true,
      media: {
        kind: "document",
        mimeType: textOrEmpty(content.documentMessage.mimetype) || "application/octet-stream",
        fileName,
        fileLength: Number(content.documentMessage.fileLength || 0) || 0,
        thumbnailDataUrl: ""
      },
      skip: false
    };
  }

  if (content.audioMessage && typeof content.audioMessage === "object") {
    return {
      type: "audio",
      text: "",
      preview: "[Audio]",
      hasMedia: true,
      media: {
        kind: "audio",
        mimeType: textOrEmpty(content.audioMessage.mimetype) || "audio/ogg",
        fileName: "",
        fileLength: Number(content.audioMessage.fileLength || 0) || 0,
        thumbnailDataUrl: ""
      },
      skip: false
    };
  }

  if (content.stickerMessage && typeof content.stickerMessage === "object") {
    return {
      type: "sticker",
      text: "",
      preview: "[Sticker]",
      hasMedia: true,
      media: {
        kind: "sticker",
        mimeType: textOrEmpty(content.stickerMessage.mimetype) || "image/webp",
        fileName: "",
        fileLength: Number(content.stickerMessage.fileLength || 0) || 0,
        thumbnailDataUrl: ""
      },
      skip: false
    };
  }

  if (content.locationMessage && typeof content.locationMessage === "object") {
    return {
      type: "location",
      text: "",
      preview: "[Location]",
      hasMedia: false,
      media: null,
      skip: false
    };
  }

  if (content.reactionMessage && typeof content.reactionMessage === "object") {
    const reactionText = textOrEmpty(content.reactionMessage.text);
    return {
      type: "reaction",
      text: reactionText,
      preview: reactionText ? `Reacted ${reactionText}` : "[Reaction]",
      hasMedia: false,
      media: null,
      skip: true
    };
  }

  if (content.protocolMessage && typeof content.protocolMessage === "object") {
    return {
      type: "protocol",
      text: "",
      preview: "",
      hasMedia: false,
      media: null,
      skip: true
    };
  }

  if (content.contactsArrayMessage && typeof content.contactsArrayMessage === "object") {
    return {
      type: "contacts",
      text: "",
      preview: "[Contact card]",
      hasMedia: false,
      media: null,
      skip: false
    };
  }

  if (content.contactMessage && typeof content.contactMessage === "object") {
    const displayName = textOrEmpty(content.contactMessage.displayName);
    return {
      type: "contact",
      text: "",
      preview: displayName ? `[Contact] ${displayName}` : "[Contact]",
      hasMedia: false,
      media: null,
      skip: false
    };
  }

  if (content.pollCreationMessage || content.pollCreationMessageV2 || content.pollCreationMessageV3) {
    return {
      type: "poll",
      text: "",
      preview: "[Poll]",
      hasMedia: false,
      media: null,
      skip: false
    };
  }

  const fallback = Object.keys(content)[0] || "message";
  return {
    type: fallback,
    text: "",
    preview: `[${fallback}]`,
    hasMedia: false,
    media: null,
    skip: false
  };
}

function normalizeMessageKey(key, fallbackRemoteJid) {
  const src = key && typeof key === "object" ? key : {};
  const remoteJid = normalizeChatJid(src.remoteJid || fallbackRemoteJid || "");
  const id = cleanString(src.id || "");
  const participant = normalizeJidForContact(src.participant || "");
  const fromMe = src.fromMe === true;
  return { remoteJid, id, participant, fromMe };
}

function messageKeyHash(key) {
  const k = key && typeof key === "object" ? key : {};
  return `${cleanString(k.remoteJid)}|${cleanString(k.id)}|${k.fromMe ? "1" : "0"}|${cleanString(k.participant)}`;
}

function getContactByChatJid(profileId, chatJid) {
  const msisdn = msisdnFromContactAddress(chatJid);
  if (!msisdn) return null;
  const byPhone =
    waContactsByProfileMem &&
    waContactsByProfileMem[profileId] &&
    typeof waContactsByProfileMem[profileId] === "object"
      ? waContactsByProfileMem[profileId]
      : {};
  return byPhone[msisdn] && typeof byPhone[msisdn] === "object" ? byPhone[msisdn] : null;
}

function ensureContactsFromChatsForProfile(profileId) {
  if (!profileId) return 0;
  const state = ensureWaChatStateForProfile(profileId);
  const chats = Object.values(state.chatsByJid || {});
  if (chats.length === 0) return 0;

  const contacts = [];
  for (const chat of chats) {
    const chatJid = normalizeChatJid(chat?.jid || "");
    if (!chatJid || !chatJid.endsWith("@s.whatsapp.net")) continue;
    contacts.push({
      id: chatJid,
      jid: chatJid,
      name: cleanString(chat?.name || ""),
      notify: cleanString(chat?.name || "")
    });
  }

  if (contacts.length === 0) return 0;
  return upsertContactsForProfile(profileId, contacts);
}

function fallbackTitleForChatJid(chatJid) {
  const jid = normalizeJidForContact(chatJid);
  if (!jid) return "Unknown chat";
  const msisdn = msisdnFromContactAddress(jid);
  if (msisdn) return msisdn;
  if (jid.endsWith("@g.us")) return "Group";
  return jid;
}

function resolveChatTitle(profileId, chat) {
  const c = chat && typeof chat === "object" ? chat : {};
  const direct = firstNonEmptyString([c.name, c.subject, c.notify, c.pushName]);
  if (direct) return direct;
  const byContact = getContactByChatJid(profileId, c.jid);
  const contactName = getContactDisplayName(byContact);
  if (contactName) return contactName;
  return fallbackTitleForChatJid(c.jid);
}

function resolveMessageSenderName(profileId, chatJid, messageRecord) {
  if (!messageRecord || typeof messageRecord !== "object") return "";
  if (messageRecord.fromMe) return "You";

  const participant = normalizeJidForContact(messageRecord?.key?.participant || "");
  const fromJid = participant || normalizeJidForContact(chatJid);
  const fromContactMsisdn = msisdnFromContactAddress(fromJid);
  if (fromContactMsisdn) {
    const byPhone =
      waContactsByProfileMem &&
      waContactsByProfileMem[profileId] &&
      typeof waContactsByProfileMem[profileId] === "object"
        ? waContactsByProfileMem[profileId]
        : {};
    const contact = byPhone[fromContactMsisdn];
    const name = getContactDisplayName(contact);
    if (name) return name;
    return fromContactMsisdn;
  }

  return messageRecord.pushName || "";
}

function ensureChatSummary(profileId, chatJid) {
  const state = ensureWaChatStateForProfile(profileId);
  const jid = normalizeChatJid(chatJid);
  if (!jid) return null;
  if (!state.chatsByJid[jid] || typeof state.chatsByJid[jid] !== "object") {
    state.chatsByJid[jid] = {
      jid,
      name: "",
      lastMessageTimestampMs: 0,
      lastMessagePreview: "",
      lastMessageType: "",
      lastMessageFromMe: false,
      unreadCount: 0,
      archived: false,
      pinned: false,
      updatedAt: nowIsoShort()
    };
  }
  return state.chatsByJid[jid];
}

function upsertChatsForProfile(profileId, chats) {
  if (!profileId || !Array.isArray(chats) || chats.length === 0) return 0;
  const state = ensureWaChatStateForProfile(profileId);
  let changed = 0;

  for (const raw of chats) {
    const src = raw && typeof raw === "object" ? raw : {};
    const chatJid = normalizeChatJid(src.id || src.jid || "");
    if (!chatJid) continue;

    const chat = ensureChatSummary(profileId, chatJid);
    if (!chat) continue;

    const nextName = firstNonEmptyString([src.name, src.subject, src.notify, src.pushName, chat.name]);
    const nextTs = normalizeEpochMs(
      src.conversationTimestamp || src.lastMessageRecvTimestamp || src.lastMessageTimestamp || src.timestamp,
      chat.lastMessageTimestampMs || 0
    );

    const rawUnread = Number(src.unreadCount);
    const nextUnread = Number.isFinite(rawUnread) ? Math.max(0, Math.round(rawUnread)) : chat.unreadCount;
    const nextArchived = typeof src.archive === "boolean" ? src.archive : chat.archived;
    const nextPinned =
      typeof src.pin !== "undefined"
        ? Number(src.pin || 0) > 0
        : typeof src.pinned === "boolean"
          ? src.pinned
          : chat.pinned;

    const same =
      chat.name === nextName &&
      Number(chat.lastMessageTimestampMs || 0) === Number(nextTs || 0) &&
      Number(chat.unreadCount || 0) === Number(nextUnread || 0) &&
      !!chat.archived === !!nextArchived &&
      !!chat.pinned === !!nextPinned;

    if (!same) {
      state.chatsByJid[chatJid] = {
        ...chat,
        jid: chatJid,
        name: nextName,
        lastMessageTimestampMs: Number(nextTs || 0),
        unreadCount: nextUnread,
        archived: !!nextArchived,
        pinned: !!nextPinned,
        updatedAt: nowIsoShort()
      };
      changed++;
    }
  }

  if (changed > 0) {
    scheduleWaChatSync(profileId, "chats");
    schedulePersistWaChatCache();
  }
  return changed;
}

function normalizeMessageRecord(rawMessage) {
  const msg = rawMessage && typeof rawMessage === "object" ? rawMessage : {};
  const key = normalizeMessageKey(msg.key, msg?.key?.remoteJid || msg?.remoteJid || "");
  const chatJid = key.remoteJid || normalizeChatJid(msg?.chatId || msg?.jid || "");
  if (!chatJid) return null;
  if (!key.id) return null;

  const summary = summarizeMessagePayload(msg);
  if (summary.skip) return null;

  const ts = normalizeEpochMs(msg.messageTimestamp, Date.now());
  return {
    key: {
      remoteJid: chatJid,
      id: key.id,
      fromMe: key.fromMe,
      participant: key.participant || ""
    },
    hash: messageKeyHash({
      remoteJid: chatJid,
      id: key.id,
      fromMe: key.fromMe,
      participant: key.participant || ""
    }),
    chatJid,
    fromMe: key.fromMe,
    pushName: cleanString(msg.pushName || msg.notifyName || msg.participantName || ""),
    timestampMs: Number(ts || Date.now()),
    type: summary.type,
    text: summary.text,
    preview: summary.preview,
    hasMedia: summary.hasMedia,
    media: summary.media,
    status: Number(msg.status || 0) || 0,
    rawMessage: msg
  };
}

function upsertMessagesForProfile(profileId, messages) {
  if (!profileId || !Array.isArray(messages) || messages.length === 0) return 0;
  const state = ensureWaChatStateForProfile(profileId);
  let changed = 0;

  for (const item of messages) {
    const normalized = normalizeMessageRecord(item);
    if (!normalized) continue;

    const chat = ensureChatSummary(profileId, normalized.chatJid);
    if (!chat) continue;

    if (!state.messagesByChat[normalized.chatJid] || !Array.isArray(state.messagesByChat[normalized.chatJid])) {
      state.messagesByChat[normalized.chatJid] = [];
    }
    const list = state.messagesByChat[normalized.chatJid];
    const existingIndex = list.findIndex((x) => x && x.hash === normalized.hash);

    if (existingIndex >= 0) {
      const prev = list[existingIndex];
      const prevMedia = prev?.media && typeof prev.media === "object" ? prev.media : null;
      const nextMedia = normalized.media
        ? {
            ...(prevMedia || {}),
            ...(normalized.media || {}),
            localPath: cleanString(normalized?.media?.localPath || prevMedia?.localPath || "")
          }
        : prevMedia || null;
      list[existingIndex] = {
        ...prev,
        ...normalized,
        media: nextMedia,
        rawMessage: normalized.rawMessage || prev.rawMessage
      };
      changed++;
    } else {
      list.push(normalized);
      changed++;
    }

    if (list.length > WA_CHAT_MAX_MESSAGES_PER_CHAT) {
      list.sort((a, b) => Number(a.timestampMs || 0) - Number(b.timestampMs || 0));
      state.messagesByChat[normalized.chatJid] = list.slice(-WA_CHAT_MAX_MESSAGES_PER_CHAT);
    }

    const isLatest = Number(normalized.timestampMs || 0) >= Number(chat.lastMessageTimestampMs || 0);
    const nextName = firstNonEmptyString([chat.name, normalized.pushName]);
    if (isLatest || !chat.lastMessagePreview || !chat.lastMessageTimestampMs) {
      state.chatsByJid[normalized.chatJid] = {
        ...chat,
        name: nextName,
        lastMessageTimestampMs: Number(normalized.timestampMs || chat.lastMessageTimestampMs || Date.now()),
        lastMessagePreview: cleanString(normalized.preview || ""),
        lastMessageType: cleanString(normalized.type || ""),
        lastMessageFromMe: normalized.fromMe === true,
        updatedAt: nowIsoShort()
      };
    } else if (chat.name !== nextName) {
      state.chatsByJid[normalized.chatJid] = {
        ...chat,
        name: nextName,
        updatedAt: nowIsoShort()
      };
    }

    if (normalized.hasMedia && String(normalized?.media?.kind || "").toLowerCase() === "image") {
      const localPath = cleanString(normalized?.media?.localPath || "");
      if (localPath && fs.existsSync(localPath)) {
        setLocalImagePathForStoredMessage(profileId, normalized.chatJid, normalized.hash, localPath);
      } else {
        queueAutoSaveImageForHash(profileId, normalized.chatJid, normalized.hash);
      }
    }
  }

  if (changed > 0) {
    scheduleWaChatSync(profileId, "messages");
    schedulePersistWaChatCache();
  }
  return changed;
}

function applyMessageUpdatesForProfile(profileId, updates) {
  if (!profileId || !Array.isArray(updates) || updates.length === 0) return 0;
  const state = ensureWaChatStateForProfile(profileId);
  let changed = 0;

  for (const patch of updates) {
    const src = patch && typeof patch === "object" ? patch : {};
    const key = normalizeMessageKey(src.key, src?.key?.remoteJid || "");
    if (!key.remoteJid || !key.id) continue;
    const chatJid = key.remoteJid;
    const list = state.messagesByChat[chatJid];
    if (!Array.isArray(list) || list.length === 0) continue;

    const hash = messageKeyHash({ ...key, remoteJid: chatJid });
    const idx = list.findIndex((x) => x && x.hash === hash);
    if (idx < 0) continue;

    const prev = list[idx];
    const updateObj = src.update && typeof src.update === "object" ? src.update : {};
    const mergedRaw = {
      ...(prev.rawMessage && typeof prev.rawMessage === "object" ? prev.rawMessage : {}),
      ...(updateObj && typeof updateObj === "object" ? updateObj : {})
    };
    if (updateObj.message && typeof updateObj.message === "object") {
      mergedRaw.message = updateObj.message;
    }

    const normalized = normalizeMessageRecord({
      ...prev.rawMessage,
      key: prev.key,
      ...updateObj,
      message: mergedRaw.message || prev.rawMessage?.message
    });
    if (!normalized) continue;

    const prevMedia = prev?.media && typeof prev.media === "object" ? prev.media : null;
    const nextMedia = normalized.media
      ? {
          ...(prevMedia || {}),
          ...(normalized.media || {}),
          localPath: cleanString(normalized?.media?.localPath || prevMedia?.localPath || "")
        }
      : prevMedia || null;

    list[idx] = {
      ...prev,
      ...normalized,
      media: nextMedia,
      rawMessage: mergedRaw,
      status: Number(updateObj.status || normalized.status || prev.status || 0) || 0
    };
    changed++;

    const chat = ensureChatSummary(profileId, chatJid);
    if (chat && Number(list[idx].timestampMs || 0) >= Number(chat.lastMessageTimestampMs || 0)) {
      state.chatsByJid[chatJid] = {
        ...chat,
        lastMessageTimestampMs: Number(list[idx].timestampMs || 0),
        lastMessagePreview: cleanString(list[idx].preview || ""),
        lastMessageType: cleanString(list[idx].type || ""),
        lastMessageFromMe: list[idx].fromMe === true,
        updatedAt: nowIsoShort()
      };
    }
  }

  if (changed > 0) {
    scheduleWaChatSync(profileId, "message_updates");
    schedulePersistWaChatCache();
  }
  return changed;
}

function serializeChatSummary(profileId, chat) {
  const c = chat && typeof chat === "object" ? chat : {};
  const jid = normalizeChatJid(c.jid || "");
  if (!jid) return null;
  const contact = getContactByChatJid(profileId, jid);
  const title = resolveChatTitle(profileId, c);
  const preview = cleanString(c.lastMessagePreview || "");
  const isGroup = jid.endsWith("@g.us");
  return {
    jid,
    title,
    preview: preview || "",
    lastMessageType: cleanString(c.lastMessageType || ""),
    lastMessageFromMe: c.lastMessageFromMe === true,
    lastMessageTimestampMs: Number(c.lastMessageTimestampMs || 0) || 0,
    unreadCount: Math.max(0, Number(c.unreadCount || 0) || 0),
    archived: c.archived === true,
    pinned: c.pinned === true,
    avatarUrl: cleanString(contact?.imgUrl || ""),
    isGroup
  };
}

function getRecentChatsForProfile(profileId, options) {
  const opts = options && typeof options === "object" ? options : {};
  const state = ensureWaChatStateForProfile(profileId);
  const search = cleanString(opts.search || "").toLowerCase();
  const limitRaw = Number(opts.limit);
  const limit = Number.isFinite(limitRaw) ? Math.max(1, Math.min(250, Math.round(limitRaw))) : 120;

  const rows = Object.values(state.chatsByJid)
    .map((c) => serializeChatSummary(profileId, c))
    .filter((c) => !!c);

  rows.sort((a, b) => {
    const ap = a.pinned ? 1 : 0;
    const bp = b.pinned ? 1 : 0;
    if (ap !== bp) return bp - ap;
    const at = Number(a.lastMessageTimestampMs || 0);
    const bt = Number(b.lastMessageTimestampMs || 0);
    if (at !== bt) return bt - at;
    return String(a.title || "").localeCompare(String(b.title || ""));
  });

  const filtered = search
    ? rows.filter((row) => {
        return (
          String(row.title || "").toLowerCase().includes(search) ||
          String(row.preview || "").toLowerCase().includes(search) ||
          String(row.jid || "").toLowerCase().includes(search)
        );
      })
    : rows;

  return filtered.slice(0, limit);
}

function serializeMessageForRenderer(profileId, chatJid, messageRecord) {
  const msg = messageRecord && typeof messageRecord === "object" ? messageRecord : {};
  const senderName = resolveMessageSenderName(profileId, chatJid, msg);
  return {
    key: {
      remoteJid: cleanString(msg?.key?.remoteJid || chatJid),
      id: cleanString(msg?.key?.id || ""),
      fromMe: msg?.key?.fromMe === true,
      participant: cleanString(msg?.key?.participant || "")
    },
    chatJid: cleanString(chatJid),
    timestampMs: Number(msg.timestampMs || 0) || 0,
    fromMe: msg.fromMe === true,
    senderName: cleanString(senderName),
    type: cleanString(msg.type || "unknown"),
    text: String(msg.text || ""),
    preview: String(msg.preview || ""),
    hasMedia: msg.hasMedia === true,
    media: msg.media
      ? {
          kind: cleanString(msg.media.kind || ""),
          mimeType: cleanString(msg.media.mimeType || ""),
          fileName: cleanString(msg.media.fileName || ""),
          fileLength: Number(msg.media.fileLength || 0) || 0,
          thumbnailDataUrl: cleanString(msg.media.thumbnailDataUrl || ""),
          localPath: cleanString(msg.media.localPath || "")
        }
      : null,
    status: Number(msg.status || 0) || 0
  };
}

function getChatMessagesForProfile(profileId, chatJid, options) {
  const jid = normalizeChatJid(chatJid);
  if (!jid) return [];
  const opts = options && typeof options === "object" ? options : {};
  const limitRaw = Number(opts.limit);
  const limit = Number.isFinite(limitRaw) ? Math.max(1, Math.min(250, Math.round(limitRaw))) : 120;

  const state = ensureWaChatStateForProfile(profileId);
  const list = Array.isArray(state.messagesByChat[jid]) ? [...state.messagesByChat[jid]] : [];
  list.sort((a, b) => Number(a.timestampMs || 0) - Number(b.timestampMs || 0));
  const clipped = list.slice(-limit);
  return clipped.map((msg) => serializeMessageForRenderer(profileId, jid, msg));
}

function lookbackCutoffMs(days) {
  const d = Number.isFinite(Number(days)) ? Math.max(1, Math.round(Number(days))) : WA_HISTORY_LOOKBACK_DAYS;
  return Date.now() - d * 24 * 60 * 60 * 1000;
}

function pruneChatStoreToLookback(profileId, days) {
  const state = ensureWaChatStateForProfile(profileId);
  const cutoff = lookbackCutoffMs(days);
  let changed = 0;

  for (const [chatJid, listRaw] of Object.entries(state.messagesByChat || {})) {
    const list = Array.isArray(listRaw) ? listRaw : [];
    const next = list.filter((m) => Number(m?.timestampMs || 0) >= cutoff);
    if (next.length === 0) {
      if (list.length > 0) changed++;
      delete state.messagesByChat[chatJid];
      continue;
    }
    if (next.length !== list.length) changed++;
    state.messagesByChat[chatJid] = next;
  }

  for (const [chatJid, chatRaw] of Object.entries(state.chatsByJid || {})) {
    const chat = chatRaw && typeof chatRaw === "object" ? chatRaw : {};
    const ts = Number(chat.lastMessageTimestampMs || 0);
    const hasUnread = Number(chat.unreadCount || 0) > 0;
    const hasRecentMessages = Array.isArray(state.messagesByChat[chatJid]) && state.messagesByChat[chatJid].length > 0;
    if (hasUnread || hasRecentMessages) continue;
    if (ts >= cutoff) continue;
    delete state.chatsByJid[chatJid];
    changed++;
  }

  if (changed > 0) {
    schedulePersistWaChatCache();
    scheduleWaChatSync(profileId, "prune");
  }
  return changed;
}

function pruneSingleChatToLookback(profileId, chatJid, days) {
  const jid = normalizeChatJid(chatJid);
  if (!jid) return 0;
  const state = ensureWaChatStateForProfile(profileId);
  const cutoff = lookbackCutoffMs(days);
  const listRaw = Array.isArray(state.messagesByChat[jid]) ? state.messagesByChat[jid] : [];
  const next = listRaw.filter((m) => Number(m?.timestampMs || 0) >= cutoff);

  if (next.length === listRaw.length) return 0;

  if (next.length === 0) delete state.messagesByChat[jid];
  else state.messagesByChat[jid] = next;

  schedulePersistWaChatCache();
  scheduleWaChatSync(profileId, "prune_chat");
  return 1;
}

async function warmHistoryForChat(profileId, chatJid, options) {
  const opts = options && typeof options === "object" ? options : {};
  const jid = normalizeChatJid(chatJid);
  if (!jid) throw new Error("Invalid chat JID");
  if (!profileId || !isConnected || !sock) return { ok: false, skipped: true, reason: "not_connected", chatJid: jid };
  if (typeof sock.fetchMessageHistory !== "function") {
    return { ok: false, skipped: true, reason: "history_fetch_unavailable", chatJid: jid };
  }

  const force = opts.force === true;
  const taskKey = `${profileId}|${jid}`;
  const existingTask = waHistoryWarmupInFlightByChat.get(taskKey);
  if (existingTask && !force) return await existingTask;

  const task = (async () => {
    const days = Number.isFinite(Number(opts.days))
      ? Math.max(1, Math.min(365, Math.round(Number(opts.days))))
      : WA_HISTORY_LOOKBACK_DAYS;
    const cutoff = lookbackCutoffMs(days);
    const maxFetch = Number.isFinite(Number(opts.maxFetch))
      ? Math.max(1, Math.min(30, Math.round(Number(opts.maxFetch))))
      : 10;
    const pageSize = Number.isFinite(Number(opts.pageSize))
      ? Math.max(10, Math.min(100, Math.round(Number(opts.pageSize))))
      : 50;

    let fetchRequests = 0;
    let previousOldestHash = "";

    while (fetchRequests < maxFetch) {
      const state = ensureWaChatStateForProfile(profileId);
      const list = Array.isArray(state.messagesByChat[jid]) ? [...state.messagesByChat[jid]] : [];
      if (list.length === 0) break;

      list.sort((a, b) => Number(a?.timestampMs || 0) - Number(b?.timestampMs || 0));
      const oldest = list[0];
      const oldestTs = Number(oldest?.timestampMs || 0);
      if (!oldest?.key?.id || oldestTs <= 0) break;
      if (oldestTs <= cutoff) break;

      const oldestHash = messageKeyHash(oldest?.key || {});
      if (oldestHash && oldestHash === previousOldestHash) break;
      previousOldestHash = oldestHash;

      try {
        await sock.fetchMessageHistory(
          pageSize,
          {
            remoteJid: jid,
            id: oldest.key.id,
            fromMe: oldest.key.fromMe === true,
            participant: oldest.key.participant || undefined
          },
          Math.floor(oldestTs / 1000)
        );
        fetchRequests++;
      } catch (e) {
        log.debug({ err: e, profileId, chatJid: jid }, "Failed to fetch chat history for opened chat");
        break;
      }

      await new Promise((r) => setTimeout(r, 140));
    }

    pruneSingleChatToLookback(profileId, jid, days);
    schedulePersistWaChatCache();
    scheduleWaChatSync(profileId, "history_warm_chat");
    return {
      ok: true,
      profileId,
      chatJid: jid,
      days,
      fetchRequests
    };
  })().finally(() => {
    waHistoryWarmupInFlightByChat.delete(taskKey);
  });

  waHistoryWarmupInFlightByChat.set(taskKey, task);
  return await task;
}

async function warmRecentHistoryForProfile(profileId, options) {
  const opts = options && typeof options === "object" ? options : {};
  if (!profileId || !isConnected || !sock) return { ok: false, skipped: true, reason: "not_connected" };
  const force = opts.force === true;

  const existingTask = waHistoryWarmupInFlightByProfile.get(profileId);
  if (existingTask && !force) return await existingTask;

  const task = (async () => {
    const cutoff = lookbackCutoffMs(opts.days || WA_HISTORY_LOOKBACK_DAYS);
    let syncTriggered = false;

    try {
      if (typeof sock.resyncAppState === "function") {
        await sock.resyncAppState(
          ["critical_unblock_low", "critical_block", "regular_high", "regular_low", "regular"],
          true
        );
        syncTriggered = true;
      }
    } catch (e) {
      log.debug({ err: e, profileId }, "Initial app-state history sync failed");
    }

    const state = ensureWaChatStateForProfile(profileId);
    const chats = getRecentChatsForProfile(profileId, { limit: 80 });
    let fetchRequests = 0;

    if (typeof sock.fetchMessageHistory === "function") {
      const maxFetchPerWarmup = 12;
      for (const chat of chats) {
        if (fetchRequests >= maxFetchPerWarmup) break;
        const chatJid = normalizeChatJid(chat?.jid || "");
        if (!chatJid) continue;
        const list = Array.isArray(state.messagesByChat[chatJid]) ? [...state.messagesByChat[chatJid]] : [];
        if (list.length === 0) continue;
        list.sort((a, b) => Number(a.timestampMs || 0) - Number(b.timestampMs || 0));
        const oldest = list[0];
        const oldestTs = Number(oldest?.timestampMs || 0);
        if (!oldest?.key?.id || oldestTs <= 0) continue;
        if (oldestTs <= cutoff) continue;

        try {
          await sock.fetchMessageHistory(
            50,
            {
              remoteJid: chatJid,
              id: oldest.key.id,
              fromMe: oldest.key.fromMe === true,
              participant: oldest.key.participant || undefined
            },
            Math.floor(oldestTs / 1000)
          );
          fetchRequests++;
          await new Promise((r) => setTimeout(r, 140));
        } catch (e) {
          log.debug({ err: e, profileId, chatJid }, "Failed to fetch older message history for chat");
        }
      }
    }

    pruneChatStoreToLookback(profileId, opts.days || WA_HISTORY_LOOKBACK_DAYS);
    schedulePersistWaChatCache();
    scheduleWaChatSync(profileId, "history_warmup");
    return {
      ok: true,
      profileId,
      syncTriggered,
      fetchRequests
    };
  })()
    .finally(() => {
      waHistoryWarmupInFlightByProfile.delete(profileId);
    });

  waHistoryWarmupInFlightByProfile.set(profileId, task);
  return await task;
}

function findMessageRecordForProfile(profileId, chatJid, key) {
  const jid = normalizeChatJid(chatJid);
  if (!jid) return null;
  const normalizedKey = normalizeMessageKey(key, jid);
  if (!normalizedKey.id) return null;
  const state = ensureWaChatStateForProfile(profileId);
  const list = Array.isArray(state.messagesByChat[jid]) ? state.messagesByChat[jid] : [];
  const hash = messageKeyHash(normalizedKey);
  return list.find((x) => x && x.hash === hash) || null;
}

async function markChatReadForProfile(profileId, chatJid) {
  const jid = normalizeChatJid(chatJid);
  if (!jid) throw new Error("Invalid chat JID");
  const state = ensureWaChatStateForProfile(profileId);
  const chat = ensureChatSummary(profileId, jid);
  if (!chat) return { ok: true, skipped: true };

  const list = Array.isArray(state.messagesByChat[jid]) ? state.messagesByChat[jid] : [];
  const incoming = [...list]
    .filter((m) => m && m.fromMe !== true && m.key && m.key.id)
    .sort((a, b) => Number(a?.timestampMs || 0) - Number(b?.timestampMs || 0));
  const unreadKeyHashes = new Set();
  const unreadKeys = [];

  for (let i = incoming.length - 1; i >= 0; i--) {
    const msg = incoming[i];
    const key = {
      remoteJid: normalizeChatJid(msg?.key?.remoteJid || jid) || jid,
      id: cleanString(msg?.key?.id || ""),
      fromMe: false,
      participant: cleanString(msg?.key?.participant || "") || undefined
    };
    if (!key.remoteJid || !key.id) continue;
    const hash = messageKeyHash(key);
    if (unreadKeyHashes.has(hash)) continue;
    unreadKeyHashes.add(hash);
    unreadKeys.push(key);
    if (unreadKeys.length >= 40) break;
  }
  const unreadKeysForSend = [...unreadKeys].reverse();

  const lastMsgInChat =
    list.length > 0
      ? [...list].sort((a, b) => Number(a?.timestampMs || 0) - Number(b?.timestampMs || 0))[list.length - 1]
      : null;
  const lastIncoming = incoming.length > 0 ? incoming[incoming.length - 1] : null;
  const lastForModifySource = lastIncoming || lastMsgInChat;
  const lastMsgForModify =
    lastForModifySource && lastForModifySource.key && lastForModifySource.key.id
      ? {
          key: {
            remoteJid: normalizeChatJid(lastForModifySource.key.remoteJid || jid) || jid,
            id: cleanString(lastForModifySource.key.id || ""),
            fromMe: lastForModifySource.key.fromMe === true,
            participant: cleanString(lastForModifySource.key.participant || "") || undefined
          },
          messageTimestamp: Math.floor(Number(lastForModifySource.timestampMs || Date.now()) / 1000)
        }
      : null;

  let readMessagesSent = false;
  let readReceiptSent = false;
  let chatModifySent = false;

  if (sock && isConnected && unreadKeysForSend.length > 0 && typeof sock.readMessages === "function") {
    try {
      await sock.readMessages(unreadKeysForSend);
      readMessagesSent = true;
    } catch (e) {
      log.warn({ err: e, profileId, chatJid: jid, keyCount: unreadKeysForSend.length }, "Failed sock.readMessages");
    }
  }

  if (
    sock &&
    isConnected &&
    !readMessagesSent &&
    unreadKeysForSend.length > 0 &&
    typeof sock.sendReceipt === "function"
  ) {
    try {
      for (const key of unreadKeysForSend.slice(-20)) {
        await sock.sendReceipt(key.remoteJid || jid, key.participant || undefined, [key.id], "read");
      }
      readReceiptSent = true;
    } catch (e) {
      log.warn({ err: e, profileId, chatJid: jid }, "Failed sock.sendReceipt read fallback");
    }
  }

  if (sock && isConnected && lastMsgForModify && typeof sock.chatModify === "function") {
    try {
      await sock.chatModify(
        {
          markRead: true,
          lastMessages: [lastMsgForModify]
        },
        jid
      );
      chatModifySent = true;
    } catch (e) {
      log.warn({ err: e, profileId, chatJid: jid }, "Failed sock.chatModify markRead");
    }
  }

  if ((Number(chat.unreadCount || 0) || 0) !== 0) {
    state.chatsByJid[jid] = {
      ...chat,
      unreadCount: 0,
      updatedAt: nowIsoShort()
    };
    scheduleWaChatSync(profileId, "read");
    schedulePersistWaChatCache();
  }
  return { ok: true, chatJid: jid, readMessagesSent, readReceiptSent, chatModifySent };
}

const MIME_BY_EXT = {
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".png": "image/png",
  ".webp": "image/webp",
  ".gif": "image/gif",
  ".bmp": "image/bmp",
  ".svg": "image/svg+xml",
  ".mp4": "video/mp4",
  ".mov": "video/quicktime",
  ".avi": "video/x-msvideo",
  ".mkv": "video/x-matroska",
  ".mp3": "audio/mpeg",
  ".ogg": "audio/ogg",
  ".wav": "audio/wav",
  ".m4a": "audio/mp4",
  ".pdf": "application/pdf",
  ".txt": "text/plain",
  ".csv": "text/csv",
  ".doc": "application/msword",
  ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
  ".xls": "application/vnd.ms-excel",
  ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
  ".ppt": "application/vnd.ms-powerpoint",
  ".pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
  ".zip": "application/zip"
};

function getMimeTypeForPath(filePath) {
  const ext = String(path.extname(String(filePath || "")).toLowerCase());
  if (!ext) return "application/octet-stream";
  return MIME_BY_EXT[ext] || "application/octet-stream";
}

function attachmentKindFromMimeOrPath(mimeType, filePath) {
  const mime = cleanString(mimeType).toLowerCase();
  if (mime.startsWith("image/")) return "image";
  if (mime.startsWith("video/")) return "video";
  if (mime.startsWith("audio/")) return "audio";

  const ext = String(path.extname(String(filePath || "")).toLowerCase());
  if ([".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp"].includes(ext)) return "image";
  if ([".mp4", ".mov", ".avi", ".mkv"].includes(ext)) return "video";
  if ([".mp3", ".ogg", ".wav", ".m4a"].includes(ext)) return "audio";
  return "document";
}

function extensionFromMime(mimeType) {
  const mime = cleanString(mimeType).toLowerCase();
  if (!mime) return "";
  for (const [ext, m] of Object.entries(MIME_BY_EXT)) {
    if (m === mime) return ext;
  }
  if (mime === "application/octet-stream") return ".bin";
  return "";
}

function buildImagePreviewDataUrlFromFile(filePath) {
  const srcPath = cleanString(filePath);
  if (!srcPath || !fs.existsSync(srcPath)) return "";

  try {
    const img = nativeImage.createFromPath(srcPath);
    if (!img || img.isEmpty()) return "";
    const size = img.getSize();
    if (!size || !size.width || !size.height) return "";

    const maxSide = 420;
    let preview = img;
    const largest = Math.max(size.width, size.height);
    if (largest > maxSide) {
      const scale = maxSide / largest;
      const width = Math.max(1, Math.round(size.width * scale));
      const height = Math.max(1, Math.round(size.height * scale));
      const resized = img.resize({ width, height, quality: "good" });
      if (resized && !resized.isEmpty()) preview = resized;
    }

    return preview.toDataURL();
  } catch (e) {
    log.debug({ err: e, filePath: srcPath }, "Failed to build local image preview");
    return "";
  }
}

function sanitizePathSegment(value, fallback = "item") {
  const raw = cleanString(value || "");
  const clean = raw
    .replace(/[<>:"/\\|?*\x00-\x1F]/g, "_")
    .replace(/\s+/g, "_")
    .replace(/\.+$/g, "")
    .slice(0, 72);
  return clean || fallback;
}

function buildStoredImagePath(profileId, messageRecord) {
  const msg = messageRecord && typeof messageRecord === "object" ? messageRecord : {};
  const media = msg.media && typeof msg.media === "object" ? msg.media : {};
  const profilePart = sanitizePathSegment(profileId, "profile");
  const chatPart = sanitizePathSegment(cleanString(msg.chatJid || "").replaceAll("@", "_"), "chat");
  const mediaDir = path.join(userDataDir, "wa_media", "images", profilePart, chatPart);
  ensureDir(mediaDir);

  const keyId = sanitizePathSegment(cleanString(msg?.key?.id || ""), "img_" + Date.now().toString(16));
  let ext = String(path.extname(cleanString(media.fileName || "")).toLowerCase());
  if (!ext || ext.length > 10) ext = extensionFromMime(media.mimeType || "") || ".jpg";
  if (!ext.startsWith(".")) ext = `.${ext}`;

  return path.join(mediaDir, `${keyId}${ext}`);
}

function imageDataUrlFromFilePath(filePath, mimeHint) {
  const srcPath = cleanString(filePath);
  if (!srcPath || !fs.existsSync(srcPath)) return "";
  try {
    const buf = fs.readFileSync(srcPath);
    if (!buf || buf.length === 0) return "";
    const guess = cleanString(getMimeTypeForPath(srcPath) || "").toLowerCase();
    const hinted = cleanString(mimeHint || "").toLowerCase();
    const mime = guess.startsWith("image/") ? guess : hinted.startsWith("image/") ? hinted : "image/jpeg";
    return `data:${mime};base64,${buf.toString("base64")}`;
  } catch (e) {
    log.debug({ err: e, filePath: srcPath }, "Failed to build image data URL from local file");
    return "";
  }
}

function setLocalImagePathForStoredMessage(profileId, chatJid, messageHash, localPath) {
  const jid = normalizeChatJid(chatJid);
  const normalizedPath = cleanString(localPath);
  if (!profileId || !jid || !messageHash || !normalizedPath) return false;

  const state = ensureWaChatStateForProfile(profileId);
  const list = Array.isArray(state.messagesByChat[jid]) ? state.messagesByChat[jid] : [];
  const idx = list.findIndex((x) => x && x.hash === messageHash);
  if (idx < 0) return false;

  const prev = list[idx] && typeof list[idx] === "object" ? list[idx] : {};
  const prevMedia = prev.media && typeof prev.media === "object" ? prev.media : {};
  const prevPath = cleanString(prevMedia.localPath || "");
  const thumbFromFile = buildImagePreviewDataUrlFromFile(normalizedPath);

  const nextMedia = {
    ...prevMedia,
    kind: cleanString(prevMedia.kind || "image") || "image",
    localPath: normalizedPath,
    thumbnailDataUrl: cleanString(prevMedia.thumbnailDataUrl || "") || thumbFromFile || ""
  };

  const changed =
    prevPath !== normalizedPath ||
    (thumbFromFile && cleanString(prevMedia.thumbnailDataUrl || "") !== cleanString(nextMedia.thumbnailDataUrl || ""));
  if (!changed) return false;

  list[idx] = {
    ...prev,
    hasMedia: true,
    media: nextMedia
  };
  schedulePersistWaChatCache();
  scheduleWaChatSync(profileId, "image_saved");
  return true;
}

async function ensureLocalImageForStoredMessage(profileId, chatJid, messageHash) {
  const jid = normalizeChatJid(chatJid);
  if (!profileId || !jid || !messageHash) return { ok: false, reason: "invalid_args" };

  const state = ensureWaChatStateForProfile(profileId);
  const list = Array.isArray(state.messagesByChat[jid]) ? state.messagesByChat[jid] : [];
  const msg = list.find((x) => x && x.hash === messageHash);
  if (!msg) return { ok: false, reason: "not_found" };
  if (!msg.hasMedia || String(msg?.media?.kind || "").toLowerCase() !== "image") {
    return { ok: false, reason: "not_image" };
  }

  const existingPath = cleanString(msg?.media?.localPath || "");
  if (existingPath && fs.existsSync(existingPath)) {
    return { ok: true, localPath: existingPath, fromCache: true };
  }

  if (!sock || !isConnected) return { ok: false, reason: "not_connected" };
  if (!msg.rawMessage || typeof msg.rawMessage !== "object") return { ok: false, reason: "raw_missing" };

  const { downloadMediaMessage } = await getBaileys();
  if (typeof downloadMediaMessage !== "function") return { ok: false, reason: "download_unavailable" };

  let mediaData = null;
  try {
    mediaData = await downloadMediaMessage(msg.rawMessage, "buffer", {}, {
      logger: log,
      reuploadRequest: sock.updateMediaMessage
    });
  } catch (e) {
    return { ok: false, reason: "download_failed", error: String(e?.message || e) };
  }

  const buffer = Buffer.isBuffer(mediaData) ? mediaData : mediaData ? Buffer.from(mediaData) : null;
  if (!buffer || buffer.length === 0) return { ok: false, reason: "empty_buffer" };

  const savePath = buildStoredImagePath(profileId, msg);
  try {
    fs.writeFileSync(savePath, buffer);
  } catch (e) {
    return { ok: false, reason: "write_failed", error: String(e?.message || e) };
  }

  setLocalImagePathForStoredMessage(profileId, jid, messageHash, savePath);
  return { ok: true, localPath: savePath, fromCache: false };
}

function queueAutoSaveImageForHash(profileId, chatJid, messageHash) {
  const jid = normalizeChatJid(chatJid);
  if (!profileId || !jid || !messageHash) return;
  const ticket = `${profileId}|${jid}|${messageHash}`;
  if (waImageAutoSaveInFlight.has(ticket)) return;
  waImageAutoSaveInFlight.add(ticket);

  setTimeout(() => {
    ensureLocalImageForStoredMessage(profileId, jid, messageHash)
      .catch(() => {})
      .finally(() => {
        waImageAutoSaveInFlight.delete(ticket);
      });
  }, 0);
}

function buildDefaultDownloadFileName(messageRecord) {
  const msg = messageRecord && typeof messageRecord === "object" ? messageRecord : {};
  const media = msg.media && typeof msg.media === "object" ? msg.media : {};
  const existing = cleanString(media.fileName || "");
  if (existing) return existing;

  const ts = Number(msg.timestampMs || Date.now());
  const d = new Date(ts);
  const pad = (n) => String(n).padStart(2, "0");
  const stamp = `${d.getFullYear()}${pad(d.getMonth() + 1)}${pad(d.getDate())}_${pad(d.getHours())}${pad(
    d.getMinutes()
  )}${pad(d.getSeconds())}`;
  const ext = extensionFromMime(media.mimeType || "") || ".bin";
  const kind = cleanString(media.kind || "media") || "media";
  return `wa_${kind}_${stamp}${ext}`;
}

async function sendChatMessage(payload) {
  const src = payload && typeof payload === "object" ? payload : {};
  if (!sock || !isConnected) throw new Error("WhatsApp not connected");

  const profileId = getActiveProfileId();
  const chatJid = normalizeSendTargetJid(src.chatJid);
  if (!chatJid) throw new Error("Invalid chat");

  const text = String(src.text || "");
  const trimmedText = text.trim();
  const attachment = src.attachment && typeof src.attachment === "object" ? src.attachment : null;
  if (!trimmedText && !attachment) throw new Error("Message is empty");

  const sendOptions = {};
  if (src.quotedKey && typeof src.quotedKey === "object") {
    const quoted = findMessageRecordForProfile(profileId, chatJid, src.quotedKey);
    if (quoted?.rawMessage) sendOptions.quoted = quoted.rawMessage;
  }

  let messagePayload = null;
  let localImagePreviewDataUrl = "";
  let localImageSourcePath = "";
  if (attachment) {
    const filePath = cleanString(attachment.path || "");
    if (!filePath || !fs.existsSync(filePath)) throw new Error("Attachment file not found");
    const fileName = cleanString(attachment.fileName || path.basename(filePath));
    const mimeType = cleanString(attachment.mimeType || getMimeTypeForPath(filePath));
    const kind = attachmentKindFromMimeOrPath(mimeType, filePath);

    if (kind === "image") {
      localImagePreviewDataUrl = buildImagePreviewDataUrlFromFile(filePath);
      localImageSourcePath = filePath;
      messagePayload = {
        image: { url: filePath },
        ...(trimmedText ? { caption: trimmedText } : {})
      };
    } else if (kind === "video") {
      messagePayload = {
        video: { url: filePath },
        ...(mimeType ? { mimetype: mimeType } : {}),
        ...(trimmedText ? { caption: trimmedText } : {})
      };
    } else if (kind === "audio") {
      messagePayload = {
        audio: { url: filePath },
        ...(mimeType ? { mimetype: mimeType } : {}),
        ptt: false
      };
    } else {
      messagePayload = {
        document: { url: filePath },
        fileName: fileName || path.basename(filePath),
        ...(mimeType ? { mimetype: mimeType } : {}),
        ...(trimmedText ? { caption: trimmedText } : {})
      };
    }
  } else {
    messagePayload = { text: trimmedText };
  }

  const sentMessage = await sock.sendMessage(chatJid, messagePayload, sendOptions);
  if (sentMessage && typeof sentMessage === "object") {
    if (localImagePreviewDataUrl) {
      sentMessage.__localThumbnailDataUrl = localImagePreviewDataUrl;
    }
    if (localImageSourcePath) {
      sentMessage.__localImagePath = localImageSourcePath;
    }
    upsertMessagesForProfile(profileId, [sentMessage]);
  } else {
    scheduleWaChatSync(profileId, "send");
  }

  return {
    ok: true,
    chatJid,
    messageId: cleanString(sentMessage?.key?.id || "")
  };
}

async function downloadChatMedia(payload) {
  const src = payload && typeof payload === "object" ? payload : {};
  if (!sock || !isConnected) throw new Error("WhatsApp not connected");

  const profileId = getActiveProfileId();
  const chatJid = normalizeChatJid(src.chatJid);
  if (!chatJid) throw new Error("Invalid chat");
  if (!src.key || typeof src.key !== "object") throw new Error("Message key is required");

  const found = findMessageRecordForProfile(profileId, chatJid, src.key);
  if (!found || !found.rawMessage) throw new Error("Message not found");
  if (!found.hasMedia) throw new Error("This message has no media");

  const { downloadMediaMessage } = await getBaileys();
  if (typeof downloadMediaMessage !== "function") {
    throw new Error("Media download is unavailable in current Baileys build");
  }

  let mediaData = null;
  try {
    mediaData = await downloadMediaMessage(found.rawMessage, "buffer", {}, {
      logger: log,
      reuploadRequest: sock.updateMediaMessage
    });
  } catch (e) {
    throw new Error(`Failed to download media: ${String(e?.message || e)}`);
  }

  const buffer = Buffer.isBuffer(mediaData) ? mediaData : mediaData ? Buffer.from(mediaData) : null;
  if (!buffer || buffer.length === 0) throw new Error("Media download returned empty file");

  const defaultName = buildDefaultDownloadFileName(found);
  const saveRes = await dialog.showSaveDialog({
    title: "Save media",
    defaultPath: defaultName
  });
  if (saveRes.canceled || !saveRes.filePath) return { ok: false, canceled: true };

  fs.writeFileSync(saveRes.filePath, buffer);
  return {
    ok: true,
    filePath: saveRes.filePath,
    size: buffer.length
  };
}

async function resolveImagePreview(payload) {
  const src = payload && typeof payload === "object" ? payload : {};
  const profileId = getActiveProfileId();
  const chatJid = normalizeChatJid(src.chatJid || "");
  if (!chatJid) throw new Error("Invalid chat");
  if (!src.key || typeof src.key !== "object") throw new Error("Message key is required");

  const found = findMessageRecordForProfile(profileId, chatJid, src.key);
  if (!found) throw new Error("Message not found");
  if (!found.hasMedia || String(found?.media?.kind || "").toLowerCase() !== "image") {
    throw new Error("Message is not an image");
  }

  let localPath = cleanString(found?.media?.localPath || "");
  if (!localPath || !fs.existsSync(localPath)) {
    const ensured = await ensureLocalImageForStoredMessage(profileId, chatJid, found.hash);
    if (ensured?.ok && ensured.localPath) localPath = cleanString(ensured.localPath);
  }

  if (localPath && fs.existsSync(localPath)) {
    const fullDataUrl = imageDataUrlFromFilePath(localPath, found?.media?.mimeType);
    if (fullDataUrl) {
      return {
        ok: true,
        dataUrl: fullDataUrl,
        localPath,
        source: "local"
      };
    }
  }

  const thumb = cleanString(found?.media?.thumbnailDataUrl || "");
  if (thumb) {
    return {
      ok: true,
      dataUrl: thumb,
      localPath: "",
      source: "thumbnail",
      degraded: true
    };
  }

  throw new Error("Image preview unavailable");
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

async function waitForConnectSetupIdle() {
  while (connectSetupPromise) {
    try {
      await connectSetupPromise;
    } catch (e) {
      // ignore setup errors while waiting for lock release
    }
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
    if (!creds || typeof creds !== "object") return false;
    if (creds.registered === true) return true;

    // Newer/older Baileys snapshots may not always expose `registered`.
    // Treat persisted account identity as a reusable session.
    const hasMe = typeof creds?.me?.id === "string" && creds.me.id.length > 0;
    const hasAccount = !!creds?.account;
    return hasMe || hasAccount;
  } catch (e) {
    return false;
  }
}

async function connectWA(method, attemptId) {
  await waitForConnectSetupIdle();
  if (attemptId !== handshakeAttemptId) return;

  const run = (async () => {
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
      if (attemptId !== handshakeAttemptId) return;
      const { state, saveCreds } = await useMultiFileAuthState(authDir);
      if (attemptId !== handshakeAttemptId) return;

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
      if (attemptId !== handshakeAttemptId) return;

      const waVersion = await getLatestWaVersionSafe(fetchLatestBaileysVersion);
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
        const contacts = Array.isArray(history?.contacts) ? history.contacts : [];
        const chats = Array.isArray(history?.chats) ? history.chats : [];
        const messages = Array.isArray(history?.messages) ? history.messages : [];

        upsertContactsForProfile(activeProfileId, contacts);
        upsertChatsAsContactsForProfile(activeProfileId, chats);
        upsertMessagesAsContactsForProfile(activeProfileId, messages);
        upsertChatsForProfile(activeProfileId, chats);
        upsertMessagesForProfile(activeProfileId, messages);
      });
      sock.ev.on("contacts.upsert", (contacts) => {
        const rows = Array.isArray(contacts) ? contacts : [];
        if (upsertContactsForProfile(activeProfileId, rows) > 0) {
          scheduleWaChatSync(activeProfileId, "contacts");
        }
      });
      sock.ev.on("contacts.update", (contacts) => {
        const rows = Array.isArray(contacts) ? contacts : [];
        if (upsertContactsForProfile(activeProfileId, rows) > 0) {
          scheduleWaChatSync(activeProfileId, "contacts");
        }
      });
      sock.ev.on("chats.upsert", (chats) => {
        const rows = Array.isArray(chats) ? chats : [];
        upsertChatsAsContactsForProfile(activeProfileId, rows);
        upsertChatsForProfile(activeProfileId, rows);
      });
      sock.ev.on("chats.set", (evt) => {
        const rows = Array.isArray(evt?.chats) ? evt.chats : [];
        upsertChatsAsContactsForProfile(activeProfileId, rows);
        upsertChatsForProfile(activeProfileId, rows);
      });
      sock.ev.on("chats.update", (chats) => {
        const rows = Array.isArray(chats) ? chats : [];
        upsertChatsAsContactsForProfile(activeProfileId, rows);
        upsertChatsForProfile(activeProfileId, rows);
      });
      sock.ev.on("messages.upsert", (evt) => {
        const rows = Array.isArray(evt?.messages) ? evt.messages : [];
        upsertMessagesAsContactsForProfile(activeProfileId, rows);
        upsertMessagesForProfile(activeProfileId, rows);
      });
      sock.ev.on("messages.set", (evt) => {
        const rows = Array.isArray(evt?.messages) ? evt.messages : [];
        upsertMessagesAsContactsForProfile(activeProfileId, rows);
        upsertMessagesForProfile(activeProfileId, rows);
      });
      sock.ev.on("messages.update", (updates) => {
        applyMessageUpdatesForProfile(activeProfileId, Array.isArray(updates) ? updates : []);
      });

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
          invalidSessionProfiles.delete(activeProfileId);
          updateConnectedProfileMeta(activeProfileId, sock?.user || {});
          syncContactNamesForProfile(activeProfileId, { force: false, isInitialSync: false }).catch(() => {});
          scheduleWaChatSync(activeProfileId, "connection_open");
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
          scheduleWaChatSync(activeProfileId, "connection_close");

          win?.webContents.send("wa:status", {
            connected: false,
            text: `Disconnected (${reason})`,
            profileId: activeProfileId
          });

          // WhatsApp commonly requests one post-login restart after QR scan.
          // Allow exactly one controlled reconnect for this handshake attempt.
          if (code === DisconnectReason.restartRequired) {
            if (!restartRecoveryDoneByAttempt.has(attemptId)) {
              restartRecoveryDoneByAttempt.add(attemptId);
              win?.webContents.send("wa:status", {
                connected: false,
                text: "Restart required by WhatsApp. Reconnecting once...",
                profileId: activeProfileId
              });
              setTimeout(() => {
                stopSocket()
                  .then(() => connectWA(handshakeState.method, attemptId))
                  .catch(() => {});
              }, 500);
              return;
            }
            win?.webContents.send("wa:status", {
              connected: false,
              text: "Disconnected (restartRequired). Click Handshake once.",
              profileId: activeProfileId
            });
            return;
          }

          // Auto-retry is intentionally disabled. If a connect attempt fails once,
          // we stop immediately and wait for explicit user action.
          const needsFreshAuth =
            code === DisconnectReason.loggedOut ||
            code === DisconnectReason.badSession ||
            code === DisconnectReason.multideviceMismatch ||
            code === DisconnectReason.forbidden;
          if (needsFreshAuth) {
            invalidSessionProfiles.add(activeProfileId);
            win?.webContents.send("wa:status", {
              connected: false,
              text: "Session invalid. Click Handshake to reset and generate new QR.",
              profileId: activeProfileId
            });
          }
        }
      });
    } finally {
      isConnecting = false;
    }
  })();

  connectSetupPromise = run;
  try {
    await run;
  } finally {
    if (connectSetupPromise === run) connectSetupPromise = null;
  }
}

async function startHandshake(payload) {
  const method = payload?.method === "pairing" ? "pairing" : "qr";
  const phoneNumber = payload?.phoneNumber ? normalizeE164NoPlus(payload.phoneNumber) : "";
  const activeProfileId = getActiveProfileId();
  const authDir = getProfileAuthDir(activeProfileId);

  // One-shot recovery for invalid creds, without background retry loops.
  if (invalidSessionProfiles.has(activeProfileId)) {
    clearAuthDir(authDir);
    invalidSessionProfiles.delete(activeProfileId);
    clearWaChatStateForProfile(activeProfileId);
  }

  // If profile is not fully registered yet, clear partial auth files first
  // to force a clean QR/pairing handshake.
  if (!hasRegisteredSession(activeProfileId)) {
    clearAuthDir(authDir);
    clearWaChatStateForProfile(activeProfileId);
  }

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
      text: "No saved session. Click Handshake to connect.",
      profileId: activeProfileId
    });
    return { ok: false, skipped: true, profileId: activeProfileId };
  }
  handshakeAttemptId++;
  handshakeState = { method: "qr", phoneNumber: "", pairingRequested: false };
  await stopSocket();
  await connectWA("qr", handshakeAttemptId);
  return { ok: true, started: true, profileId: activeProfileId };
}

function getConnectionState() {
  const activeProfileId = getActiveProfileId();
  return {
    ok: true,
    connected: !!isConnected,
    connecting: !!isConnecting,
    profileId: activeProfileId,
    text: isConnected ? "Connected" : isConnecting ? "Connecting..." : "Not connected"
  };
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

ipcMain.handle("app:getAppointmentTemplates", async () => {
  return { ok: true, templates: getAppointmentTemplates() };
});

ipcMain.handle("app:saveAppointmentTemplates", async (_evt, templates) => {
  const saved = saveAppointmentTemplates(templates || {});
  return { ok: true, templates: saved };
});

ipcMain.handle("app:getClinicSettings", async () => {
  return { ok: true, settings: getClinicSettings() };
});

ipcMain.handle("app:saveClinicSettings", async (_evt, settings) => {
  const saved = saveClinicSettings(settings || {});
  return { ok: true, settings: saved };
});

ipcMain.handle("app:exportTemplatesBundle", async () => {
  const saveRes = await dialog.showSaveDialog({
    title: "Export templates",
    defaultPath: `clinic_templates_${new Date().toISOString().slice(0, 10)}.json`,
    filters: [{ name: "JSON", extensions: ["json"] }]
  });
  if (saveRes.canceled || !saveRes.filePath) return { ok: false, canceled: true };

  const payload = getTemplateExportBundle();
  fs.writeFileSync(saveRes.filePath, JSON.stringify(payload, null, 2), "utf-8");
  return { ok: true, filePath: saveRes.filePath };
});

ipcMain.handle("app:importTemplatesBundle", async () => {
  const openRes = await dialog.showOpenDialog({
    title: "Import templates",
    properties: ["openFile"],
    filters: [{ name: "JSON", extensions: ["json"] }]
  });
  if (openRes.canceled || !openRes.filePaths || openRes.filePaths.length === 0) return { ok: false, canceled: true };

  const filePath = openRes.filePaths[0];
  const text = fs.readFileSync(filePath, "utf-8");
  let parsed = null;
  try {
    parsed = text ? JSON.parse(text) : {};
  } catch (e) {
    throw new Error("Invalid JSON file");
  }

  const result = importTemplateBundle(parsed);
  return { ok: true, filePath, ...result };
});

ipcMain.handle("clinic:getSession", async () => {
  const session = getAuthSession();
  return { ok: true, session };
});

ipcMain.handle("clinic:login", async (_evt, payload) => {
  const src = payload && typeof payload === "object" ? payload : {};
  const token = await clinicLogin(src.email, src.password);
  const user = await clinicGetMe(token);
  const session = saveAuthSession(token, user);
  return { ok: true, session };
});

ipcMain.handle("clinic:logout", async () => {
  clearAuthSession();
  return { ok: true };
});

ipcMain.handle("clinic:refreshMe", async () => {
  const session = getAuthSession();
  if (!session.authToken) throw new Error("Not logged in");
  const user = await clinicGetMe(session.authToken);
  const saved = saveAuthSession(session.authToken, user);
  return { ok: true, session: saved };
});

ipcMain.handle("clinic:getBranchList", async () => {
  const session = getAuthSession();
  if (!session.authToken) throw new Error("Not logged in");
  const branches = await clinicGetBranchList(session.authToken);
  return { ok: true, branches };
});

ipcMain.handle("clinic:getAppointmentList", async (_evt, payload) => {
  const session = getAuthSession();
  if (!session.authToken) throw new Error("Not logged in");
  const appointments = await clinicGetAppointmentList(session.authToken, payload || {});
  return { ok: true, appointments };
});

ipcMain.handle("clinic:getPatient", async (_evt, payload) => {
  const session = getAuthSession();
  if (!session.authToken) throw new Error("Not logged in");
  const icNumber = payload && typeof payload === "object" ? payload.ic_number || payload.icNumber : payload;
  const patient = await clinicGetPatient(session.authToken, icNumber);
  return { ok: true, patient };
});

ipcMain.handle("clinic:getPastPatients", async (_evt, payload) => {
  const session = getAuthSession();
  if (!session.authToken) throw new Error("Not logged in");
  const records = await clinicGetPastPatients(session.authToken, payload || {});
  return { ok: true, patients: records };
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

ipcMain.handle("app:renameProfile", async (_evt, profileId, name) => {
  const profile = renameProfile(profileId, name);
  return { ok: true, profile };
});

ipcMain.handle("app:terminateProfileSession", async (_evt, profileId) => {
  return await terminateProfileSession(profileId);
});

ipcMain.handle("app:deleteProfile", async (_evt, profileId) => {
  return await deleteProfile(profileId);
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

ipcMain.handle("wa:getConnectionState", async () => {
  return getConnectionState();
});

ipcMain.handle("wa:getContacts", async (_evt, options) => {
  const profileId = getActiveProfileId();
  const opts = options && typeof options === "object" ? options : {};
  let contacts = getContactsForProfile(profileId);

  if (isConnected && sock) {
    if (opts.forceNameSync) {
      await syncContactNamesForProfile(profileId, { force: true, isInitialSync: false, cooldownMs: 0 });
      contacts = getContactsForProfile(profileId);
    } else if (contacts.length > 0 && contacts.some((c) => !contactHasAnyName(c))) {
      syncContactNamesForProfile(profileId, { force: false, isInitialSync: false }).catch(() => {});
    }
  }

  if (opts.includePhotos && isConnected && sock) {
    contacts = await enrichContactPhotosForProfile(profileId, opts);
  }
  return {
    ok: true,
    connected: !!isConnected,
    profileId,
    contacts,
    count: contacts.length
  };
});

ipcMain.handle("wa:getRecentChats", async (_evt, options) => {
  const profileId = getActiveProfileId();
  const opts = options && typeof options === "object" ? options : {};

  if (isConnected && sock) {
    if (opts.ensureHistory === true && opts.scopeAllHistory === true) {
      await warmRecentHistoryForProfile(profileId, {
        days: WA_HISTORY_LOOKBACK_DAYS,
        force: opts.forceHistory === true
      });
    }

    if (opts.forceNameSync === true) {
      await syncContactNamesForProfile(profileId, { force: true, isInitialSync: false, cooldownMs: 0 });
    }

    const includePhotos = opts.includePhotos !== false;
    if (includePhotos) {
      ensureContactsFromChatsForProfile(profileId);
      enrichContactPhotosForProfile(profileId, {
        maxPhotoFetch: Number.isFinite(Number(opts.maxPhotoFetch)) ? Number(opts.maxPhotoFetch) : 40,
        minMinutesBetweenPhotoChecks: Number.isFinite(Number(opts.minMinutesBetweenPhotoChecks))
          ? Number(opts.minMinutesBetweenPhotoChecks)
          : 120,
        photoFetchConcurrency: Number.isFinite(Number(opts.photoFetchConcurrency))
          ? Number(opts.photoFetchConcurrency)
          : 3
      })
        .then(() => {
          scheduleWaChatSync(profileId, "photos");
        })
        .catch(() => {});
    }
  }

  const chats = getRecentChatsForProfile(profileId, opts);
  return {
    ok: true,
    connected: !!isConnected,
    profileId,
    chats,
    count: chats.length
  };
});

ipcMain.handle("wa:getChatMessages", async (_evt, payload) => {
  const src = payload && typeof payload === "object" ? payload : {};
  const profileId = getActiveProfileId();
  const chatJid = normalizeChatJid(src.chatJid || "");
  if (!chatJid) throw new Error("Invalid chat");
  const messages = getChatMessagesForProfile(profileId, chatJid, src || {});
  return {
    ok: true,
    connected: !!isConnected,
    profileId,
    chatJid,
    messages,
    count: messages.length
  };
});

ipcMain.handle("wa:warmChatHistory", async (_evt, payload) => {
  const src = payload && typeof payload === "object" ? payload : {};
  const profileId = getActiveProfileId();
  const chatJid = normalizeChatJid(src.chatJid || "");
  if (!chatJid) throw new Error("Invalid chat");
  return await warmHistoryForChat(profileId, chatJid, src || {});
});

ipcMain.handle("wa:markChatRead", async (_evt, payload) => {
  const src = payload && typeof payload === "object" ? payload : {};
  const profileId = getActiveProfileId();
  const chatJid = normalizeChatJid(src.chatJid || "");
  if (!chatJid) throw new Error("Invalid chat");
  return await markChatReadForProfile(profileId, chatJid);
});

ipcMain.handle("wa:sendChatMessage", async (_evt, payload) => {
  return await sendChatMessage(payload || {});
});

ipcMain.handle("wa:pickAttachment", async () => {
  const openRes = await dialog.showOpenDialog({
    title: "Select attachment",
    properties: ["openFile"],
    filters: [
      { name: "Images", extensions: ["jpg", "jpeg", "png", "gif", "webp", "bmp"] },
      { name: "Videos", extensions: ["mp4", "mov", "avi", "mkv"] },
      { name: "Documents", extensions: ["pdf", "doc", "docx", "xls", "xlsx", "ppt", "pptx", "txt", "csv", "zip"] },
      { name: "All files", extensions: ["*"] }
    ]
  });
  if (openRes.canceled || !openRes.filePaths || openRes.filePaths.length === 0) {
    return { ok: false, canceled: true };
  }

  const filePath = openRes.filePaths[0];
  const stat = fs.statSync(filePath);
  const fileName = path.basename(filePath);
  const mimeType = getMimeTypeForPath(filePath);
  const kind = attachmentKindFromMimeOrPath(mimeType, filePath);
  return {
    ok: true,
    attachment: {
      path: filePath,
      fileName,
      mimeType,
      kind,
      size: Number(stat.size || 0) || 0
    }
  };
});

ipcMain.handle("wa:downloadMedia", async (_evt, payload) => {
  return await downloadChatMedia(payload || {});
});

ipcMain.handle("wa:resolveImagePreview", async (_evt, payload) => {
  return await resolveImagePreview(payload || {});
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
  const clinicSession = getAuthSession();
  const sessionAuthToken = cleanString(clinicSession?.authToken);
  const aiAuthToken = cleanString(aiCfg.authToken) || sessionAuthToken;
  if (aiCfg.enabled && !aiCfg.endpoint) {
    throw new Error("AI rewrite is enabled but backend endpoint is empty");
  }
  if (aiCfg.enabled && !aiCfg.prompt) {
    throw new Error("AI rewrite is enabled but prompt is empty");
  }
  if (aiCfg.enabled && !aiAuthToken) {
    throw new Error("AI rewrite requires Authorization token. Please log in first.");
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
            authToken: aiAuthToken,
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

ipcMain.handle("wa:sendPreparedBatch", async (_evt, payload) => {
  const src = payload && typeof payload === "object" ? payload : {};
  const items = Array.isArray(src.items) ? src.items : [];
  if (items.length === 0) throw new Error("No messages to send");
  if (!isConnected || !sock) throw new Error("WhatsApp not connected");

  const pacing = src.pacing && typeof src.pacing === "object" ? src.pacing : {};
  const pattern = pacing.pattern === "cycle" ? "cycle" : "random";
  const minSec = clampInt(pacing.minSec, 1, 90, 7);
  const maxSec = Math.max(minSec, clampInt(pacing.maxSec, 1, 90, 10));
  const safety = src.safety && typeof src.safety === "object" ? src.safety : {};
  const maxRecipients = Math.max(1, Number(safety.maxRecipients || 500));
  if (items.length > maxRecipients) throw new Error(`Too many recipients. Limit is ${maxRecipients}.`);

  const aiCfg = normalizeAiRewriteConfig({ ...getAiRewriteConfig(), ...(src.aiRewrite || {}) });
  const clinicSession = getAuthSession();
  const sessionAuthToken = cleanString(clinicSession?.authToken);
  const aiAuthToken = cleanString(aiCfg.authToken) || sessionAuthToken;
  if (aiCfg.enabled && !aiCfg.endpoint) throw new Error("AI rewrite endpoint is required");
  if (aiCfg.enabled && !aiCfg.prompt) throw new Error("AI rewrite prompt is required");
  if (aiCfg.enabled && !aiAuthToken) {
    throw new Error("AI rewrite requires Authorization token. Please log in first.");
  }

  let sent = 0;
  let failed = 0;
  let skipped = 0;

  for (let i = 0; i < items.length; i++) {
    const item = items[i] && typeof items[i] === "object" ? items[i] : {};
    const rawPhone = cleanString(item.phone);
    const rowName = cleanString(item.name);
    const templateId = cleanString(item.templateId) || cleanString(src.batchLabel) || "prepared";
    let msisdn = "";

    try {
      msisdn = normalizeMsisdn(rawPhone);
    } catch (e) {
      failed++;
      win?.webContents.send("batch:progress", {
        ts: nowIsoShort(),
        index: i + 1,
        total: items.length,
        phone: rawPhone,
        name: rowName,
        status: "failed",
        error: "Invalid phone number"
      });
      continue;
    }

    const baseText = String(item.text || "").trim();
    if (!baseText) {
      skipped++;
      win?.webContents.send("batch:progress", {
        ts: nowIsoShort(),
        index: i + 1,
        total: items.length,
        phone: msisdn,
        name: rowName,
        status: "skipped",
        error: "Message is empty"
      });
      continue;
    }

    try {
      let text = baseText;
      if (aiCfg.enabled) {
        const aiVars = item.aiVariables && typeof item.aiVariables === "object" ? item.aiVariables : {};
        const aiPromptTemplate = cleanString(item.aiPrompt) || aiCfg.prompt;
        try {
          text = await rewriteMessageViaBackend({
            endpoint: aiCfg.endpoint,
            authToken: aiAuthToken,
            prompt: aiPromptTemplate,
            message: baseText,
            variables: aiVars,
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
            total: items.length,
            phone: msisdn,
            name: rowName,
            status: "sending",
            error: `AI fallback: ${String(aiErr?.message || aiErr)}`
          });
        }
      }

      win?.webContents.send("batch:progress", {
        ts: nowIsoShort(),
        index: i + 1,
        total: items.length,
        phone: msisdn,
        name: rowName,
        status: "sending"
      });

      await sendText(msisdn, text);
      sent++;

      win?.webContents.send("batch:progress", {
        ts: nowIsoShort(),
        index: i + 1,
        total: items.length,
        phone: msisdn,
        name: rowName,
        status: "sent"
      });
    } catch (e) {
      failed++;
      win?.webContents.send("batch:progress", {
        ts: nowIsoShort(),
        index: i + 1,
        total: items.length,
        phone: msisdn,
        name: rowName,
        status: "failed",
        error: String(e?.message || e)
      });
    }

    if (i < items.length - 1) {
      const ms = delayMsFromPattern(pattern, minSec, maxSec, i);
      await new Promise((r) => setTimeout(r, ms));
    }
  }

  return { ok: true, sent, failed, skipped, total: items.length };
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

  win.webContents.once("did-finish-load", () => {
    if (startupReconnectScheduled) return;
    startupReconnectScheduled = true;
    autoReconnectActiveProfile().catch((e) => {
      log.warn({ err: e }, "Startup auto reconnect failed");
    });
  });

  win.loadFile(path.join(__dirname, "renderer", "index.html"));
}

app.whenReady().then(() => {
  ensureDataFiles();
  loadProfiles();
  loadPersistedContactsCache();
  loadPersistedWaChatCache();
  createWindow();
});

app.on("before-quit", () => {
  try {
    if (waChatPersistTimer) {
      clearTimeout(waChatPersistTimer);
      waChatPersistTimer = null;
    }
    persistWaChatCache();
  } catch (e) {
    // ignore
  }
});

app.on("window-all-closed", () => {
  if (process.platform !== "darwin") app.quit();
});
