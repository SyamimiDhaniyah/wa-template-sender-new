const electron = require("electron");
const { spawn } = require("child_process");

if ((!electron || typeof electron !== "object" || !electron.app) && process.env.ELECTRON_RUN_AS_NODE) {
  // Self-heal when launched from an environment that forces Electron into Node mode.
  const relaunchEnv = { ...process.env };
  delete relaunchEnv.ELECTRON_RUN_AS_NODE;
  try {
    const child = spawn(process.execPath, process.argv.slice(1), {
      detached: true,
      stdio: "ignore",
      env: relaunchEnv
    });
    child.unref();
  } catch {
    // ignore; explicit error below handles unrecoverable startup failures
  }
  process.exit(0);
}

const { app, BrowserWindow, ipcMain, dialog, nativeImage } = electron;
if (!app) {
  throw new Error("Electron app module unavailable. Ensure ELECTRON_RUN_AS_NODE is not set.");
}
const path = require("path");
const fs = require("fs");
const crypto = require("crypto");
const Store = require("electron-store");

const log = {
  info: console.log,
  warn: console.warn,
  error: console.error
};


// Removed pino init

let win = null;
let isConnecting = false;
let isConnected = false;
let waContactsByProfileMem = {};
const invalidSessionProfiles = new Set();
const restartRecoveryDoneByAttempt = new Set();
const contactNameSyncByProfile = new Map();
const lastContactNameSyncAtByProfile = new Map();
const appStateResyncInFlightByProfile = new Map();
const appStateResyncBackoffUntilByProfile = new Map();
const contactPhotoEnrichInFlightByProfile = new Map();
const contactPhotoBackoffUntilByProfile = new Map();
const contactLookupCacheByProfile = new Map();
const profileSelfIdentityCache = new Map();
const waLidPnMapByProfileMem = {};
const startupPhotoBackfillDoneByProfile = new Set();

// Used to ignore late events from old sockets
let handshakeAttemptId = 0;
let startupReconnectScheduled = false;
const WA_CHAT_MAX_MESSAGES_PER_CHAT = 180;
const WA_HISTORY_LOOKBACK_DAYS = 7;
const WA_CHAT_MAX_STORED_CHATS = 360;
const WA_CHAT_STORE_KEY = "waChatByProfile";
const waChatByProfileMem = {};
const waChatSyncTimers = new Map();
const waHistoryWarmupInFlightByProfile = new Map();
const waAliasReconcileTimers = new Map();
const waImageAutoSaveInFlight = new Set();
const WA_ALLOWED_OUTGOING_CHAT_PRESENCE = new Set(["composing", "paused", "recording"]);

// --- Go Backend Sidecar ---
let goBackendProcess = null;

function resolveGoBackendExecutable(backendDir) {
  const candidates = [];

  if (process.platform === "win32") {
    candidates.push("go-backend.exe");
  } else if (process.platform === "darwin") {
    candidates.push(process.arch === "arm64" ? "go-backend-mac-arm64" : "go-backend-mac-amd64");
    candidates.push("go-backend");
  } else {
    candidates.push("go-backend");
  }

  for (const fileName of candidates) {
    const fullPath = path.join(backendDir, fileName);
    if (!fs.existsSync(fullPath)) continue;

    if (process.platform !== "win32") {
      try {
        fs.chmodSync(fullPath, 0o755);
      } catch (error) {
        console.warn("Unable to ensure backend executable permissions:", error);
      }
    }

    return fullPath;
  }

  throw new Error(`Go backend executable not found in ${backendDir}`);
}

function startGoBackend() {
  if (goBackendProcess) return;
  // In a packaged Electron app, __dirname points inside app.asar which is read-only.
  // Executables must live in app.asar.unpacked instead.
  const backendDir = app.isPackaged
    ? path.join(process.resourcesPath, "app.asar.unpacked", "go-backend")
    : path.join(__dirname, "go-backend");
  const backendPath = resolveGoBackendExecutable(backendDir);
  console.log("Starting Go Backend...");
  console.log("Using Go backend executable:", backendPath);
  goBackendProcess = spawn(backendPath, [], {
    cwd: backendDir,
    stdio: ["ignore", "pipe", "pipe"]
  });

  goBackendProcess.stdout.on("data", (data) => {
    const output = data.toString();
    const lines = output.split("\n");
    for (let line of lines) {
      line = line.trim();
      if (!line) continue;
      try {
        const payload = JSON.parse(line);
        if (payload && payload.type) {
          if (payload.type === "chatSync") {
            const profileId = getActiveProfileId();
            if (profileId && Array.isArray(payload.data)) {
              upsertMessagesForProfile(profileId, payload.data);
            }
            win?.webContents.send("wa:chatSync", { profileId });
          } else if (payload.type === "historySync") {
            const profileId = getActiveProfileId();
            if (profileId && payload.data?.file) {
              try {
                const fs = require("fs");
                const jStr = fs.readFileSync(payload.data.file, "utf8");
                const msgs = JSON.parse(jStr);
                if (Array.isArray(msgs)) upsertMessagesForProfile(profileId, msgs);
                fs.unlinkSync(payload.data.file);
              } catch (e) {
                console.error("History sync file error:", e);
              }
            }
            win?.webContents.send("wa:chatSync", { profileId });
          } else if (payload.type === "presence") {
            win?.webContents.send("wa:presence", payload.data);
          } else if (payload.type === "status") {
            isConnected = (payload.data === "connected");
            win?.webContents.send("wa:status", {
              isConnected,
              connected: isConnected,
              text: isConnected ? "Connected" : (payload.data === "logged_out" ? "Logged Out" : "Not connected"),
              profileId: getActiveProfileId()
            });
          }
        } else {
          console.log("[Go]", line);
        }
      } catch (e) {
        // Not JSON, just standard log
        console.log("[Go]", line);
      }
    }
  });

  goBackendProcess.stderr.on("data", (data) => {
    console.error("[Go Error]", data.toString().trim());
  });

  goBackendProcess.on("error", (err) => {
    console.error("Failed to start Go sidecar:", err);
  });
  goBackendProcess.on("close", (code) => {
    console.log(`Go sidecar exited with code ${code}`);
    goBackendProcess = null;
  });
}

function killGoBackend() {
  if (goBackendProcess) {
    console.log("Killing Go backend process...");
    goBackendProcess.kill("SIGTERM");
    goBackendProcess = null;
  }
}
// --------------------------
const WA_ALLOWED_INCOMING_CHAT_PRESENCE = new Set(["composing", "recording", "paused", "available", "unavailable"]);
const WA_CHAT_PERSIST_DEBOUNCE_MS = 2200;
const WA_PERSISTED_THUMB_MAX_CHARS = 18 * 1024;
const WA_ENABLE_AUTO_IMAGE_PRESAVE = false;
let waChatPersistTimer = null;
let activeBatchJob = null;

const store = new Store();

const userDataDir = app.getPath("userData");
const profilesRootDir = path.join(userDataDir, "wa_profiles");

const bundledDataDir = path.join(__dirname, "data");
const dataDir = app.isPackaged ? path.join(userDataDir, "data") : bundledDataDir;
const templatesFile = path.join(dataDir, "templates.json");
const templateMediaDir = path.join(dataDir, "template_media");
const CLINIC_API_BASE = "https://xqoc-ewo0-x3u2.s2.xano.io";
const CLINIC_TZ = "Asia/Kuala_Lumpur";
const CLINIC_AUTH_SESSION_KEY = "clinicAuthSession";
const CLINIC_SETTINGS_KEY = "clinicSettings";
const CLINIC_APPOINTMENT_TEMPLATES_KEY = "clinicAppointmentTemplates";
const CLINIC_API_KEY_AUTH = "api:s4bMNy03";
const CLINIC_API_KEY_DATA = "api:lY50ALPv";
const DEFAULT_WA_PROFILE_NAME = "Dentabay";

const DEFAULT_CLINIC_SETTINGS = {
  timezone: CLINIC_TZ,
  gapMinSec: 7,
  gapMaxSec: 45,
  templateGapMinSec: 2,
  templateGapMaxSec: 4,
  marketingMonthsAgoDefault: 6,
  marketingPageSizeDefault: 50
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
  const templateGapMinSec = clampInt(src.templateGapMinSec, 1, 30, DEFAULT_CLINIC_SETTINGS.templateGapMinSec);
  const templateGapMaxCandidate = clampInt(
    src.templateGapMaxSec,
    1,
    30,
    DEFAULT_CLINIC_SETTINGS.templateGapMaxSec
  );
  const templateGapMaxSec = Math.max(templateGapMinSec, templateGapMaxCandidate);
  const marketingMonthsAgoDefault = clampInt(src.marketingMonthsAgoDefault, 1, 24, 6);
  const marketingPageSizeDefault = clampInt(src.marketingPageSizeDefault, 10, 500, DEFAULT_CLINIC_SETTINGS.marketingPageSizeDefault);

  return {
    timezone: CLINIC_TZ,
    gapMinSec,
    gapMaxSec,
    templateGapMinSec,
    templateGapMaxSec,
    marketingMonthsAgoDefault,
    marketingPageSizeDefault
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

async function clinicRecordSentMessage(authToken, payload) {
  const endpoint = `${CLINIC_API_BASE}/api:lY50ALPv/sent_message`;
  const src = payload && typeof payload === "object" ? payload : {};
  const body = {
    branch: cleanString(src.branch),
    name: cleanString(src.name),
    phone: cleanString(src.phone),
    sent_by: cleanString(src.sent_by),
    message: String(src.message || "")
  };

  if (!body.phone) throw new Error("phone is required");

  const controller = new AbortController();
  const timeoutMs = 4000;
  const t = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await clinicFetchJson(endpoint, {
      method: "POST",
      headers: clinicAuthHeaders(authToken, true),
      body: JSON.stringify(body),
      signal: controller.signal
    });
  } finally {
    clearTimeout(t);
  }
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
    nickname: cleanString(src.nickname),
    gender: cleanString(src.gender).toLowerCase(),
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
    nickname: cleanString(src.nickname),
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
    Appt_Start_Time: Number(src.Appt_Start_Time || 0) || 0,
    Dentist_Name: cleanString(src.Dentist_Name),
    Patient_Name: cleanString(src.Patient_Name),
    nickname: cleanString(src.nickname),
    gender: cleanString(src.gender).toLowerCase(),
    Patient_Phone_No: cleanString(src.Patient_Phone_No),
    ic_number: cleanString(src.ic_number || src.IC_Number)
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

async function clinicEditPatient(authToken, payload) {
  const src = payload && typeof payload === "object" ? payload : {};
  const icNumber = cleanString(src.ic_number || src.icNumber);
  const nickname = cleanString(src.nickname);
  const gender = cleanString(src.gender).toLowerCase();
  if (!icNumber) throw new Error("ic_number is required");
  if (!nickname && !gender) throw new Error("nickname or gender is required");

  const endpoint = `${CLINIC_API_BASE}/api:lY50ALPv/edit_patient`;
  return await clinicFetchJson(endpoint, {
    method: "PUT",
    headers: clinicAuthHeaders(authToken, true),
    body: JSON.stringify({
      ic_number: icNumber,
      nickname,
      gender
    })
  });
}

function clearPersistedContactsCache() {
  try {
    if (typeof store.delete === "function") store.delete("waContactsByProfile");
    else store.set("waContactsByProfile", {});
  } catch (e) {
    store.set("waContactsByProfile", {});
  }
  waContactsByProfileMem = {};
  contactLookupCacheByProfile.clear();
  for (const key of Object.keys(waLidPnMapByProfileMem)) delete waLidPnMapByProfileMem[key];
  startupPhotoBackfillDoneByProfile.clear();
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

      const jid =
        normalizePhoneNumberJid(contactRaw.phoneNumber || contactRaw.jid || "") ||
        `${msisdn}@s.whatsapp.net`;
      const lid = normalizeLidJid(contactRaw.lid || contactRaw.lidJid || "");
      byPhone[msisdn] = {
        msisdn,
        jid,
        lid: sanitizeContactName(lid || contactRaw.lid || contactRaw.lidJid || ""),
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
  contactLookupCacheByProfile.clear();
  rebuildLidPnMappingsFromContactsCache();
}

function clearContactsCacheForProfile(profileId) {
  if (!profileId) return;
  clearLidPnMappingsForProfile(profileId);
  if (!waContactsByProfileMem || typeof waContactsByProfileMem !== "object") return;
  if (!Object.prototype.hasOwnProperty.call(waContactsByProfileMem, profileId)) return;
  delete waContactsByProfileMem[profileId];
  persistContactsCache();
  contactLookupCacheByProfile.delete(profileId);
  startupPhotoBackfillDoneByProfile.delete(profileId);
}

function ensureLidPnMapStateForProfile(profileId) {
  const key = cleanString(profileId);
  if (!key) return null;
  const existing = waLidPnMapByProfileMem[key];
  if (existing && typeof existing === "object") {
    if (!existing.lidToPn || typeof existing.lidToPn !== "object") existing.lidToPn = {};
    if (!existing.pnToLid || typeof existing.pnToLid !== "object") existing.pnToLid = {};
    return existing;
  }
  const created = { lidToPn: {}, pnToLid: {} };
  waLidPnMapByProfileMem[key] = created;
  return created;
}

function clearLidPnMappingsForProfile(profileId) {
  const key = cleanString(profileId);
  if (!key) return;
  if (!Object.prototype.hasOwnProperty.call(waLidPnMapByProfileMem, key)) return;
  delete waLidPnMapByProfileMem[key];
}

function lidPnMappingStrength(lidJid, pnJid) {
  const lidUser = jidUserPart(lidJid);
  const pnUser = jidUserPart(pnJid);
  if (!lidUser || !pnUser) return 0;
  // Weak mapping: PN candidate is just the numeric LID user echoed as @s.whatsapp.net.
  if (lidUser === pnUser) return 1;
  return 2;
}

function rememberLidPnMappingForProfile(profileId, lidInput, pnInput) {
  const key = cleanString(profileId);
  if (!key) return 0;
  const lid = normalizeLidJid(lidInput);
  const pn = normalizePhoneNumberJid(pnInput);
  if (!lid || !pn) return 0;
  const state = ensureLidPnMapStateForProfile(key);
  if (!state) return 0;

  let changed = 0;
  const knownPn = normalizePhoneNumberJid(state.lidToPn[lid] || "");
  const knownStrength = knownPn ? lidPnMappingStrength(lid, knownPn) : 0;
  const incomingStrength = lidPnMappingStrength(lid, pn);

  if (
    !knownPn ||
    knownPn === pn ||
    incomingStrength > knownStrength
  ) {
    if (state.lidToPn[lid] !== pn) {
      state.lidToPn[lid] = pn;
      changed++;
    }
  }

  const knownLid = normalizeLidJid(state.pnToLid[pn] || "");
  const knownLidStrength = knownLid ? lidPnMappingStrength(knownLid, pn) : 0;
  const incomingLidStrength = lidPnMappingStrength(lid, pn);
  if (
    !knownLid ||
    knownLid === lid ||
    incomingLidStrength > knownLidStrength
  ) {
    if (state.pnToLid[pn] !== lid) {
      state.pnToLid[pn] = lid;
      changed++;
    }
  }

  return changed;
}

function getMappedPnJidForProfile(profileId, inputJid) {
  const key = cleanString(profileId);
  if (!key) return "";
  const jid = normalizeJidForContact(inputJid);
  if (!jid) return "";
  const state = waLidPnMapByProfileMem[key];
  if (!state || typeof state !== "object") return "";
  const mapped = normalizePhoneNumberJid(state?.lidToPn?.[jid] || "");
  return mapped;
}

function getMappedLidJidForProfile(profileId, inputJid) {
  const key = cleanString(profileId);
  if (!key) return "";
  const jid = normalizeJidForContact(inputJid);
  if (!jid) return "";
  const state = waLidPnMapByProfileMem[key];
  if (!state || typeof state !== "object") return "";
  const mapped = normalizeLidJid(state?.pnToLid?.[jid] || "");
  return mapped;
}

function getPreferredPnJidForLidFromContacts(profileId, lidInput) {
  const key = cleanString(profileId);
  const lid = normalizeLidJid(lidInput);
  if (!key || !lid) return "";

  const byPhone =
    waContactsByProfileMem &&
      waContactsByProfileMem[key] &&
      typeof waContactsByProfileMem[key] === "object"
      ? waContactsByProfileMem[key]
      : {};

  let strong = "";
  let weak = "";
  for (const contactRaw of Object.values(byPhone)) {
    const contact = contactRaw && typeof contactRaw === "object" ? contactRaw : null;
    if (!contact) continue;
    const contactLid = normalizeLidJid(contact.lid || contact.lidJid || "");
    if (!contactLid || contactLid !== lid) continue;

    const pn = normalizePhoneNumberJid(contact.jid || contact.phoneNumber || contact.pnJid || "");
    if (!pn) continue;
    const strength = lidPnMappingStrength(lid, pn);
    if (strength >= 2) {
      if (!strong) strong = pn;
      continue;
    }
    if (!weak) weak = pn;
  }

  return strong || weak || "";
}

function rebuildLidPnMappingsFromContactsCache() {
  for (const key of Object.keys(waLidPnMapByProfileMem)) delete waLidPnMapByProfileMem[key];
  const root = getContactsStoreObj();
  for (const [profileId, byPhoneRaw] of Object.entries(root || {})) {
    const byPhone = byPhoneRaw && typeof byPhoneRaw === "object" ? byPhoneRaw : {};
    for (const contactRaw of Object.values(byPhone)) {
      const contact = contactRaw && typeof contactRaw === "object" ? contactRaw : {};
      const pn = normalizePhoneNumberJid(contact.jid || "");
      const lid = normalizeLidJid(contact.lid || "");
      if (!pn || !lid) continue;
      rememberLidPnMappingForProfile(profileId, lid, pn);
    }
  }
}

function compactThumbnailDataUrl(value) {
  const src = cleanString(value || "");
  if (!src) return "";
  if (src.length <= WA_PERSISTED_THUMB_MAX_CHARS) return src;
  return "";
}

function normalizePersistedChatSummary(raw, fallbackJid) {
  const src = raw && typeof raw === "object" ? raw : {};
  const jid = normalizeChatJid(src.jid || fallbackJid || "");
  if (!jid) return null;
  const rawType = cleanString(src.lastMessageType || "");
  const rawPreview = String(src.lastMessagePreview || "");
  const isMetaOnlySummary = rawType === "messageContextInfo" || rawPreview === "[messageContextInfo]";
  return {
    jid,
    name: cleanString(src.name || ""),
    lastMessageTimestampMs: Math.max(0, Number(src.lastMessageTimestampMs || 0) || 0),
    lastMessagePreview: isMetaOnlySummary ? "" : rawPreview,
    lastMessageType: isMetaOnlySummary ? "" : rawType,
    lastMessageFromMe: !isMetaOnlySummary && src.lastMessageFromMe === true,
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
  const thumbnailDataUrl = compactThumbnailDataUrl(cleanString(src.thumbnailDataUrl || ""));
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
  if (isStatusBroadcastEnvelope(src)) return null;
  const key = normalizeMessageKey(src.key || {}, fallbackChatJid);
  const chatJid = normalizeChatJid(src.chatJid || key.remoteJid || fallbackChatJid || "");
  if (!chatJid || !key.id) return null;

  const timestampMs = Math.max(0, Number(src.timestampMs || 0) || 0);
  const type = cleanString(src.type || "unknown");
  const text = String(src.text || "");
  const preview = String(src.preview || text || "");
  if (type === "messageContextInfo" || preview === "[messageContextInfo]") return null;
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
  }, WA_CHAT_PERSIST_DEBOUNCE_MS);
}

function loadPersistedWaChatCache() {
  try {
    const raw = store.get(WA_CHAT_STORE_KEY);
    const normalized = normalizeWaChatCacheRoot(raw);
    for (const key of Object.keys(waChatByProfileMem)) delete waChatByProfileMem[key];
    const loadedProfileIds = [];
    for (const [profileId, profileState] of Object.entries(normalized)) {
      waChatByProfileMem[profileId] = profileState;
      enforceWaChatStorageLimitsForProfile(profileId);
      loadedProfileIds.push(profileId);
    }

    // Seed minimal contacts from cached chats first so alias reconciliation can
    // collapse legacy @lid and @s.whatsapp.net split threads on startup.
    for (const profileId of loadedProfileIds) {
      ensureContactsFromChatsForProfile(profileId);
    }

    if (loadedProfileIds.length > 0) {
      reconcileCanonicalChatAliasesForAllProfiles();
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

function normalizeTemplateMessageType(typeRaw) {
  const type = cleanString(typeRaw).toLowerCase();
  if (type === "image" || type === "video" || type === "audio" || type === "document") return type;
  return "text";
}

function normalizeTemplateAttachmentRecord(raw, forcedType = "") {
  const src = raw && typeof raw === "object" ? raw : {};
  const filePath = cleanString(src.path || src.filePath || "");
  const fileName = cleanString(src.fileName || src.name || path.basename(filePath || ""));
  const mimeType = cleanString(src.mimeType || src.type || (filePath ? getMimeTypeForPath(filePath) : ""));
  const inferredKind = normalizeTemplateMessageType(src.kind || attachmentKindFromMimeOrPath(mimeType, filePath));
  const forcedKind = normalizeTemplateMessageType(forcedType);
  const kind = forcedKind === "text" ? inferredKind : forcedKind;
  const size = Math.max(0, Number(src.size || 0) || 0);
  const assetId = cleanString(src.assetId || src.mediaAssetId || "");
  if (!filePath && !fileName && !assetId) return null;
  return {
    path: filePath,
    fileName: fileName || "Attachment",
    mimeType,
    kind,
    size,
    ...(assetId ? { assetId } : {})
  };
}

function normalizeTemplateMessageRecord(raw, idx) {
  const src = raw && typeof raw === "object" ? raw : {};
  const type = normalizeTemplateMessageType(src.type);
  const text = String(src.text ?? src.body ?? src.caption ?? "");
  const attachment = type === "text" ? null : normalizeTemplateAttachmentRecord(src.attachment || src.media, type);
  const fallbackId = `tm_${idx + 1}`;
  return {
    id: cleanString(src.id || fallbackId),
    type,
    text,
    attachment
  };
}

function normalizeTemplateMessagesList(input, legacyBody = "") {
  const rows = Array.isArray(input) ? input.map((x, idx) => normalizeTemplateMessageRecord(x, idx)).filter(Boolean) : [];
  const out =
    rows.length > 0
      ? rows
      : [
        {
          id: "tm_1",
          type: "text",
          text: String(legacyBody || ""),
          attachment: null
        }
      ];
  if (out.length === 0) {
    out.push({
      id: "tm_1",
      type: "text",
      text: "",
      attachment: null
    });
  }

  const seenIds = new Set();
  for (let i = 0; i < out.length; i++) {
    let nextId = cleanString(out[i].id || `tm_${i + 1}`);
    if (!nextId || seenIds.has(nextId)) nextId = `tm_${i + 1}`;
    while (seenIds.has(nextId)) nextId = `${nextId}_${i + 1}`;
    out[i].id = nextId;
    seenIds.add(nextId);
  }

  return out;
}

function extractTemplateVarsFromMessages(messages) {
  const vars = new Set();
  for (const msg of Array.isArray(messages) ? messages : []) {
    for (const key of extractTemplateVars(msg?.text || "")) vars.add(key);
  }
  return Array.from(vars);
}

function getTemplatePrimaryBody(messages, fallback = "") {
  const rows = Array.isArray(messages) ? messages : [];
  for (const row of rows) {
    if (normalizeTemplateMessageType(row?.type) !== "text") continue;
    const text = String(row?.text || "");
    if (text.trim()) return text;
  }
  return String(fallback || rows[0]?.text || "");
}

function normalizeTemplateRecord(raw, idx) {
  const t = raw && typeof raw === "object" ? raw : {};
  const messages = normalizeTemplateMessagesList(t.messages, t.body || "");
  const body = getTemplatePrimaryBody(messages, t.body || "");

  const vars = new Set();
  for (const v of Array.isArray(t.variables) ? t.variables : []) {
    if (isValidTemplateVarName(v)) vars.add(String(v));
  }
  for (const v of extractTemplateVarsFromMessages(messages)) vars.add(v);
  for (const v of extractTemplateVars(body)) vars.add(v);

  const sendPolicy = t.sendPolicy === "multiple" ? "multiple" : "once";
  const fallbackId = "t_" + String(idx + 1);

  return {
    id: String(t.id || fallbackId),
    name: String(t.name || "Untitled"),
    body,
    messages,
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
  ensureDir(templateMediaDir);
  if (!fs.existsSync(templatesFile)) {
    const seededTemplatesPath = path.join(bundledDataDir, "templates.json");
    if (fs.existsSync(seededTemplatesPath)) {
      try {
        const seededRaw = fs.readFileSync(seededTemplatesPath, "utf-8");
        const seededTemplates = normalizeTemplatesList(JSON.parse(seededRaw));
        fs.writeFileSync(templatesFile, JSON.stringify(seededTemplates, null, 2), "utf-8");
        return;
      } catch {
        // Fallback to an empty template list if bundled seed data is invalid.
      }
    }

    fs.writeFileSync(templatesFile, JSON.stringify([], null, 2), "utf-8");
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

function normalizeTemplateAssetRecord(raw, idx) {
  const src = raw && typeof raw === "object" ? raw : {};
  const assetId = cleanString(src.assetId || src.id || `asset_${idx + 1}`);
  const mimeType = cleanString(src.mimeType || src.type || "");
  const fileName = cleanString(src.fileName || src.name || "");
  const dataBase64 = cleanString(src.dataBase64 || src.base64 || src.data || "");
  const kindRaw = cleanString(src.kind || attachmentKindFromMimeOrPath(mimeType, fileName));
  const size = Math.max(0, Number(src.size || 0) || 0);
  if (!assetId || !dataBase64) return null;
  return {
    assetId,
    fileName,
    mimeType,
    kind: kindRaw,
    size,
    dataBase64
  };
}

function buildTemplateAttachmentExportPayload(attachmentRaw, assetByPath, assetsOut) {
  const attachment = normalizeTemplateAttachmentRecord(attachmentRaw);
  if (!attachment) return null;

  const filePath = cleanString(attachment.path);
  const hasLocalFile = filePath && fs.existsSync(filePath) && fs.statSync(filePath).isFile();
  if (!hasLocalFile) {
    return {
      path: filePath,
      fileName: attachment.fileName || path.basename(filePath || "Attachment"),
      mimeType: attachment.mimeType || (filePath ? getMimeTypeForPath(filePath) : ""),
      kind: attachment.kind || attachmentKindFromMimeOrPath(attachment.mimeType, filePath),
      size: Number(attachment.size || 0) || 0,
      ...(attachment.assetId ? { assetId: attachment.assetId } : {})
    };
  }

  const pathKey = filePath.toLowerCase();
  let assetMeta = assetByPath.get(pathKey) || null;
  if (!assetMeta) {
    const fileBuffer = fs.readFileSync(filePath);
    const stat = fs.statSync(filePath);
    const mimeType = attachment.mimeType || getMimeTypeForPath(filePath);
    const kind = attachment.kind || attachmentKindFromMimeOrPath(mimeType, filePath);
    const hash = crypto.createHash("sha1").update(fileBuffer).digest("hex");
    const ext = path.extname(filePath || "").toLowerCase();
    const assetId = cleanString(attachment.assetId || `${hash}${ext ? "_" + ext.slice(1) : ""}`) || hash;
    assetMeta = {
      assetId,
      fileName: attachment.fileName || path.basename(filePath),
      mimeType,
      kind,
      size: Number(stat.size || 0) || 0
    };
    assetsOut.push({
      ...assetMeta,
      dataBase64: fileBuffer.toString("base64")
    });
    assetByPath.set(pathKey, assetMeta);
  }

  return {
    path: filePath,
    fileName: assetMeta.fileName,
    mimeType: assetMeta.mimeType,
    kind: assetMeta.kind,
    size: assetMeta.size,
    assetId: assetMeta.assetId
  };
}

function buildMarketingTemplateExportData(templatesInput) {
  const templates = normalizeTemplatesList(templatesInput);
  const marketingTemplateAssets = [];
  const assetsByPath = new Map();

  const marketingTemplates = templates.map((template, idx) => {
    const messages = normalizeTemplateMessagesList(template.messages, template.body || "").map((msg, msgIdx) => {
      const type = normalizeTemplateMessageType(msg.type);
      const attachment =
        type === "text"
          ? null
          : buildTemplateAttachmentExportPayload(msg.attachment, assetsByPath, marketingTemplateAssets);
      return {
        id: cleanString(msg.id || `tm_${msgIdx + 1}`),
        type,
        text: String(msg.text || ""),
        ...(attachment ? { attachment } : {})
      };
    });

    return normalizeTemplateRecord(
      {
        ...template,
        messages
      },
      idx
    );
  });

  return { marketingTemplates, marketingTemplateAssets };
}

function restoreTemplateAssetsFromBundle(assetsInput) {
  const rows = Array.isArray(assetsInput) ? assetsInput : [];
  const assetMap = new Map();
  if (rows.length === 0) return assetMap;
  ensureDataFiles();
  ensureDir(templateMediaDir);

  rows.forEach((raw, idx) => {
    const asset = normalizeTemplateAssetRecord(raw, idx);
    if (!asset) return;
    try {
      const buffer = Buffer.from(asset.dataBase64, "base64");
      if (!buffer || buffer.length === 0) return;
      const inferredExt = path.extname(asset.fileName || "") || extensionFromMime(asset.mimeType) || "";
      const fileNameBase = cleanString(asset.fileName) || `template_asset_${idx + 1}${inferredExt}`;
      const safeName = sanitizePathSegment(fileNameBase, `template_asset_${idx + 1}${inferredExt}`);
      const outputName = `${sanitizePathSegment(asset.assetId, "asset").slice(0, 20)}_${safeName}`;
      const outputPath = path.join(templateMediaDir, outputName);
      fs.writeFileSync(outputPath, buffer);
      const mimeType = asset.mimeType || getMimeTypeForPath(outputPath);
      const inferredKind = attachmentKindFromMimeOrPath(mimeType, outputPath);
      assetMap.set(asset.assetId, {
        path: outputPath,
        fileName: cleanString(asset.fileName) || path.basename(outputPath),
        mimeType,
        kind: inferredKind,
        size: Number(buffer.length || 0) || Number(asset.size || 0) || 0,
        assetId: asset.assetId
      });
    } catch (e) {
      log.warn({ err: e, assetId: asset.assetId }, "Failed to restore marketing template asset");
    }
  });

  return assetMap;
}

function rehydrateTemplateAttachmentFromBundle(attachmentRaw, type, assetMap) {
  const attachment = normalizeTemplateAttachmentRecord(attachmentRaw, type);
  const assetId = cleanString(attachmentRaw?.assetId || attachment?.assetId || "");
  const mappedAsset = assetId ? assetMap.get(assetId) || null : null;
  if ((!attachment || !attachment.path) && mappedAsset) {
    return {
      ...mappedAsset,
      kind: normalizeTemplateMessageType(type),
      assetId
    };
  }
  if (!attachment) return null;
  return {
    ...attachment,
    kind: normalizeTemplateMessageType(type),
    ...(assetId ? { assetId } : {})
  };
}

function rehydrateMarketingTemplatesWithAssets(marketingTemplatesInput, assetsInput) {
  const templates = Array.isArray(marketingTemplatesInput) ? marketingTemplatesInput : [];
  const assetMap = restoreTemplateAssetsFromBundle(assetsInput);
  return templates.map((template, idx) => {
    const normalized = normalizeTemplateRecord(template, idx);
    const messages = normalizeTemplateMessagesList(normalized.messages, normalized.body || "").map((msg, msgIdx) => {
      const type = normalizeTemplateMessageType(msg.type);
      const attachment =
        type === "text" ? null : rehydrateTemplateAttachmentFromBundle(msg.attachment || {}, type, assetMap);
      return {
        id: cleanString(msg.id || `tm_${msgIdx + 1}`),
        type,
        text: String(msg.text || ""),
        ...(attachment ? { attachment } : {})
      };
    });

    return normalizeTemplateRecord(
      {
        ...normalized,
        messages
      },
      idx
    );
  });
}

function getTemplateExportBundle() {
  const marketingData = buildMarketingTemplateExportData(readTemplates());
  return {
    exported_at: nowIsoShort(),
    timezone: CLINIC_TZ,
    marketingTemplates: marketingData.marketingTemplates,
    marketingTemplateAssets: marketingData.marketingTemplateAssets,
    appointmentTemplates: getAppointmentTemplates()
  };
}

function getSingleMarketingTemplateExportBundle(templateId) {
  const targetId = cleanString(templateId);
  if (!targetId) throw new Error("Missing templateId");
  const currentTemplates = readTemplates();
  const selected = currentTemplates.find((t) => cleanString(t?.id) === targetId) || null;
  if (!selected) throw new Error("Marketing template not found");

  const marketingData = buildMarketingTemplateExportData([selected]);
  return {
    exported_at: nowIsoShort(),
    timezone: CLINIC_TZ,
    scope: "single_marketing_template",
    marketingTemplate: marketingData.marketingTemplates[0] || null,
    marketingTemplateAssets: marketingData.marketingTemplateAssets
  };
}

function extractSingleMarketingTemplateFromImport(raw) {
  const src = raw && typeof raw === "object" ? raw : {};
  const one = src.marketingTemplate && typeof src.marketingTemplate === "object" ? src.marketingTemplate : null;
  if (one) {
    const assets = Array.isArray(src.marketingTemplateAssets)
      ? src.marketingTemplateAssets
      : Array.isArray(src.templateAssets)
        ? src.templateAssets
        : [];
    return { template: one, assets };
  }

  const list = Array.isArray(src.marketingTemplates)
    ? src.marketingTemplates
    : Array.isArray(src.templates)
      ? src.templates
      : [];
  if (list.length === 0) return { template: null, assets: [] };
  if (list.length > 1) {
    throw new Error("Import file contains multiple marketing templates. Please use a single-template export file.");
  }
  const assets = Array.isArray(src.marketingTemplateAssets)
    ? src.marketingTemplateAssets
    : Array.isArray(src.templateAssets)
      ? src.templateAssets
      : [];
  return { template: list[0], assets };
}

function importSingleMarketingTemplateBundle(raw) {
  const extracted = extractSingleMarketingTemplateFromImport(raw);
  if (!extracted.template) throw new Error("No marketing template found in import file");

  const normalizedList = rehydrateMarketingTemplatesWithAssets([extracted.template], extracted.assets);
  if (!Array.isArray(normalizedList) || normalizedList.length === 0) {
    throw new Error("No valid marketing template found in import file");
  }
  const imported = normalizedList[0];

  const current = readTemplates();
  const idx = current.findIndex((t) => cleanString(t?.id) === cleanString(imported.id));
  let replaced = false;
  if (idx >= 0) {
    current[idx] = imported;
    replaced = true;
  } else {
    current.push(imported);
  }
  saveTemplates(current);

  return {
    marketingCount: current.length,
    templateId: imported.id,
    templateName: imported.name,
    replaced
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
  const marketingTemplateAssets = Array.isArray(src.marketingTemplateAssets)
    ? src.marketingTemplateAssets
    : Array.isArray(src.templateAssets)
      ? src.templateAssets
      : [];
  const appointmentTemplates =
    src.appointmentTemplates && typeof src.appointmentTemplates === "object" ? src.appointmentTemplates : null;

  let marketingCount = readTemplates().length;
  let appointmentUpdated = false;

  if (marketingTemplates) {
    const normalizedMarketing = rehydrateMarketingTemplatesWithAssets(marketingTemplates, marketingTemplateAssets);
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

function jidUserPart(jid) {
  const normalized = normalizeJidForContact(jid);
  if (!normalized) return "";
  const at = normalized.indexOf("@");
  return at > 0 ? normalized.slice(0, at) : "";
}

function isPnJid(jid) {
  const normalized = normalizeJidForContact(jid).toLowerCase();
  if (!normalized) return false;
  return (
    normalized.endsWith("@s.whatsapp.net") ||
    normalized.endsWith("@hosted") ||
    normalized.endsWith("@c.us")
  );
}

function isLidJid(jid) {
  const normalized = normalizeJidForContact(jid).toLowerCase();
  if (!normalized) return false;
  return normalized.endsWith("@lid") || normalized.endsWith("@hosted.lid");
}

function normalizePhoneNumberJid(input) {
  const normalized = normalizeJidForContact(input);
  if (normalized && normalized.includes("@")) {
    return isPnJid(normalized) ? normalized : "";
  }
  const msisdn = normalizeMsisdnSafe(input);
  if (!msisdn) return "";
  return `${msisdn}@s.whatsapp.net`;
}

function normalizeLidJid(input) {
  const normalized = normalizeJidForContact(input);
  if (normalized && normalized.includes("@")) {
    return isLidJid(normalized) ? normalized : "";
  }
  const user = cleanString(input).replace(/\s+/g, "");
  if (!/^\d{4,}$/.test(user)) return "";
  return `${user}@lid`;
}

function msisdnFromUserJid(jid) {
  const j = normalizeJidForContact(jid);
  if (!isPnJid(j)) return "";
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

function normalizeMsisdnSafe(input) {
  try {
    return normalizeMsisdn(input);
  } catch (e) {
    return "";
  }
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

function beginBatchJob(kind, totalCount) {
  if (activeBatchJob && activeBatchJob.active === true) {
    throw new Error("Another sending process is already running");
  }
  activeBatchJob = {
    id: `batch_${Date.now()}_${Math.random().toString(16).slice(2, 8)}`,
    kind: cleanString(kind) || "batch",
    totalCount: Math.max(0, Number(totalCount || 0) || 0),
    active: true,
    cancelRequested: false,
    startedAtIso: nowIsoShort()
  };
  return activeBatchJob;
}

function finishBatchJob(job) {
  if (!job || !activeBatchJob) return;
  if (activeBatchJob.id !== job.id) return;
  activeBatchJob = null;
}

function isBatchJobCancelRequested(job) {
  if (!job || !job.id) return false;
  if (!activeBatchJob || activeBatchJob.id !== job.id) return true;
  return activeBatchJob.cancelRequested === true;
}

async function waitDelayOrBatchCancel(job, ms) {
  let remaining = Math.max(0, Number(ms || 0) || 0);
  while (remaining > 0) {
    if (isBatchJobCancelRequested(job)) return false;
    const slice = Math.min(250, remaining);
    await new Promise((r) => setTimeout(r, slice));
    remaining -= slice;
  }
  return !isBatchJobCancelRequested(job);
}

function requestStopActiveBatchJob() {
  const job = activeBatchJob;
  if (!job || job.active !== true) {
    return { ok: false, active: false, message: "No active sending process" };
  }
  job.cancelRequested = true;
  return { ok: true, active: true, batchId: job.id, kind: job.kind };
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
  profileSelfIdentityCache.clear();
  return normalized;
}

function loadProfiles() {
  ensureDir(profilesRootDir);
  const profiles = store.get("profiles");
  if (Array.isArray(profiles) && profiles.length > 0) {
    const normalized = saveProfiles(profiles);
    let migrated = false;
    const renamed = normalized.map((p) => {
      if (p?.id !== "p_default") return p;
      if (p?.customName === true) return p;
      // Rename any non-user-customized default profile to the current brand name
      migrated = true;
      return {
        ...p,
        name: DEFAULT_WA_PROFILE_NAME,
        customName: false
      };
    });
    if (migrated) return saveProfiles(renamed);
    return normalized;
  }
  const defaultProfile = normalizeProfileRecord({ id: "p_default", name: DEFAULT_WA_PROFILE_NAME, customName: false });
  store.set("profiles", [defaultProfile]);
  store.set("activeProfileId", defaultProfile.id);
  profileSelfIdentityCache.clear();
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
    // waitForConnectSetupIdle removed (WhatsMeow: no idle wait needed)
    await disconnectActiveProfileSocket();
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
      name: DEFAULT_WA_PROFILE_NAME,
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
    // waitForConnectSetupIdle removed (WhatsMeow: no idle wait needed)
    await disconnectActiveProfileSocket();
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
      text: "Session terminated. Connect required.",
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

function getSentAt(templateId, msisdn) {
  const logObj = getSentLog();
  const v = logObj[sentKey(templateId, msisdn)];
  return typeof v === "string" ? v : "";
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

function isLikelyFallbackIdentityLabel(value) {
  const s = sanitizeContactName(value).toLowerCase();
  if (!s) return false;
  if (/^\d{6,}$/.test(s)) return true;
  if (/^\d+@(lid|hosted\.lid)$/.test(s)) return true;
  if (/^\d+@(s\.whatsapp\.net|hosted)$/.test(s)) return true;
  if (/^\d+@c\.us$/.test(s)) return true;
  if (s === "unknown" || s === "unknown chat" || s === "someone") return true;
  const compact = s.replace(/[\s()+-]/g, "");
  return /^\d{6,}$/.test(compact);
}

function choosePreferredIdentityLabel(existingValue, incomingValue) {
  const existing = sanitizeContactName(existingValue);
  const incoming = sanitizeContactName(incomingValue);
  if (!incoming) return existing;
  if (!existing) return incoming;
  const existingFallback = isLikelyFallbackIdentityLabel(existing);
  const incomingFallback = isLikelyFallbackIdentityLabel(incoming);
  if (existingFallback && !incomingFallback) return incoming;
  if (!existingFallback && incomingFallback) return existing;
  if (!existingFallback && !incomingFallback && incoming.length > existing.length + 2) return incoming;
  return existing;
}

function getProfileSelfIdentity(profileId) {
  const key = cleanString(profileId);
  if (!key) return { msisdn: "", name: "", nameLower: "" };
  const cached = profileSelfIdentityCache.get(key);
  if (cached && typeof cached === "object") return cached;
  const profile = loadProfiles().find((p) => p && p.id === key);
  const msisdn = normalizeMsisdnSafe(profile?.waMsisdn || msisdnFromUserJid(profile?.waJid || ""));
  const name = sanitizeContactName(profile?.waName || "");
  const identity = {
    msisdn,
    name,
    nameLower: name.toLowerCase()
  };
  profileSelfIdentityCache.set(key, identity);
  return identity;
}

function sanitizeIdentityLabelForProfile(profileId, candidateLabel, candidateMsisdn) {
  const label = sanitizeContactName(candidateLabel);
  if (!label) return "";

  const self = getProfileSelfIdentity(profileId);
  if (!self.nameLower) return label;

  const candidateLower = label.toLowerCase();
  if (candidateLower !== self.nameLower) return label;

  const msisdn = normalizeMsisdnSafe(candidateMsisdn);
  if (self.msisdn && msisdn && self.msisdn === msisdn) return label;
  return "";
}

function getContactDisplayName(c, profileId = "") {
  const msisdn = normalizeMsisdnSafe(c?.msisdn || msisdnFromContactAddress(c?.jid || c?.lid || ""));
  let best = "";
  best = choosePreferredIdentityLabel(best, sanitizeIdentityLabelForProfile(profileId, c?.name, msisdn));
  best = choosePreferredIdentityLabel(best, sanitizeIdentityLabelForProfile(profileId, c?.notify, msisdn));
  best = choosePreferredIdentityLabel(best, sanitizeIdentityLabelForProfile(profileId, c?.verifiedName, msisdn));
  return sanitizeIdentityLabelForProfile(profileId, best, msisdn);
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
  if (isLidJid(norm)) {
    const user = jidUserPart(norm);
    if (/^\d{6,}$/.test(user)) return user;
  }
  if (/^\d{6,}$/.test(norm)) return norm;
  return "";
}

function findContactByJidForProfile(profileId, jidOrAddress) {
  if (!profileId) return null;
  const target = normalizeJidForContact(jidOrAddress);
  if (!target) return null;

  const byPhone =
    waContactsByProfileMem &&
      waContactsByProfileMem[profileId] &&
      typeof waContactsByProfileMem[profileId] === "object"
      ? waContactsByProfileMem[profileId]
      : {};

  const mappedPnJid = getMappedPnJidForProfile(profileId, target);
  const mappedMsisdn = msisdnFromContactAddress(mappedPnJid);
  if (mappedMsisdn && byPhone[mappedMsisdn] && typeof byPhone[mappedMsisdn] === "object") {
    return byPhone[mappedMsisdn];
  }

  const msisdn = msisdnFromContactAddress(target);
  if (msisdn && byPhone[msisdn] && typeof byPhone[msisdn] === "object") {
    return byPhone[msisdn];
  }

  const targetLower = String(target).toLowerCase();
  let cache = contactLookupCacheByProfile.get(profileId);
  if (!cache) {
    cache = new Map();
    contactLookupCacheByProfile.set(profileId, cache);
  }
  if (cache.has(targetLower)) {
    const cachedMsisdn = cleanString(cache.get(targetLower));
    if (!cachedMsisdn) return null;
    const cached = byPhone[cachedMsisdn];
    return cached && typeof cached === "object" ? cached : null;
  }

  for (const contactRaw of Object.values(byPhone)) {
    const contact = contactRaw && typeof contactRaw === "object" ? contactRaw : null;
    if (!contact) continue;
    const jid = normalizeJidForContact(contact.jid || "");
    if (jid && String(jid).toLowerCase() === targetLower) {
      cache.set(targetLower, cleanString(contact.msisdn || ""));
      return contact;
    }
    const lid = normalizeJidForContact(contact.lid || "");
    if (lid && String(lid).toLowerCase() === targetLower) {
      cache.set(targetLower, cleanString(contact.msisdn || ""));
      return contact;
    }
  }

  cache.set(targetLower, "");
  return null;
}

function upsertContactsForProfile(profileId, contacts) {
  if (!profileId || !Array.isArray(contacts) || contacts.length === 0) return 0;

  const root = getContactsStoreObj();
  const byPhone = root[profileId] && typeof root[profileId] === "object" ? { ...root[profileId] } : {};
  let changed = 0;
  let mappingChanged = 0;

  for (const c of contacts) {
    const contactPhoneJid = normalizePhoneNumberJid(c?.phoneNumber || c?.pnJid || c?.pn || "");
    const contactLidJid = normalizeLidJid(c?.lid || c?.lidJid || "");
    const primaryJidSource = contactPhoneJid || c?.pnJid || c?.phoneNumber || c?.jid || c?.id || "";
    const secondaryJidSource = c?.jid || c?.id || contactLidJid || "";
    const jid = contactPhoneJid || normalizeJidForContact(primaryJidSource) || normalizeJidForContact(secondaryJidSource);
    const msisdn =
      normalizeMsisdnSafe(c?.phoneNumber || c?.msisdn || "") ||
      msisdnFromContactAddress(contactPhoneJid || primaryJidSource) ||
      msisdnFromContactAddress(secondaryJidSource);
    if (!msisdn) continue;

    const secondaryJid = normalizeJidForContact(secondaryJidSource);
    const secondaryLid = isLidJid(secondaryJid) ? secondaryJid : "";

    const existing = byPhone[msisdn] && typeof byPhone[msisdn] === "object" ? byPhone[msisdn] : {};
    const incomingNameRaw = firstNonEmptyString([
      c?.name,
      c?.fullName,
      c?.firstName,
      c?.shortName,
      c?.short,
      c?.username
    ]);
    const incomingName = sanitizeIdentityLabelForProfile(profileId, incomingNameRaw, msisdn);
    const nameCandidate = choosePreferredIdentityLabel(existing?.name, incomingName);
    const incomingNotifyRaw = firstNonEmptyString([c?.notify, c?.pushName, c?.pushname]);
    const incomingNotify = sanitizeIdentityLabelForProfile(profileId, incomingNotifyRaw, msisdn);
    const notify = choosePreferredIdentityLabel(existing?.notify, incomingNotify);
    const incomingVerifiedNameRaw = firstNonEmptyString([c?.verifiedName, c?.vname]);
    const incomingVerifiedName = sanitizeIdentityLabelForProfile(profileId, incomingVerifiedNameRaw, msisdn);
    const verifiedName = choosePreferredIdentityLabel(existing?.verifiedName, incomingVerifiedName);
    const lid = firstNonEmptyString([contactLidJid, c?.lid, c?.lidJid, secondaryLid, existing?.lid]);
    const imgUrl =
      c?.imgUrl === null || c?.imgUrl === undefined
        ? existing?.imgUrl ?? null
        : String(c.imgUrl || "").trim() || null;
    const status = sanitizeContactName(c?.status) || sanitizeContactName(existing?.status);
    const normalizedIncomingJid = normalizeJidForContact(jid || "");
    const preferredPnJid = normalizePhoneNumberJid(contactPhoneJid || normalizedIncomingJid || existing?.jid || "");
    const preferredLidJid = normalizeLidJid(contactLidJid || lid || normalizedIncomingJid || existing?.lid || "");
    const fallbackPhoneJid = /^\d{8,15}$/.test(msisdn) ? `${msisdn}@s.whatsapp.net` : "";
    const stableJid =
      preferredPnJid ||
      normalizedIncomingJid ||
      normalizeJidForContact(existing?.jid || "") ||
      preferredLidJid ||
      fallbackPhoneJid;
    if (!stableJid) continue;

    const next = {
      msisdn,
      jid: stableJid,
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

    mappingChanged += rememberLidPnMappingForProfile(profileId, next.lid, next.jid);
    mappingChanged += rememberLidPnMappingForProfile(profileId, contactLidJid || secondaryLid, contactPhoneJid || next.jid);
  }

  if (changed > 0) {
    root[profileId] = byPhone;
    persistContactsCache();
    contactLookupCacheByProfile.delete(profileId);
    promoteChatNamesFromContactsForProfile(profileId);
  }
  if (changed > 0 || mappingChanged > 0) {
    scheduleChatAliasReconcileForProfile(profileId);
  }

  return changed;
}

function upsertChatsAsContactsForProfile(profileId, chats) {
  if (!profileId || !Array.isArray(chats) || chats.length === 0) return 0;

  const contacts = [];
  for (const chat of chats) {
    const chatPhoneJid = normalizePhoneNumberJid(chat?.phoneNumber || chat?.pnJid || "");
    const chatLidJid = normalizeLidJid(chat?.lid || chat?.lidJid || "");
    const primaryJidSource = chatPhoneJid || chat?.pnJid || chat?.phoneNumber || chat?.id || chat?.jid || "";
    const secondaryJidSource = chat?.id || chat?.jid || chatLidJid || "";
    const jid = chatPhoneJid || normalizeJidForContact(primaryJidSource) || normalizeJidForContact(secondaryJidSource);
    const msisdn =
      normalizeMsisdnSafe(chat?.phoneNumber || "") ||
      msisdnFromContactAddress(chatPhoneJid || primaryJidSource) ||
      msisdnFromContactAddress(secondaryJidSource);
    if (!msisdn) continue;

    const secondaryJid = normalizeJidForContact(secondaryJidSource);
    const name = firstNonEmptyString([chat?.name, chat?.notify, chat?.pushName]);
    const notify = firstNonEmptyString([chat?.notify, chat?.pushName, name]);
    const lid = firstNonEmptyString([chatLidJid, chat?.lid, chat?.lidJid, isLidJid(secondaryJid) ? secondaryJid : ""]);
    contacts.push({
      id: jid || `${msisdn}@s.whatsapp.net`,
      jid: jid || `${msisdn}@s.whatsapp.net`,
      lid,
      phoneNumber: chatPhoneJid || "",
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
    if (isStatusBroadcastEnvelope(msg)) continue;
    const key = msg?.key && typeof msg.key === "object" ? msg.key : {};
    const fromMe = key.fromMe === true || msg?.fromMe === true;
    if (fromMe) continue;
    const primaryJidSource =
      key.participantAlt ||
      key.remoteJidAlt ||
      key.participantPn ||
      key.remoteJidPn ||
      key.participant ||
      key.remoteJid ||
      "";
    const secondaryJidSource = key.participant || key.remoteJid || "";
    const jid = normalizeJidForContact(primaryJidSource) || normalizeJidForContact(secondaryJidSource);
    const altPhoneJid = normalizePhoneNumberJid(key.participantAlt || key.remoteJidAlt || key.participantPn || key.remoteJidPn || "");
    const msisdn = msisdnFromContactAddress(primaryJidSource) || msisdnFromContactAddress(secondaryJidSource);
    if (!msisdn) continue;

    const secondaryJid = normalizeJidForContact(secondaryJidSource);
    const lid = isLidJid(secondaryJid) ? secondaryJid : isLidJid(jid) ? jid : "";
    const notify = firstNonEmptyString([msg?.pushName, msg?.notifyName, msg?.notify, msg?.participantName]);
    const verifiedName = firstNonEmptyString([msg?.verifiedBizName, msg?.verifiedName]);
    if (!notify && !verifiedName) continue;

    contacts.push({
      id: altPhoneJid || jid || `${msisdn}@s.whatsapp.net`,
      jid: altPhoneJid || jid || `${msisdn}@s.whatsapp.net`,
      lid,
      phoneNumber: altPhoneJid || "",
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
    const an = getContactDisplayName(a, profileId).toLowerCase();
    const bn = getContactDisplayName(b, profileId).toLowerCase();
    if (an && bn) return an.localeCompare(bn);
    if (an) return -1;
    if (bn) return 1;
    return String(a?.msisdn || "").localeCompare(String(b?.msisdn || ""));
  });

  return rows;
}

function contactHasAnyName(contact, profileId = "") {
  return !!getContactDisplayName(contact, profileId);
}

function rememberRecipientNameForProfile(profileId, recipient, rawName) {
  const key = cleanString(profileId);
  if (!key) return 0;

  const name = sanitizeContactName(rawName);
  if (!name || isLikelyFallbackIdentityLabel(name)) return 0;

  const normalizedRecipient = cleanString(recipient);
  const recipientJid = normalizeJidForContact(normalizedRecipient);
  const msisdn = msisdnFromContactAddress(recipientJid || normalizedRecipient) || normalizeMsisdnSafe(normalizedRecipient);
  if (!msisdn) return 0;

  const jid =
    (recipientJid && recipientJid.includes("@") ? recipientJid : "") ||
    `${msisdn}@s.whatsapp.net`;

  return upsertContactsForProfile(key, [
    {
      id: jid,
      jid,
      name,
      notify: name
    }
  ]);
}

function promoteChatNamesFromContactsForProfile(profileId) {
  if (!profileId) return 0;
  const state = ensureWaChatStateForProfile(profileId);
  let changed = 0;

  for (const [chatJid, chatRaw] of Object.entries(state.chatsByJid || {})) {
    const chat = chatRaw && typeof chatRaw === "object" ? chatRaw : {};
    const contact = getContactByChatJid(profileId, chatJid);
    const contactName = getContactDisplayName(contact, profileId);
    const nextName = choosePreferredIdentityLabel(chat.name, contactName);
    if (!nextName || nextName === String(chat.name || "")) continue;
    state.chatsByJid[chatJid] = {
      ...chat,
      name: nextName,
      updatedAt: nowIsoShort()
    };
    changed++;
  }

  if (changed > 0) {
    schedulePersistWaChatCache();
  }
  return changed;
}

function isKnownAppStatePatchConflictError(err) {
  const msg = String(err?.message || err || "");
  return /tried remove, but no previous op/i.test(msg);
}

async function resyncAppStateForProfile(profileId, collections, isInitialSync, source) {
  // Go backend handles app state resync (archive/pin/mute) automatically.
  return { ok: true, synced: true };
}

async function syncContactNamesForProfile(profileId, options) {
  // Go backend handles name sync automatically.
  return { ok: true, synced: true };
}

function resolveContactPhotoFetchJid(profileId, contact) {
  if (!contact || typeof contact !== "object") return "";

  let pnCandidate = "";
  let lidCandidate = "";
  const sourceCandidates = [contact.jid, contact.phoneNumber, contact.pnJid, contact.lid, contact.lidJid];
  for (const candidate of sourceCandidates) {
    const jid = normalizeJidForContact(candidate || "");
    if (!jid) continue;
    if (jid.endsWith("@g.us") || jid.endsWith("@broadcast")) continue;
    if (!pnCandidate && isPnJid(jid)) pnCandidate = jid;
    if (!lidCandidate && isLidJid(jid)) lidCandidate = jid;
  }
  if (lidCandidate) {
    const mappedPn = getMappedPnJidForProfile(profileId, lidCandidate);
    if (mappedPn) return mappedPn;
    return lidCandidate;
  }
  if (pnCandidate) return pnCandidate;

  const msisdn = normalizeMsisdnSafe(contact.msisdn || "");
  return /^\d{8,15}$/.test(msisdn) ? `${msisdn}@s.whatsapp.net` : "";
}

function shouldRefreshContactPhoto(profileId, contact, minMinutesBetweenChecks, forceMissing) {
  if (!contact || typeof contact !== "object") return false;
  const fetchJid = resolveContactPhotoFetchJid(profileId, contact);
  if (!fetchJid) return false;
  if (forceMissing === true) return true;
  const mins = Math.max(1, Number(minMinutesBetweenChecks || 180));
  const last = Date.parse(String(contact.photoCheckedAt || ""));
  if (!Number.isFinite(last)) return true;
  return Date.now() - last >= mins * 60 * 1000;
}

function extractErrorStatusCode(err) {
  const direct = Number(err?.statusCode || err?.status || err?.code || 0);
  if (Number.isFinite(direct) && direct > 0) return direct;
  const output = Number(err?.output?.statusCode || err?.output?.status || 0);
  if (Number.isFinite(output) && output > 0) return output;
  const data = Number(err?.data?.statusCode || err?.data?.status || 0);
  if (Number.isFinite(data) && data > 0) return data;
  return 0;
}

function isRateLimitPhotoFetchError(err) {
  const status = extractErrorStatusCode(err);
  if (status === 429) return true;
  const msg = String(err?.message || err || "").toLowerCase();
  return msg.includes("rate") || msg.includes("too many");
}

function isPermanentPhotoFetchError(err) {
  const status = extractErrorStatusCode(err);
  if ([401, 403, 404].includes(status)) return true;
  const msg = String(err?.message || err || "").toLowerCase();
  return (
    msg.includes("not-authorized") ||
    msg.includes("not authorized") ||
    msg.includes("privacy") ||
    msg.includes("forbidden")
  );
}

async function enrichContactPhotosForProfile(profileId, options) {
  // Go backend or frontend can handle photo enrichment.
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

function enforceWaChatStorageLimitsForProfile(profileId) {
  if (!profileId) return 0;
  const state = ensureWaChatStateForProfile(profileId);
  const rows = Object.values(state.chatsByJid || {})
    .filter((x) => x && typeof x === "object" && normalizeChatJid(x.jid || ""))
    .map((chat) => {
      const jid = normalizeChatJid(chat.jid || "");
      const ts = Number(chat.lastMessageTimestampMs || 0) || 0;
      const pinned = chat.pinned === true;
      const unread = Math.max(0, Number(chat.unreadCount || 0) || 0);
      return { jid, ts, pinned, unread };
    });

  if (rows.length <= WA_CHAT_MAX_STORED_CHATS) return 0;

  rows.sort((a, b) => {
    const ap = a.pinned ? 1 : 0;
    const bp = b.pinned ? 1 : 0;
    if (ap !== bp) return bp - ap;
    const au = a.unread > 0 ? 1 : 0;
    const bu = b.unread > 0 ? 1 : 0;
    if (au !== bu) return bu - au;
    if (a.ts !== b.ts) return b.ts - a.ts;
    return a.jid.localeCompare(b.jid);
  });

  const keep = new Set(rows.slice(0, WA_CHAT_MAX_STORED_CHATS).map((x) => x.jid));
  let changed = 0;

  for (const jid of Object.keys(state.chatsByJid || {})) {
    const normalized = normalizeChatJid(jid);
    if (normalized && keep.has(normalized)) continue;
    delete state.chatsByJid[jid];
    changed++;
  }
  for (const jid of Object.keys(state.messagesByChat || {})) {
    const normalized = normalizeChatJid(jid);
    if (normalized && keep.has(normalized)) continue;
    delete state.messagesByChat[jid];
    changed++;
  }

  return changed;
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

function canonicalizeChatJidForProfile(profileId, chatJid) {
  const normalized = normalizeChatJid(chatJid);
  if (!normalized) return "";
  if (!profileId) return normalized;

  const mappedByCache = getMappedPnJidForProfile(profileId, normalized);
  if (isLidJid(normalized)) {
    const preferredFromContacts = getPreferredPnJidForLidFromContacts(profileId, normalized);
    if (preferredFromContacts) {
      rememberLidPnMappingForProfile(profileId, normalized, preferredFromContacts);
      return preferredFromContacts;
    }
    if (mappedByCache) return mappedByCache;
  } else if (mappedByCache) {
    return mappedByCache;
  }

  if (isPnJid(normalized)) {
    const contact = findContactByJidForProfile(profileId, normalized);
    const contactLid = normalizeLidJid(contact?.lid || contact?.lidJid || "");
    const mappedFromContactLid = contactLid
      ? getPreferredPnJidForLidFromContacts(profileId, contactLid) || getMappedPnJidForProfile(profileId, contactLid)
      : "";
    if (mappedFromContactLid && mappedFromContactLid !== normalized) {
      rememberLidPnMappingForProfile(profileId, contactLid, mappedFromContactLid);
      return mappedFromContactLid;
    }
    return normalized;
  }

  if (!isLidJid(normalized)) return normalized;

  const contact = findContactByJidForProfile(profileId, normalized);
  const preferred = normalizePhoneNumberJid(contact?.jid || contact?.phoneNumber || contact?.pnJid || "");
  if (preferred) {
    rememberLidPnMappingForProfile(profileId, normalized, preferred);
    return preferred;
  }

  const mappedMsisdn = normalizeMsisdnSafe(contact?.msisdn || contact?.phoneNumber || "");
  if (mappedMsisdn) {
    const fallbackPn = `${mappedMsisdn}@s.whatsapp.net`;
    rememberLidPnMappingForProfile(profileId, normalized, fallbackPn);
    return fallbackPn;
  }

  return normalized;
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

function resolveRecentRemoteJidForChat(profileId, chatJid) {
  if (!profileId) return "";
  const jid = normalizeChatJid(chatJid);
  if (!jid) return "";

  const state = ensureWaChatStateForProfile(profileId);
  const list = Array.isArray(state.messagesByChat[jid]) ? state.messagesByChat[jid] : [];
  for (let i = list.length - 1; i >= 0; i--) {
    const msg = list[i] && typeof list[i] === "object" ? list[i] : null;
    if (!msg) continue;
    const raw = msg.rawMessage && typeof msg.rawMessage === "object" ? msg.rawMessage : {};
    const key = raw.key && typeof raw.key === "object" ? raw.key : {};
    const candidates = [key.remoteJid, key.remoteJidAlt, key.remoteJidPn, raw.remoteJid, raw.chatId];
    for (const candidate of candidates) {
      const normalized = normalizeChatJid(candidate);
      if (!normalized) continue;
      if (normalized.endsWith("@g.us")) return normalized;
      if (isPnJid(normalized) || isLidJid(normalized)) return normalized;
    }
  }

  return "";
}

function buildSendTargetCandidatesForProfile(profileId, chatJidOrPhone) {
  const target = normalizeSendTargetJid(chatJidOrPhone);
  if (!target) return [];

  const out = [];
  const seen = new Set();
  const push = (candidate) => {
    const normalized = normalizeChatJid(candidate);
    if (!normalized) return;
    if (seen.has(normalized)) return;
    seen.add(normalized);
    out.push(normalized);
  };

  const profileKey = cleanString(profileId);
  if (!profileKey) {
    push(target);
    return out;
  }

  // Prefer phone-number JIDs for direct chats because they are the most
  // reliable send target across Baileys v7 RC builds.
  if (isLidJid(target)) {
    push(getPreferredPnJidForLidFromContacts(profileKey, target));
    push(getMappedPnJidForProfile(profileKey, target));
    const lidUser = jidUserPart(target);
    if (/^\d{8,15}$/.test(lidUser)) {
      push(`${lidUser}@s.whatsapp.net`);
    }
  }

  push(target);

  if (isPnJid(target)) {
    push(getMappedLidJidForProfile(profileKey, target));
  }

  const contact = findContactByJidForProfile(profileKey, target);
  if (contact && typeof contact === "object") {
    push(normalizePhoneNumberJid(contact.jid || contact.phoneNumber || contact.pnJid || ""));
    push(normalizeLidJid(contact.lid || contact.lidJid || ""));
  }

  const recent = resolveRecentRemoteJidForChat(profileKey, target);
  if (recent) {
    if (isLidJid(recent)) {
      push(getPreferredPnJidForLidFromContacts(profileKey, recent));
      push(getMappedPnJidForProfile(profileKey, recent));
      const recentUser = jidUserPart(recent);
      if (/^\d{8,15}$/.test(recentUser)) {
        push(`${recentUser}@s.whatsapp.net`);
      }
    }
    if (isPnJid(recent)) push(getMappedLidJidForProfile(profileKey, recent));
  }
  push(recent);

  return out;
}

function resolveMsisdnForSendTarget(profileId, chatJidOrPhone) {
  const target = normalizeSendTargetJid(chatJidOrPhone);
  if (!target) return "";

  const candidates = [];
  const seen = new Set();
  const push = (value) => {
    const raw = cleanString(value);
    if (!raw) return;
    const normalized = normalizeJidForContact(raw) || raw;
    if (!normalized || seen.has(normalized)) return;
    seen.add(normalized);
    candidates.push(normalized);
  };

  push(target);
  const profileKey = cleanString(profileId);
  if (profileKey) {
    push(getPreferredPnJidForLidFromContacts(profileKey, target));
    push(getMappedPnJidForProfile(profileKey, target));
    push(getMappedLidJidForProfile(profileKey, target));

    const contact = findContactByJidForProfile(profileKey, target);
    if (contact && typeof contact === "object") {
      push(contact.msisdn);
      push(contact.phoneNumber);
      push(contact.jid);
      push(contact.pnJid);
      push(contact.lid);
      push(contact.lidJid);
    }

    const recent = resolveRecentRemoteJidForChat(profileKey, target);
    push(recent);
    push(getPreferredPnJidForLidFromContacts(profileKey, recent));
    push(getMappedPnJidForProfile(profileKey, recent));

    if (isLidJid(target)) {
      const lidUser = jidUserPart(target);
      if (/^\d{8,15}$/.test(lidUser)) push(lidUser);
    }
  }

  for (const value of candidates) {
    const fromAddress = msisdnFromContactAddress(value);
    const msisdn = normalizeMsisdnSafe(fromAddress || value);
    if (msisdn) return msisdn;
  }

  return "";
}

function normalizeStoredMessageForChatJid(rawMessage, targetChatJid) {
  const targetJid = normalizeChatJid(targetChatJid);
  if (!targetJid) return null;

  const src = rawMessage && typeof rawMessage === "object" ? rawMessage : {};
  const key = normalizeMessageKey(src.key || {}, src.chatJid || targetJid || "");
  if (!key.id) return null;

  const fromMe = src.fromMe === true || key.fromMe === true;
  const participant = cleanString(key.participant || "");
  const media = normalizePersistedChatMedia(src.media);
  const text = String(src.text || "");
  const preview = String(src.preview || text || "");
  const type = cleanString(src.type || "unknown");
  const timestampMs = normalizeEpochMs(src.timestampMs || src?.rawMessage?.messageTimestamp || Date.now(), Date.now());

  return {
    key: {
      remoteJid: targetJid,
      id: key.id,
      fromMe,
      participant
    },
    hash: messageKeyHash({
      remoteJid: targetJid,
      id: key.id,
      fromMe,
      participant
    }),
    chatJid: targetJid,
    fromMe,
    pushName: cleanString(src.pushName || ""),
    timestampMs: Number(timestampMs || Date.now()),
    type,
    text,
    preview,
    hasMedia: src.hasMedia === true || !!media,
    media,
    status: Math.max(0, Number(src.status || 0) || 0),
    rawMessage: src.rawMessage && typeof src.rawMessage === "object" ? src.rawMessage : null
  };
}

function mergeStoredMessageListsForChat(targetChatJid, lists) {
  const targetJid = normalizeChatJid(targetChatJid);
  if (!targetJid) return [];

  const combined = [];
  for (const listRaw of Array.isArray(lists) ? lists : []) {
    const list = Array.isArray(listRaw) ? listRaw : [];
    for (const raw of list) {
      const normalized = normalizeStoredMessageForChatJid(raw, targetJid);
      if (!normalized) continue;
      combined.push(normalized);
    }
  }
  if (combined.length === 0) return [];

  combined.sort((a, b) => Number(a.timestampMs || 0) - Number(b.timestampMs || 0));
  const byHash = new Map();

  for (const msg of combined) {
    const prev = byHash.get(msg.hash);
    if (!prev) {
      byHash.set(msg.hash, msg);
      continue;
    }

    const prevMedia = prev?.media && typeof prev.media === "object" ? prev.media : null;
    const nextMedia = msg?.media && typeof msg.media === "object" ? msg.media : null;
    const mergedMedia =
      prevMedia || nextMedia
        ? {
          ...(prevMedia || {}),
          ...(nextMedia || {}),
          localPath: cleanString(nextMedia?.localPath || prevMedia?.localPath || ""),
          thumbnailDataUrl: compactThumbnailDataUrl(
            cleanString(nextMedia?.thumbnailDataUrl || prevMedia?.thumbnailDataUrl || "")
          )
        }
        : null;

    byHash.set(msg.hash, {
      ...prev,
      ...msg,
      key: {
        ...msg.key,
        remoteJid: targetJid
      },
      hash: msg.hash,
      chatJid: targetJid,
      timestampMs: Math.max(Number(prev.timestampMs || 0), Number(msg.timestampMs || 0)),
      text: String(msg.text || prev.text || ""),
      preview: String(msg.preview || prev.preview || msg.text || prev.text || ""),
      pushName: cleanString(msg.pushName || prev.pushName || ""),
      hasMedia: prev.hasMedia === true || msg.hasMedia === true || !!mergedMedia,
      media: mergedMedia,
      status: Math.max(Number(prev.status || 0), Number(msg.status || 0)),
      rawMessage: msg.rawMessage || prev.rawMessage || null
    });
  }

  return Array.from(byHash.values())
    .sort((a, b) => Number(a.timestampMs || 0) - Number(b.timestampMs || 0))
    .slice(-WA_CHAT_MAX_MESSAGES_PER_CHAT);
}

function mergeChatSummaryForCanonicalJid(targetChatJid, primaryChat, aliasChat, mergedMessages) {
  const targetJid = normalizeChatJid(targetChatJid);
  if (!targetJid) return null;

  const primary = primaryChat && typeof primaryChat === "object" ? primaryChat : {};
  const alias = aliasChat && typeof aliasChat === "object" ? aliasChat : {};
  const messageList = Array.isArray(mergedMessages) ? mergedMessages : [];
  const latestMessage = messageList.length > 0 ? messageList[messageList.length - 1] : null;

  const primaryTs = Number(primary.lastMessageTimestampMs || 0) || 0;
  const aliasTs = Number(alias.lastMessageTimestampMs || 0) || 0;
  const latestTs = Math.max(primaryTs, aliasTs, Number(latestMessage?.timestampMs || 0) || 0);
  const newestChat = aliasTs > primaryTs ? alias : primary;

  return {
    jid: targetJid,
    name: choosePreferredIdentityLabel(primary.name, alias.name),
    lastMessageTimestampMs: latestTs,
    lastMessagePreview: String(
      firstNonEmptyString([latestMessage?.preview, newestChat.lastMessagePreview, primary.lastMessagePreview, alias.lastMessagePreview])
    ),
    lastMessageType: cleanString(
      firstNonEmptyString([latestMessage?.type, newestChat.lastMessageType, primary.lastMessageType, alias.lastMessageType])
    ),
    lastMessageFromMe:
      latestMessage
        ? latestMessage.fromMe === true
        : newestChat.lastMessageFromMe === true || primary.lastMessageFromMe === true || alias.lastMessageFromMe === true,
    unreadCount: Math.max(Number(primary.unreadCount || 0) || 0, Number(alias.unreadCount || 0) || 0),
    archived: primary.archived === true || alias.archived === true,
    pinned: primary.pinned === true || alias.pinned === true,
    updatedAt: nowIsoShort()
  };
}

function mergeChatAliasIntoCanonicalForProfile(profileId, aliasJid, canonicalJid) {
  if (!profileId) return false;
  const alias = normalizeChatJid(aliasJid);
  const canonical = normalizeChatJid(canonicalJid);
  if (!alias || !canonical || alias === canonical) return false;

  const state = ensureWaChatStateForProfile(profileId);
  const aliasChat = state.chatsByJid[alias] && typeof state.chatsByJid[alias] === "object" ? state.chatsByJid[alias] : null;
  const canonicalChat =
    state.chatsByJid[canonical] && typeof state.chatsByJid[canonical] === "object" ? state.chatsByJid[canonical] : null;
  const aliasMessages = Array.isArray(state.messagesByChat[alias]) ? state.messagesByChat[alias] : [];
  const canonicalMessages = Array.isArray(state.messagesByChat[canonical]) ? state.messagesByChat[canonical] : [];

  if (!aliasChat && aliasMessages.length === 0) return false;

  const mergedMessages = mergeStoredMessageListsForChat(canonical, [canonicalMessages, aliasMessages]);
  if (mergedMessages.length > 0) {
    state.messagesByChat[canonical] = mergedMessages;
  } else if (!Array.isArray(state.messagesByChat[canonical]) || state.messagesByChat[canonical].length === 0) {
    delete state.messagesByChat[canonical];
  }
  delete state.messagesByChat[alias];

  const mergedChat = mergeChatSummaryForCanonicalJid(canonical, canonicalChat, aliasChat, mergedMessages);
  if (mergedChat) {
    state.chatsByJid[canonical] = mergedChat;
  }
  delete state.chatsByJid[alias];

  return true;
}

function reconcileCanonicalChatAliasesForProfile(profileId) {
  if (!profileId) return 0;

  const state = ensureWaChatStateForProfile(profileId);
  const keys = new Set([...Object.keys(state.chatsByJid || {}), ...Object.keys(state.messagesByChat || {})]);
  let changed = 0;

  for (const sourceJid of Array.from(keys)) {
    const canonicalJid = canonicalizeChatJidForProfile(profileId, sourceJid);
    if (!canonicalJid || canonicalJid === sourceJid) continue;
    if (mergeChatAliasIntoCanonicalForProfile(profileId, sourceJid, canonicalJid)) {
      changed++;
    }
  }

  if (changed > 0) {
    schedulePersistWaChatCache();
    scheduleWaChatSync(profileId, "jid_alias_reconcile");
  }

  return changed;
}

function reconcileCanonicalChatAliasesForAllProfiles() {
  let changed = 0;
  for (const profileId of Object.keys(waChatByProfileMem || {})) {
    changed += reconcileCanonicalChatAliasesForProfile(profileId);
  }
  return changed;
}

function scheduleChatAliasReconcileForProfile(profileId, delayMs = 420) {
  const key = cleanString(profileId);
  if (!key) return;
  const oldTimer = waAliasReconcileTimers.get(key);
  if (oldTimer) clearTimeout(oldTimer);

  const timer = setTimeout(() => {
    waAliasReconcileTimers.delete(key);
    try {
      reconcileCanonicalChatAliasesForProfile(key);
    } catch (e) {
      log.debug({ err: e, profileId: key }, "Alias chat reconcile failed");
    }
  }, Math.max(80, Number(delayMs || 420)));

  waAliasReconcileTimers.set(key, timer);
}

function normalizeIncomingChatPresenceType(raw) {
  const value = cleanString(raw).toLowerCase();
  return WA_ALLOWED_INCOMING_CHAT_PRESENCE.has(value) ? value : "";
}

function bytesToBase64(value) {
  if (!value) return "";
  try {
    // protojson (WhatsMeow) serializes bytes as base64 strings — pass through directly
    if (typeof value === "string") return value;
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
  const nonDisplayOnlyKeys = new Set(["messageContextInfo"]);

  if (!content || Object.keys(content).length === 0) {
    return {
      type: "unknown",
      text: "",
      preview: "",
      hasMedia: false,
      media: null,
      skip: false
    };
  }

  const displayKeys = Object.keys(content).filter((k) => !nonDisplayOnlyKeys.has(k));
  if (displayKeys.length === 0) {
    return {
      type: "context",
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
    const thumbDataUrl = compactThumbnailDataUrl(
      thumb ? `data:image/jpeg;base64,${thumb}` : localThumb.startsWith("data:image/") ? localThumb : ""
    );
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
        thumbnailDataUrl: compactThumbnailDataUrl(thumb ? `data:image/jpeg;base64,${thumb}` : "")
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

  const fallback = displayKeys[0] || "message";
  return {
    type: fallback,
    text: "",
    preview: `[${fallback}]`,
    hasMedia: false,
    media: null,
    skip: false
  };
}

function normalizeMessageKey(key, fallbackRemoteJid, profileId = "") {
  const src = key && typeof key === "object" ? key : {};
  const rawRemotePrimary = normalizeJidForContact(src.remoteJid || fallbackRemoteJid || "");
  const id = cleanString(src.id || "");
  const fromMe = src.fromMe === true;
  if (rawRemotePrimary === "status@broadcast") {
    return { remoteJid: "", id, participant: "", fromMe };
  }
  const remotePrimary = normalizeChatJid(src.remoteJid || fallbackRemoteJid || "");
  const remoteAlt = normalizeChatJid(src.remoteJidAlt || src.remoteJidPn || "");
  let remoteJid = remoteAlt || remotePrimary;
  const participantPrimary = normalizeJidForContact(src.participant || "");
  const participantAlt = normalizeJidForContact(src.participantAlt || src.participantPn || "");
  let participant = participantAlt || participantPrimary;

  const profileKey = cleanString(profileId);
  if (profileKey) {
    if (isLidJid(remotePrimary) && isPnJid(remoteAlt)) {
      rememberLidPnMappingForProfile(profileKey, remotePrimary, remoteAlt);
    } else if (isLidJid(remoteAlt) && isPnJid(remotePrimary)) {
      rememberLidPnMappingForProfile(profileKey, remoteAlt, remotePrimary);
    }

    if (isLidJid(participantPrimary) && isPnJid(participantAlt)) {
      rememberLidPnMappingForProfile(profileKey, participantPrimary, participantAlt);
    } else if (isLidJid(participantAlt) && isPnJid(participantPrimary)) {
      rememberLidPnMappingForProfile(profileKey, participantAlt, participantPrimary);
    }

    if (remoteJid) {
      remoteJid = canonicalizeChatJidForProfile(profileKey, remoteJid);
    }

    if (isLidJid(participant)) {
      const mappedParticipant = getMappedPnJidForProfile(profileKey, participant);
      if (mappedParticipant) participant = mappedParticipant;
    }
  }

  return { remoteJid, id, participant, fromMe };
}

function isStatusBroadcastEnvelope(rawMessage) {
  const msg = rawMessage && typeof rawMessage === "object" ? rawMessage : {};
  const key = msg.key && typeof msg.key === "object" ? msg.key : {};
  const candidates = [key.remoteJid, key.remoteJidAlt, key.remoteJidPn, msg.remoteJid, msg.chatId, msg.jid];
  for (const candidate of candidates) {
    const normalized = normalizeJidForContact(candidate || "");
    if (normalized === "status@broadcast") return true;
  }
  return false;
}

function messageKeyHash(key) {
  const k = key && typeof key === "object" ? key : {};
  return `${cleanString(k.remoteJid)}|${cleanString(k.id)}|${k.fromMe ? "1" : "0"}|${cleanString(k.participant)}`;
}

function getContactByChatJid(profileId, chatJid) {
  return findContactByJidForProfile(profileId, chatJid);
}

function ensureContactsFromChatsForProfile(profileId) {
  if (!profileId) return 0;
  const state = ensureWaChatStateForProfile(profileId);
  const chats = Object.values(state.chatsByJid || {});
  if (chats.length === 0) return 0;

  const contacts = [];
  for (const chat of chats) {
    const chatJid = normalizeChatJid(chat?.jid || "");
    if (!chatJid) continue;
    const isDirectContact = isPnJid(chatJid) || isLidJid(chatJid);
    if (!isDirectContact) continue;
    const canonicalJid = canonicalizeChatJidForProfile(profileId, chatJid) || chatJid;
    contacts.push({
      id: canonicalJid,
      jid: canonicalJid,
      lid: isLidJid(chatJid) ? chatJid : "",
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
  const chatMsisdn = msisdnFromContactAddress(c.jid);
  const direct = sanitizeIdentityLabelForProfile(
    profileId,
    firstNonEmptyString([c.name, c.subject, c.notify, c.pushName]),
    chatMsisdn
  );
  const byContact = getContactByChatJid(profileId, c.jid);
  const contactName = getContactDisplayName(byContact, profileId);
  const fallback = fallbackTitleForChatJid(c.jid);
  let best = "";
  best = choosePreferredIdentityLabel(best, direct);
  best = choosePreferredIdentityLabel(best, contactName);
  if (!best) best = fallback;
  return best || fallback;
}

function resolveMessageSenderName(profileId, chatJid, messageRecord) {
  if (!messageRecord || typeof messageRecord !== "object") return "";
  if (messageRecord.fromMe) return "You";

  const participant = normalizeJidForContact(messageRecord?.key?.participant || "");
  const fromJid = participant || normalizeJidForContact(chatJid);
  const contact = findContactByJidForProfile(profileId, fromJid);
  if (contact) {
    const name = getContactDisplayName(contact, profileId);
    if (name) return name;
    if (contact.msisdn) return String(contact.msisdn);
  }

  return messageRecord.pushName || "";
}

function ensureChatSummary(profileId, chatJid) {
  const state = ensureWaChatStateForProfile(profileId);
  const jid = canonicalizeChatJidForProfile(profileId, chatJid);
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
  let mappingChanged = 0;

  for (const raw of chats) {
    const src = raw && typeof raw === "object" ? raw : {};
    const chatPhoneJid = normalizePhoneNumberJid(src.phoneNumber || src.pnJid || "");
    const chatLidJid = normalizeLidJid(src.lid || src.lidJid || "");
    if (chatPhoneJid && chatLidJid) {
      mappingChanged += rememberLidPnMappingForProfile(profileId, chatLidJid, chatPhoneJid);
    }
    const chatJid = canonicalizeChatJidForProfile(profileId, chatPhoneJid || src.id || src.jid || chatLidJid || "");
    if (!chatJid) continue;

    const chat = ensureChatSummary(profileId, chatJid);
    if (!chat) continue;

    const incomingName = sanitizeIdentityLabelForProfile(
      profileId,
      firstNonEmptyString([src.name, src.subject, src.notify, src.pushName]),
      msisdnFromContactAddress(chatJid)
    );
    const nextName = choosePreferredIdentityLabel(chat.name, incomingName);
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
    changed += enforceWaChatStorageLimitsForProfile(profileId);
    scheduleWaChatSync(profileId, "chats");
    schedulePersistWaChatCache();
  }
  if (mappingChanged > 0) {
    scheduleChatAliasReconcileForProfile(profileId, 120);
  }
  return changed;
}

function normalizeMessageRecord(profileId, rawMessage) {
  const msg = rawMessage && typeof rawMessage === "object" ? rawMessage : {};
  if (isStatusBroadcastEnvelope(msg)) return null;
  const key = normalizeMessageKey(msg.key, msg?.key?.remoteJid || msg?.remoteJid || "", profileId);
  const rawChatJid = key.remoteJid || normalizeChatJid(msg?.chatId || msg?.jid || "");
  const chatJid = canonicalizeChatJidForProfile(profileId, rawChatJid);
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
    const normalized = normalizeMessageRecord(profileId, item);
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
          localPath: cleanString(normalized?.media?.localPath || prevMedia?.localPath || ""),
          thumbnailDataUrl: compactThumbnailDataUrl(
            cleanString(normalized?.media?.thumbnailDataUrl || prevMedia?.thumbnailDataUrl || "")
          )
        }
        : prevMedia || null;
      const nextRecord = {
        ...prev,
        ...normalized,
        media: nextMedia,
        rawMessage: normalized.rawMessage || prev.rawMessage
      };
      const sameMedia =
        String(prevMedia?.kind || "") === String(nextMedia?.kind || "") &&
        String(prevMedia?.mimeType || "") === String(nextMedia?.mimeType || "") &&
        String(prevMedia?.fileName || "") === String(nextMedia?.fileName || "") &&
        Number(prevMedia?.fileLength || 0) === Number(nextMedia?.fileLength || 0) &&
        String(prevMedia?.thumbnailDataUrl || "") === String(nextMedia?.thumbnailDataUrl || "") &&
        String(prevMedia?.localPath || "") === String(nextMedia?.localPath || "");
      const same =
        Number(prev?.timestampMs || 0) === Number(nextRecord.timestampMs || 0) &&
        prev?.fromMe === nextRecord.fromMe &&
        String(prev?.pushName || "") === String(nextRecord.pushName || "") &&
        String(prev?.type || "") === String(nextRecord.type || "") &&
        String(prev?.text || "") === String(nextRecord.text || "") &&
        String(prev?.preview || "") === String(nextRecord.preview || "") &&
        prev?.hasMedia === nextRecord.hasMedia &&
        Number(prev?.status || 0) === Number(nextRecord.status || 0) &&
        sameMedia;
      const rawUpgraded =
        !prev?.rawMessage &&
        !!nextRecord.rawMessage &&
        typeof nextRecord.rawMessage === "object" &&
        Object.keys(nextRecord.rawMessage).length > 0;
      if (!same || rawUpgraded) {
        list[existingIndex] = nextRecord;
        changed++;
      }
    } else {
      list.push(normalized);
      changed++;
    }

    if (list.length > WA_CHAT_MAX_MESSAGES_PER_CHAT) {
      list.sort((a, b) => Number(a.timestampMs || 0) - Number(b.timestampMs || 0));
      state.messagesByChat[normalized.chatJid] = list.slice(-WA_CHAT_MAX_MESSAGES_PER_CHAT);
    }

    const isLatest = Number(normalized.timestampMs || 0) >= Number(chat.lastMessageTimestampMs || 0);
    // Ignore outgoing pushName hints so our own profile name cannot overwrite recipient chat labels.
    const incomingPushName = normalized.fromMe ? "" : normalized.pushName;
    const nextName = choosePreferredIdentityLabel(chat.name, incomingPushName);
    if (isLatest || !chat.lastMessagePreview || !chat.lastMessageTimestampMs) {
      const nextChat = {
        ...chat,
        name: nextName,
        lastMessageTimestampMs: Number(normalized.timestampMs || chat.lastMessageTimestampMs || Date.now()),
        lastMessagePreview: cleanString(normalized.preview || ""),
        lastMessageType: cleanString(normalized.type || ""),
        lastMessageFromMe: normalized.fromMe === true,
        updatedAt: nowIsoShort()
      };
      const sameChat =
        String(chat.name || "") === String(nextChat.name || "") &&
        Number(chat.lastMessageTimestampMs || 0) === Number(nextChat.lastMessageTimestampMs || 0) &&
        String(chat.lastMessagePreview || "") === String(nextChat.lastMessagePreview || "") &&
        String(chat.lastMessageType || "") === String(nextChat.lastMessageType || "") &&
        chat.lastMessageFromMe === nextChat.lastMessageFromMe;
      if (!sameChat) {
        state.chatsByJid[normalized.chatJid] = nextChat;
        changed++;
      }
    } else if (chat.name !== nextName) {
      state.chatsByJid[normalized.chatJid] = {
        ...chat,
        name: nextName,
        updatedAt: nowIsoShort()
      };
      changed++;
    }

    if (normalized.hasMedia && String(normalized?.media?.kind || "").toLowerCase() === "image") {
      const localPath = cleanString(normalized?.media?.localPath || "");
      if (localPath && fs.existsSync(localPath)) {
        setLocalImagePathForStoredMessage(profileId, normalized.chatJid, normalized.hash, localPath);
      } else if (WA_ENABLE_AUTO_IMAGE_PRESAVE) {
        queueAutoSaveImageForHash(profileId, normalized.chatJid, normalized.hash);
      }
    }
  }

  if (changed > 0) {
    changed += enforceWaChatStorageLimitsForProfile(profileId);
    scheduleWaChatSync(profileId, "messages");
    schedulePersistWaChatCache();
    scheduleChatAliasReconcileForProfile(profileId, 120);
  }
  return changed;
}

function applyMessageUpdatesForProfile(profileId, updates) {
  if (!profileId || !Array.isArray(updates) || updates.length === 0) return 0;
  const state = ensureWaChatStateForProfile(profileId);
  let changed = 0;

  for (const patch of updates) {
    const src = patch && typeof patch === "object" ? patch : {};
    const key = normalizeMessageKey(src.key, src?.key?.remoteJid || "", profileId);
    if (!key.remoteJid || !key.id) continue;
    const chatJid = canonicalizeChatJidForProfile(profileId, key.remoteJid);
    if (!chatJid) continue;
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

    const normalized = normalizeMessageRecord(profileId, {
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
        localPath: cleanString(normalized?.media?.localPath || prevMedia?.localPath || ""),
        thumbnailDataUrl: compactThumbnailDataUrl(
          cleanString(normalized?.media?.thumbnailDataUrl || prevMedia?.thumbnailDataUrl || "")
        )
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
  const jid = canonicalizeChatJidForProfile(profileId, c.jid || "");
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
  if (opts.reconcileAliases === true) {
    reconcileCanonicalChatAliasesForProfile(profileId);
  }
  const state = ensureWaChatStateForProfile(profileId);
  const search = cleanString(opts.search || "").toLowerCase();
  const limitRaw = Number(opts.limit);
  const limit = Number.isFinite(limitRaw) ? Math.max(1, Math.min(250, Math.round(limitRaw))) : 120;

  const rowsByJid = new Map();
  for (const rawChat of Object.values(state.chatsByJid)) {
    const row = serializeChatSummary(profileId, rawChat);
    if (!row) continue;

    const existing = rowsByJid.get(row.jid);
    if (!existing) {
      rowsByJid.set(row.jid, row);
      continue;
    }

    const existingTs = Number(existing.lastMessageTimestampMs || 0);
    const nextTs = Number(row.lastMessageTimestampMs || 0);
    const preferred = nextTs >= existingTs ? row : existing;
    rowsByJid.set(row.jid, {
      ...preferred,
      title: choosePreferredIdentityLabel(existing.title, row.title),
      preview: String(firstNonEmptyString([preferred.preview, existing.preview, row.preview])),
      avatarUrl: cleanString(preferred.avatarUrl || existing.avatarUrl || row.avatarUrl || ""),
      unreadCount: Math.max(Number(existing.unreadCount || 0), Number(row.unreadCount || 0)),
      archived: existing.archived === true || row.archived === true,
      pinned: existing.pinned === true || row.pinned === true
    });
  }
  const rows = Array.from(rowsByJid.values());

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
        thumbnailDataUrl: compactThumbnailDataUrl(cleanString(msg.media.thumbnailDataUrl || "")),
        localPath: cleanString(msg.media.localPath || "")
      }
      : null,
    status: Number(msg.status || 0) || 0
  };
}

function getChatMessagesForProfile(profileId, chatJid, options) {
  const jid = canonicalizeChatJidForProfile(profileId, chatJid);
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

async function warmRecentHistoryForProfile(profileId, options) {
  return { ok: true, syncTriggered: false };
}

async function resetChatHistoryForProfile(profileId, chatJid, options) {
  const jid = canonicalizeChatJidForProfile(profileId, chatJid);
  if (!jid) throw new Error("Invalid chat");
  const opts = options && typeof options === "object" ? options : {};

  const state = ensureWaChatStateForProfile(profileId);
  const previousList = Array.isArray(state.messagesByChat[jid]) ? [...state.messagesByChat[jid]] : [];
  previousList.sort((a, b) => Number(a?.timestampMs || 0) - Number(b?.timestampMs || 0));
  const fallbackNewest = previousList.length > 0 ? previousList[previousList.length - 1] : null;
  const clearedCount = previousList.length;

  delete state.messagesByChat[jid];
  const chat = ensureChatSummary(profileId, jid);
  if (chat) {
    state.chatsByJid[jid] = {
      ...chat,
      updatedAt: nowIsoShort()
    };
  }
  schedulePersistWaChatCache();
  scheduleWaChatSync(profileId, "chat_reset");

  // Go backend handles chat reset sync.
  return { ok: true, clearedCount };
}

function findMessageRecordForProfile(profileId, chatJid, key) {
  const jid = canonicalizeChatJidForProfile(profileId, chatJid);
  if (!jid) return null;
  const normalizedKey = normalizeMessageKey(key, jid, profileId);
  normalizedKey.remoteJid = canonicalizeChatJidForProfile(profileId, normalizedKey.remoteJid || jid) || jid;
  if (!normalizedKey.id) return null;
  const state = ensureWaChatStateForProfile(profileId);
  const list = Array.isArray(state.messagesByChat[jid]) ? state.messagesByChat[jid] : [];
  const hash = messageKeyHash(normalizedKey);
  return list.find((x) => x && x.hash === hash) || null;
}

// Legacy markChatRead and sendChatPresence removed.
// These are now handled by Go-based versions.

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

    const maxSide = 220;
    let preview = img;
    const largest = Math.max(size.width, size.height);
    if (largest > maxSide) {
      const scale = maxSide / largest;
      const width = Math.max(1, Math.round(size.width * scale));
      const height = Math.max(1, Math.round(size.height * scale));
      const resized = img.resize({ width, height, quality: "good" });
      if (resized && !resized.isEmpty()) preview = resized;
    }

    const jpeg = preview.toJPEG(62);
    if (jpeg && jpeg.length > 0) {
      return compactThumbnailDataUrl(`data:image/jpeg;base64,${jpeg.toString("base64")}`);
    }
    return compactThumbnailDataUrl(preview.toDataURL());
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
    thumbnailDataUrl: compactThumbnailDataUrl(cleanString(prevMedia.thumbnailDataUrl || "") || thumbFromFile || "")
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
  if (!profileId || !jid || !messageHash) return { ok: false, reason: "bad_args" };

  const state = ensureWaChatStateForProfile(profileId);
  const list = Array.isArray(state.messagesByChat[jid]) ? state.messagesByChat[jid] : [];
  const found = list.find((x) => x && x.hash === messageHash);
  if (!found) return { ok: false, reason: "message_not_found" };

  // Already downloaded?
  const existingPath = cleanString(found?.media?.localPath || "");
  if (existingPath && fs.existsSync(existingPath)) return { ok: true, localPath: existingPath };

  // Need the raw proto message to ask the Go backend to download it
  const rawMessage = found?.rawMessage;
  const protoMessage = rawMessage?.message;
  if (!protoMessage) return { ok: false, reason: "no_raw_message" };

  try {
    const resp = await fetch("http://127.0.0.1:12345/api/media/download", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ message: protoMessage }),
      signal: AbortSignal.timeout(30000)
    });
    const body = await resp.json();
    if (!body?.ok || !body?.data) return { ok: false, reason: body?.error || "download_failed" };

    const buf = Buffer.from(body.data, "base64");
    const filePath = buildStoredImagePath(profileId, found);
    fs.writeFileSync(filePath, buf);
    setLocalImagePathForStoredMessage(profileId, jid, messageHash, filePath);
    return { ok: true, localPath: filePath };
  } catch (e) {
    return { ok: false, reason: String(e?.message || e) };
  }
}

function queueAutoSaveImageForHash(profileId, chatJid, messageHash) {
  const jid = normalizeChatJid(chatJid);
  if (!profileId || !jid || !messageHash) return;
  const ticket = `${profileId}|${jid}|${messageHash}`;
  if (waImageAutoSaveInFlight.has(ticket)) return;
  waImageAutoSaveInFlight.add(ticket);

  setTimeout(() => {
    ensureLocalImageForStoredMessage(profileId, jid, messageHash)
      .catch(() => { })
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

function shouldRetrySendWithAlternateTarget(error) {
  const status = extractErrorStatusCode(error);
  if ([400, 404, 406, 410].includes(status)) return true;

  const msg = String(error?.message || error || "").toLowerCase();
  if (!msg) return false;
  if (msg.includes("connection closed") || msg.includes("timed out") || msg.includes("stream erro")) return false;
  if (msg.includes("invalid jid")) return true;
  if (msg.includes("not a whatsapp user")) return true;
  if (msg.includes("recipient") && (msg.includes("invalid") || msg.includes("unknown") || msg.includes("not found"))) {
    return true;
  }
  if (msg.includes("jid") && (msg.includes("invalid") || msg.includes("unknown") || msg.includes("not found"))) {
    return true;
  }
  return false;
}

function isSendConnectionError(error) {
  const status = extractErrorStatusCode(error);
  if ([408, 428, 440, 499].includes(status)) return true;

  const msg = String(error?.message || error || "").toLowerCase();
  if (!msg) return false;
  if (msg.includes("connection closed")) return true;
  if (msg.includes("not connected")) return true;
  if (msg.includes("timed out")) return true;
  if (msg.includes("stream erro")) return true;
  if (msg.includes("connection lost")) return true;
  return false;
}

function shouldExpandSendTargetsAfterFailure(error) {
  if (isSendConnectionError(error)) return false;
  const msg = String(error?.message || error || "").toLowerCase();
  if (!msg) return true;
  return true;
}

async function expandSendTargetCandidatesWithSignalMappings(targetCandidates) {
  const out = [];
  const seen = new Set();
  const push = (candidate) => {
    const normalized = normalizeChatJid(candidate);
    if (!normalized) return;
    if (seen.has(normalized)) return;
    seen.add(normalized);
    out.push(normalized);
  };

  for (const candidate of Array.isArray(targetCandidates) ? targetCandidates : []) {
    push(candidate);
  }

  const lidMapping = null;
  const canMapPnToLid = lidMapping && typeof lidMapping.getLIDForPN === "function";
  const canMapLidToPn = lidMapping && typeof lidMapping.getPNForLID === "function";

  for (const candidate of [...out]) {
    if (isPnJid(candidate)) {
      if (canMapPnToLid) {
        try {
          const mappedLid = await lidMapping.getLIDForPN(candidate);
          push(mappedLid);
        } catch {
          // ignore and continue best-effort
        }
      }

      const user = jidUserPart(candidate);
      if (/^\d{8,15}$/.test(user)) {
        push(`${user}@lid`);
      }
      continue;
    }

    if (!isLidJid(candidate)) continue;

    if (canMapLidToPn) {
      try {
        const mappedPn = await lidMapping.getPNForLID(candidate);
        push(mappedPn);
      } catch {
        // ignore and continue best-effort
      }
    }

    const user = jidUserPart(candidate);
    if (/^\d{8,15}$/.test(user)) {
      push(`${user}@s.whatsapp.net`);
    }
  }

  return out;
}

async function expandSendTargetCandidatesWithOnWhatsApp(targetCandidates) {
  const out = [];
  const seen = new Set();
  const push = (candidate) => {
    const normalized = normalizeChatJid(candidate);
    if (!normalized) return;
    if (seen.has(normalized)) return;
    seen.add(normalized);
    out.push(normalized);
  };

  for (const candidate of Array.isArray(targetCandidates) ? targetCandidates : []) {
    push(candidate);
  }

  if (!isConnected) return out;

  const lookupPhones = [];
  const lookupSeen = new Set();
  for (const candidate of out) {
    if (!isPnJid(candidate)) continue;
    const msisdn = normalizeMsisdnSafe(candidate);
    if (!msisdn || lookupSeen.has(msisdn)) continue;
    lookupSeen.add(msisdn);
    lookupPhones.push(msisdn);
  }
  if (lookupPhones.length === 0) return out;

  try {
    const res = await fetch("http://localhost:12345/api/onwhatsapp", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ phones: lookupPhones })
    });
    const data = await res.json();
    if (!data.ok) throw new Error(data.error || "Go onwhatsapp failed");

    for (const row of Array.isArray(data.data) ? data.data : []) {
      if (row.IsIn === false) continue;
      const jid = row.JID || "";
      if (jid) {
        push(jid);
        push(normalizePhoneNumberJid(jid));
      }
    }
  } catch (e) {
    log.debug({ err: e, phoneCount: lookupPhones.length }, "expandSendTargetCandidatesWithOnWhatsApp Go API failed");
  }

  return out;
}

function sendTargetPriority(jid) {
  const normalized = normalizeChatJid(jid);
  if (!normalized) return 99;
  if (normalized.endsWith("@g.us")) return 0;
  if (isPnJid(normalized)) return 1;
  if (isLidJid(normalized)) return 2;
  return 3;
}

function prioritizeSendTargets(candidates) {
  const list = Array.isArray(candidates) ? candidates.filter(Boolean) : [];
  if (list.length <= 1) return list;
  return [...list].sort((a, b) => {
    const scoreDiff = sendTargetPriority(a) - sendTargetPriority(b);
    if (scoreDiff !== 0) return scoreDiff;
    return String(a).localeCompare(String(b));
  });
}


// Legacy sending logic removed.

function normalizeWaPresenceStatus(input) {
  const value = cleanString(input).toLowerCase();
  if (value === "composing" || value === "paused") return value;
  return "";
}

async function sendChatPresence(payload) {
  const { chatJid, status } = payload || {};
  if (!chatJid || !status) throw new Error("Invalid presence request");

  try {
    const res = await fetch("http://localhost:12345/api/presence", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ chatJid, status })
    });
    const data = await res.json();
    if (!data.ok) throw new Error(data.error || "Go presence update failed");
    return { ok: true, chatJid, status };
  } catch (err) {
    return { ok: false, error: err.message };
  }
}

async function sendChatTypingPresence(payload) {
  const { chatJid, isTyping = true } = payload || {};
  const status = isTyping ? "composing" : "paused";
  return await sendChatPresence({ chatJid, status });
}

async function downloadChatMedia(payload) {
  // Go backend handles media download.
  throw new Error("Media download is not yet implemented for the Go backend.");
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
  message,
  timeoutMs
}) {
  if (!endpoint) throw new Error("AI rewrite endpoint is not set");
  const messageText = String(message || "").trim();
  if (!messageText) throw new Error("AI rewrite message is empty");

  const controller = new AbortController();
  const timeoutValue = Number.isFinite(Number(timeoutMs)) ? Number(timeoutMs) : 30000;
  const timeout = setTimeout(() => controller.abort(), timeoutValue);
  try {
    const headers = { "Content-Type": "application/json" };
    if (authToken) {
      headers.Authorization = authToken.startsWith("Bearer ") ? authToken : `Bearer ${authToken}`;
    }

    // Xano contract: one input field named "prompt".
    // Backend already owns the system prompt; client sends only message text.
    const payload = { prompt: messageText };

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


// Baileys legacy connection logic removed.
// Go backend manages its own connection state.


let sentCountTodayByProfile = new Map();
let lastResetDate = new Date().toDateString();

function checkDailyLimit(profileId) {
  const today = new Date().toDateString();
  if (today !== lastResetDate) {
    sentCountTodayByProfile.clear();
    lastResetDate = today;
  }
  const count = sentCountTodayByProfile.get(profileId) || 0;
  const limit = 200; // Default limit
  if (count >= limit) {
    throw new Error(`Daily sending limit reached (${limit}). Account safety measure.`);
  }
}

function incrementSentCount(profileId) {
  const count = sentCountTodayByProfile.get(profileId) || 0;
  sentCountTodayByProfile.set(profileId, count + 1);
}

async function autoReconnectActiveProfile() {
  const profileId = getActiveProfileId();
  if (!profileId) return { ok: false, error: "No active profile" };

  log.info({ profileId }, "Auto reconnecting profile...");
  try {
    const controller = new AbortController();
    const t = setTimeout(() => controller.abort(), 4000);
    const res = await fetch("http://localhost:12345/api/status", { signal: controller.signal });
    clearTimeout(t);
    const data = await res.json();
    if (data.ok && data.data) {
      isConnected = data.data.connected;
      const statusText = isConnected ? "Connected" : "Not connected";
      win?.webContents.send("wa:status", {
        isConnected,
        connected: isConnected,
        text: statusText,
        pushName: data.data.pushName || "",
        profileId
      });
      return { ok: true, isConnected };
    }
    return { ok: false, error: "Backend status check failed" };
  } catch (err) {
    log.warn({ err: err.message }, "Failed to auto reconnect profile check");
    return { ok: false, error: err.message };
  }
}

async function ensureConnectedForSend(profileId) {
  if (isConnected) return true;
  const res = await autoReconnectActiveProfile().catch(() => ({ isConnected: false }));
  return res.isConnected || false;
}

async function sendPayloadToMsisdn(msisdn, messagePayload, sendOptions = {}) {
  const profileId = getActiveProfileId();
  if (profileId) {
    checkDailyLimit(profileId);
  }

  const chatJid = `${msisdn}@s.whatsapp.net`;
  // ... rest of function remains the same ...

  let text = "";
  let attachment = null;

  if (messagePayload.text) {
    text = messagePayload.text;
  } else if (messagePayload.caption) {
    text = messagePayload.caption;
  }

  const mediaKinds = ["image", "video", "audio", "document"];
  for (const kind of mediaKinds) {
    if (messagePayload[kind]) {
      const media = messagePayload[kind];
      attachment = {
        path: media.url || media.path,
        fileName: messagePayload.fileName || path.basename(media.url || media.path || "file"),
        mimeType: messagePayload.mimetype || getMimeTypeForPath(media.url || media.path || ""),
        kind: kind
      };
      break;
    }
  }

  const res = await sendChatMessage({ chatJid, text, attachment });
  if (!res.ok) throw new Error(res.error || "Failed to send message via Go");
  if (profileId) {
    incrementSentCount(profileId);
  }
}

async function sendText(msisdn, text) {
  if (!(await ensureConnectedForSend()) || !isConnected) {
    throw new Error("WhatsApp not connected");
  }
  await sendPayloadToMsisdn(msisdn, { text: String(text || "") });
}

function normalizeTemplateMessagesForSend(inputMessages, legacyBody = "") {
  return normalizeTemplateMessagesList(inputMessages, legacyBody).map((msg) => {
    const type = normalizeTemplateMessageType(msg.type);
    return {
      id: cleanString(msg.id),
      type,
      text: String(msg.text || ""),
      attachment: type === "text" ? null : normalizeTemplateAttachmentRecord(msg.attachment, type)
    };
  });
}

function validateTemplateMessagesForSend(messages) {
  const rows = Array.isArray(messages) ? messages : [];
  if (rows.length === 0) throw new Error("Template has no messages");
  for (let i = 0; i < rows.length; i++) {
    const row = rows[i] && typeof rows[i] === "object" ? rows[i] : {};
    const type = normalizeTemplateMessageType(row.type);
    if (type === "text") continue;
    const attachment = normalizeTemplateAttachmentRecord(row.attachment, type);
    const filePath = cleanString(attachment?.path || "");
    if (!filePath) throw new Error(`Template message ${i + 1} is missing media attachment`);
    if (!fs.existsSync(filePath)) throw new Error(`Template message ${i + 1} attachment file not found`);
    const stat = fs.statSync(filePath);
    if (!stat || !stat.isFile()) throw new Error(`Template message ${i + 1} attachment must be a file`);
  }
}

function buildTemplateMessagePayload(message, textOverride = null) {
  const row = message && typeof message === "object" ? message : {};
  const type = normalizeTemplateMessageType(row.type);
  const textValue = textOverride === null ? row.text : textOverride;
  const text = String(textValue || "");
  const trimmedText = text.trim();

  if (type === "text") {
    if (!trimmedText) return null;
    return { text: trimmedText };
  }

  const attachment = normalizeTemplateAttachmentRecord(row.attachment, type);
  const filePath = cleanString(attachment?.path || "");
  if (!filePath) throw new Error("Template media attachment is missing");
  if (!fs.existsSync(filePath)) throw new Error("Template media attachment file not found");
  const fileStat = fs.statSync(filePath);
  if (!fileStat || !fileStat.isFile()) throw new Error("Template media attachment must be a file");

  const fileName = cleanString(attachment.fileName || path.basename(filePath));
  const mimeType = cleanString(attachment.mimeType || getMimeTypeForPath(filePath));
  const detectedKind = attachmentKindFromMimeOrPath(mimeType, filePath);
  let kind = type;
  if (kind === "text") kind = normalizeTemplateMessageType(detectedKind);
  if (kind === "text") kind = detectedKind || "document";

  if (kind === "image") {
    return {
      image: { url: filePath },
      ...(trimmedText ? { caption: trimmedText } : {})
    };
  }
  if (kind === "video") {
    return {
      video: { url: filePath },
      ...(mimeType ? { mimetype: mimeType } : {}),
      ...(trimmedText ? { caption: trimmedText } : {})
    };
  }
  if (kind === "audio") {
    return {
      audio: { url: filePath },
      ...(mimeType ? { mimetype: mimeType } : {}),
      ptt: false
    };
  }
  return {
    document: { url: filePath },
    fileName: fileName || path.basename(filePath),
    ...(mimeType ? { mimetype: mimeType } : {}),
    ...(trimmedText ? { caption: trimmedText } : {})
  };
}

function summarizeTemplateMessagesForAudit(messages) {
  const rows = Array.isArray(messages) ? messages : [];
  const out = [];
  for (const row of rows) {
    const type = normalizeTemplateMessageType(row?.type);
    const text = String(row?.text || "").trim();
    if (type === "text") {
      if (text) out.push(text);
      continue;
    }
    const kindLabel =
      type === "image"
        ? "Image"
        : type === "video"
          ? "Video"
          : type === "audio"
            ? "Audio"
            : type === "document"
              ? "Document"
              : "Media";
    const fileLabel = cleanString(row?.attachment?.fileName || "");
    const mediaHead = fileLabel ? `[${kindLabel}] ${fileLabel}` : `[${kindLabel}]`;
    if (text) out.push(`${mediaHead}\n${text}`);
    else out.push(mediaHead);
  }
  return out.join("\n\n").trim();
}

async function disconnectActiveProfileSocket(statusText = "Disconnected") {
  const activeProfileId = getActiveProfileId();
  try {
    const controller = new AbortController();
    const t = setTimeout(() => controller.abort(), 5000); // 5s timeout
    const res = await fetch("http://localhost:12345/api/logout", { signal: controller.signal });
    clearTimeout(t);
    const data = await res.json();
    if (!data.ok) log.warn({ data }, "Go logout returned error");
  } catch (err) {
    log.warn({ err: err.message }, "Failed to call Go logout");
  }

  win?.webContents.send("wa:status", {
    connected: false,
    isConnected: false,
    text: String(statusText || "Disconnected"),
    profileId: activeProfileId
  });
  return { ok: true, profileId: activeProfileId, disconnected: true };
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

ipcMain.handle("app:exportTemplatesBundle", async (_evt, payload) => {
  const src = payload && typeof payload === "object" ? payload : {};
  const mode = cleanString(src.mode).toLowerCase();
  const selectedTemplateId = cleanString(src.templateId);
  const isSingleTemplateMode = mode === "single_marketing_template";
  const exportLabel = isSingleTemplateMode ? "Export marketing template" : "Export templates";
  let defaultPath = `clinic_templates_${new Date().toISOString().slice(0, 10)}.json`;
  if (isSingleTemplateMode) {
    const currentTemplates = readTemplates();
    const selected = currentTemplates.find((t) => cleanString(t?.id) === selectedTemplateId) || null;
    const safeName = sanitizePathSegment(selected?.name || selectedTemplateId || "marketing_template", "marketing_template");
    defaultPath = `${safeName}_${new Date().toISOString().slice(0, 10)}.json`;
  }

  const saveRes = await dialog.showSaveDialog({
    title: exportLabel,
    defaultPath,
    filters: [{ name: "JSON", extensions: ["json"] }]
  });
  if (saveRes.canceled || !saveRes.filePath) return { ok: false, canceled: true };

  const exportPayload = isSingleTemplateMode
    ? getSingleMarketingTemplateExportBundle(selectedTemplateId)
    : getTemplateExportBundle();
  fs.writeFileSync(saveRes.filePath, JSON.stringify(exportPayload, null, 2), "utf-8");
  return { ok: true, filePath: saveRes.filePath };
});

ipcMain.handle("app:importTemplatesBundle", async (_evt, payload) => {
  const src = payload && typeof payload === "object" ? payload : {};
  const mode = cleanString(src.mode).toLowerCase();
  const isSingleTemplateMode = mode === "single_marketing_template";

  const openRes = await dialog.showOpenDialog({
    title: isSingleTemplateMode ? "Import marketing template" : "Import templates",
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

  const result = isSingleTemplateMode ? importSingleMarketingTemplateBundle(parsed) : importTemplateBundle(parsed);
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

ipcMain.handle("clinic:editPatient", async (_evt, payload) => {
  const session = getAuthSession();
  if (!session.authToken) return { ok: false, skipped: true, reason: "not_logged_in" };

  try {
    await clinicEditPatient(session.authToken, payload || {});
    return { ok: true };
  } catch (e) {
    return {
      ok: false,
      error: String(e?.message || e)
    };
  }
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
  await disconnectActiveProfileSocket("Profile selected. Click Connect to start session.");
  return { ok: true, activeProfileId: getActiveProfileId() };
});

ipcMain.handle("wa:handshake", async (_evt, payload) => {
  const method = payload?.method === "pairing" ? "pairing" : "qr";
  win?.webContents.send("wa:status", { text: "Connecting to Go Backend...", reason: "connecting" });

  try {
    if (method === "qr") {
      const resp = await fetch("http://localhost:12345/api/login/qr");
      const data = await resp.json();
      if (!data.ok) throw new Error(data.error || "Cannot fetch QR code");
      const qrcode = require("qrcode");
      const dataUrl = await qrcode.toDataURL(data.data, { margin: 2, scale: 6 });
      win?.webContents.send("wa:qr", dataUrl); // Signal ready
      win?.webContents.send("wa:status", { text: "Scan QR in WhatsApp", reason: "connecting" });
      return { ok: true, data: dataUrl };
    } else {
      const phone = cleanString(payload?.phone || "");
      if (!phone) throw new Error("Phone number required");
      const resp = await fetch(`http://localhost:12345/api/login/pair?phone=${phone}`);
      const data = await resp.json();
      if (!data.ok) throw new Error(data.error || "Cannot fetch pairing code");
      win?.webContents.send("wa:pairingCode", { code: data.data });
      win?.webContents.send("wa:status", { text: "Enter pairing code in WhatsApp", reason: "connecting" });
      return { ok: true, data: data.data };
    }
  } catch (err) {
    win?.webContents.send("wa:status", { text: err.message || "Failed to connect", isError: true, isConnected: false });
    return { ok: false, error: err.message };
  }
});

ipcMain.handle("wa:autoReconnect", async () => {
  return await autoReconnectActiveProfile();
});

ipcMain.handle("wa:disconnect", async () => {
  return await disconnectActiveProfileSocket();
});

ipcMain.handle("wa:getConnectionState", async () => {
  try {
    const res = await fetch("http://localhost:12345/api/status");
    const data = await res.json();
    const isConn = !!data.data?.connected;
    return {
      ok: true,
      connected: isConn,
      isConnected: isConn,
      text: isConn ? "Connected" : "Not connected",
      profileId: getActiveProfileId(),
      waVersion: "Go-WhatsMeow",
      pushName: data.data?.pushName || ""
    };
  } catch (err) {
    return { connected: false, isConnected: false, text: "Not connected", profileId: getActiveProfileId() };
  }
});

ipcMain.handle("wa:getContacts", async (_evt, options) => {
  const profileId = getActiveProfileId();
  const opts = options && typeof options === "object" ? options : {};
  try {
    const resp = await fetch("http://localhost:12345/api/contacts");
    const json = await resp.json();
    if (!json.ok) throw new Error(json.error || "Failed to fetch contacts");

    // Go returns map[string]ContactInfo
    const rawContacts = json.data || {};
    const contacts = [];
    for (const [jid, info] of Object.entries(rawContacts)) {
      contacts.push({
        jid,
        msisdn: msisdnFromUserJid(jid),
        name: info.FullName || info.PushName || "",
        pushName: info.PushName || "",
        isBusiness: info.IsBusiness || false,
        profilePictureUrl: "" // Can be enriched later
      });
    }

    return {
      ok: true,
      connected: !!isConnected,
      profileId,
      contacts,
      count: contacts.length
    };
  } catch (err) {
    console.error("Failed to fetch contacts from Go:", err);
    return { ok: false, error: err.message, contacts: [], count: 0 };
  }
});

ipcMain.handle("wa:getRecentChats", async (_evt, options) => {
  const profileId = getActiveProfileId();
  const opts = options && typeof options === "object" ? options : {};

  try {
    const resp = await fetch("http://localhost:12345/api/chats");
    const json = await resp.json();
    if (json.ok && json.data) {
      const state = ensureWaChatStateForProfile(profileId);
      for (const [jid, settings] of Object.entries(json.data)) {
        const chat = ensureChatSummary(profileId, jid);
        if (chat) {
          chat.archived = settings.Archived || false;
          chat.pinned = settings.Pinned || false;
          chat.unreadCount = settings.UnreadCount || chat.unreadCount || 0;
          chat.muteUntil = settings.MutedUntil || 0;
        }
      }
    }
  } catch (err) {
    log.debug({ err }, "Failed to bridge wa:getRecentChats to Go API");
  }

  promoteChatNamesFromContactsForProfile(profileId);
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

  // Bridge call to Go (allows Go to trigger on-demand sync if needed)
  try {
    const limit = src.limit || 50;
    await fetch(`http://localhost:12345/api/messages?chatJid=${encodeURIComponent(chatJid)}&limit=${limit}`);
  } catch (err) {
    // Ignore errors for now as we have local cache
  }

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

async function markChatReadForProfile(profileId, chatJid) {
  try {
    const lastMsgJid = chatJid; // Simplified
    const res = await fetch("http://localhost:12345/api/read", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ chatJid: lastMsgJid, messageId: "" }) // Go backend will need more logic if we want specific msg
    });
    const data = await res.json();
    if (!data.ok) throw new Error(data.error || "Go mark read failed");
    return { ok: true };
  } catch (err) {
    return { ok: false, error: err.message };
  }
}

ipcMain.handle("wa:markChatRead", async (_evt, payload) => {
  const src = payload && typeof payload === "object" ? payload : {};
  const profileId = getActiveProfileId();
  const chatJid = normalizeChatJid(src.chatJid || "");
  if (!chatJid) throw new Error("Invalid chat");
  return await markChatReadForProfile(profileId, chatJid);
});

ipcMain.handle("wa:resetChatHistory", async (_evt, payload) => {
  const src = payload && typeof payload === "object" ? payload : {};
  const profileId = getActiveProfileId();
  const chatJid = normalizeChatJid(src.chatJid || "");
  if (!chatJid) throw new Error("Invalid chat");
  return await resetChatHistoryForProfile(profileId, chatJid, src || {});
});

async function sendChatMessage(payload) {
  const { chatJid, text, attachment } = payload || {};
  if (!chatJid) throw new Error("Invalid message request: missing chatJid");

  try {
    const res = await fetch("http://localhost:12345/api/send", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ chatJid, text, attachment })
    });
    const data = await res.json();
    if (!data.ok) throw new Error(data.error || "Go send failed");
    return { ok: true, data: data.data };
  } catch (err) {
    return { ok: false, error: err.message };
  }
}

ipcMain.handle("wa:sendPresence", async (_evt, payload) => {
  return await sendChatPresence(payload || {});
});

ipcMain.handle("wa:sendChatMessage", async (_evt, payload) => {
  return await sendChatMessage(payload || {});
});

ipcMain.handle("wa:setTyping", async (_evt, payload) => {
  return await sendChatTypingPresence(payload || {});
});

ipcMain.handle("wa:pickAttachment", async () => {
  const openRes = await dialog.showOpenDialog({
    title: "Select attachment",
    properties: ["openFile", "multiSelections"],
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

  const attachments = [];
  for (const filePathRaw of openRes.filePaths) {
    const filePath = cleanString(filePathRaw);
    if (!filePath) continue;
    if (!fs.existsSync(filePath)) continue;
    const stat = fs.statSync(filePath);
    if (!stat || !stat.isFile()) continue;
    const fileName = path.basename(filePath);
    const mimeType = getMimeTypeForPath(filePath);
    const kind = attachmentKindFromMimeOrPath(mimeType, filePath);
    attachments.push({
      path: filePath,
      fileName,
      mimeType,
      kind,
      size: Number(stat.size || 0) || 0
    });
  }
  if (attachments.length === 0) return { ok: false, canceled: true };

  return {
    ok: true,
    attachment: attachments[0],
    attachments
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
    templateMessages,
    templateBody,
    recipients,
    varsByPhone,
    aiRewrite,
    pacing = { pattern: "cycle", minSec: 7, maxSec: 10 },
    templatePacing = { pattern: "random", minSec: 2, maxSec: 4 },
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
  const templatePattern = templatePacing.pattern === "cycle" ? "cycle" : "random";
  const templateMinSec = clampInt(templatePacing.minSec, 1, 90, 2);
  const templateMaxSec = Math.max(templateMinSec, clampInt(templatePacing.maxSec, 1, 90, 4));
  const aiCfg = normalizeAiRewriteConfig({ ...getAiRewriteConfig(), ...(aiRewrite || {}) });
  const clinicSession = getAuthSession();
  const sessionAuthToken = cleanString(clinicSession?.authToken);
  const aiAuthToken = cleanString(aiCfg.authToken) || sessionAuthToken;
  if (aiCfg.enabled && !aiCfg.endpoint) {
    throw new Error("AI rewrite is enabled but backend endpoint is empty");
  }
  if (aiCfg.enabled && !aiAuthToken) {
    throw new Error("AI rewrite requires Authorization token. Please log in first.");
  }
  const normalizedBatchMessages = normalizeTemplateMessagesForSend(templateMessages, templateBody || "");
  validateTemplateMessagesForSend(normalizedBatchMessages);

  const activeProfileIdForBatch = getActiveProfileId();
  if (!(await ensureConnectedForSend(activeProfileIdForBatch))) {
    throw new Error("WhatsApp not connected");
  }

  let sent = 0;
  let failed = 0;
  let skipped = 0;
  const skippedAlreadySentPhones = [];
  const skippedAlreadySentAtByPhone = {};
  const batchJob = beginBatchJob("sendBatch", recipients.length);
  let stopped = false;
  let stoppedAtIndex = 0;
  let stopEventSent = false;
  const emitStopped = (indexHint) => {
    if (stopEventSent) return;
    stopEventSent = true;
    win?.webContents.send("batch:progress", {
      ts: nowIsoShort(),
      index: Math.max(1, Math.min(recipients.length || 1, Number(indexHint || 1))),
      total: recipients.length,
      phone: "",
      status: "stopped",
      error: "Stopped by user"
    });
  };

  try {
    for (let i = 0; i < recipients.length; i++) {
      if (isBatchJobCancelRequested(batchJob)) {
        stopped = true;
        stoppedAtIndex = i + 1;
        emitStopped(stoppedAtIndex);
        break;
      }

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
        const lastSentAt = getSentAt(templateId, msisdn);
        skippedAlreadySentPhones.push(msisdn);
        if (lastSentAt) skippedAlreadySentAtByPhone[msisdn] = lastSentAt;
        win?.webContents.send("batch:progress", {
          ts: nowIsoShort(),
          index: i + 1,
          total: recipients.length,
          phone: msisdn,
          status: "skipped",
          error: lastSentAt ? `Already sent on ${lastSentAt}` : "Already sent (this template)",
          sentAt: lastSentAt || ""
        });

        if (i < recipients.length - 1) {
          const ms = delayMsFromPattern(pattern, minSec, maxSec, i);
          const completedDelay = await waitDelayOrBatchCancel(batchJob, ms);
          if (!completedDelay) {
            stopped = true;
            stoppedAtIndex = i + 2;
            emitStopped(stoppedAtIndex);
            break;
          }
        }
        continue;
      }

      try {
        const vars = (varsByPhone && (varsByPhone[rawPhone] || varsByPhone[msisdn])) || {};
        const hintedName = firstNonEmptyString([vars?.name, vars?.Name, vars?.patient_name, vars?.patientName]);
        if (hintedName) {
          rememberRecipientNameForProfile(activeProfileIdForBatch, msisdn, hintedName);
        }
        const preparedMessages = [];
        for (let msgIdx = 0; msgIdx < normalizedBatchMessages.length; msgIdx++) {
          const templateMessage = normalizedBatchMessages[msgIdx];
          const baseText = renderTemplate(templateMessage.text, vars);
          let renderedText = baseText;

          if (aiCfg.enabled && baseText.trim()) {
            try {
              renderedText = await rewriteMessageViaBackend({
                endpoint: aiCfg.endpoint,
                authToken: aiAuthToken,
                message: baseText,
                timeoutMs: aiCfg.timeoutMs
              });
            } catch (aiErr) {
              if (!aiCfg.fallbackToOriginal) {
                throw new Error(`AI rewrite failed: ${String(aiErr?.message || aiErr)}`);
              }
              renderedText = baseText;
              win?.webContents.send("batch:progress", {
                ts: nowIsoShort(),
                index: i + 1,
                total: recipients.length,
                phone: msisdn,
                status: "sending",
                error: `AI fallback (msg ${msgIdx + 1}): ${String(aiErr?.message || aiErr)}`
              });
            }
          }

          const messagePayload = buildTemplateMessagePayload(templateMessage, renderedText);
          if (!messagePayload) continue;
          preparedMessages.push({
            type: templateMessage.type,
            text: renderedText,
            attachment: templateMessage.attachment,
            payload: messagePayload
          });
        }

        if (preparedMessages.length === 0) {
          skipped++;
          win?.webContents.send("batch:progress", {
            ts: nowIsoShort(),
            index: i + 1,
            total: recipients.length,
            phone: msisdn,
            status: "skipped",
            error: "No sendable message after rendering"
          });
          if (i < recipients.length - 1) {
            const ms = delayMsFromPattern(pattern, minSec, maxSec, i);
            const completedDelay = await waitDelayOrBatchCancel(batchJob, ms);
            if (!completedDelay) {
              stopped = true;
              stoppedAtIndex = i + 2;
              emitStopped(stoppedAtIndex);
              break;
            }
          }
          continue;
        }

        if (isBatchJobCancelRequested(batchJob)) {
          stopped = true;
          stoppedAtIndex = i + 1;
          emitStopped(stoppedAtIndex);
          break;
        }

        win?.webContents.send("batch:progress", {
          ts: nowIsoShort(),
          index: i + 1,
          total: recipients.length,
          phone: msisdn,
          status: "sending"
        });

        for (let preparedIdx = 0; preparedIdx < preparedMessages.length; preparedIdx++) {
          const entry = preparedMessages[preparedIdx];
          if (isBatchJobCancelRequested(batchJob)) {
            stopped = true;
            stoppedAtIndex = i + 1;
            emitStopped(stoppedAtIndex);
            break;
          }
          await sendPayloadToMsisdn(msisdn, entry.payload);

          if (preparedIdx < preparedMessages.length - 1) {
            const templateDelayMs = delayMsFromPattern(templatePattern, templateMinSec, templateMaxSec, preparedIdx);
            const completedTemplateDelay = await waitDelayOrBatchCancel(batchJob, templateDelayMs);
            if (!completedTemplateDelay) {
              stopped = true;
              stoppedAtIndex = i + 1;
              emitStopped(stoppedAtIndex);
              break;
            }
          }
        }
        if (stopped) break;

        markSent(templateId, msisdn);
        sent++;

        if (String(templateId || "").startsWith("marketing_")) {
          const recipientBranch =
            cleanString(vars?.branch) || cleanString(vars?.Branch) || cleanString(clinicSession?.user?.Branch);
          const recipientName = cleanString(vars?.name) || cleanString(vars?.Name) || cleanString(hintedName) || "Patient";
          const sentBy = firstNonEmptyString([
            clinicSession?.user?.name,
            clinicSession?.user?.nickname,
            clinicSession?.user?.email
          ]);

          clinicRecordSentMessage(sessionAuthToken, {
            branch: recipientBranch,
            name: recipientName,
            phone: msisdn,
            sent_by: sentBy,
            message: summarizeTemplateMessagesForAudit(preparedMessages)
          }).catch((err) => {
            log.warn({ err, msisdn }, "Failed to record sent marketing message");
          });
        }

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
        const completedDelay = await waitDelayOrBatchCancel(batchJob, ms);
        if (!completedDelay) {
          stopped = true;
          stoppedAtIndex = i + 2;
          emitStopped(stoppedAtIndex);
          break;
        }
      }
    }
  } finally {
    finishBatchJob(batchJob);
  }

  return {
    ok: true,
    sent,
    failed,
    skipped,
    skippedAlreadySentPhones,
    skippedAlreadySentAtByPhone,
    stopped,
    stoppedAtIndex,
    total: recipients.length
  };
});

ipcMain.handle("wa:sendPreparedBatch", async (_evt, payload) => {
  const src = payload && typeof payload === "object" ? payload : {};
  const items = Array.isArray(src.items) ? src.items : [];
  if (items.length === 0) throw new Error("No messages to send");
  const activeProfileIdForPreparedBatch = getActiveProfileId();
  if (!(await ensureConnectedForSend(activeProfileIdForPreparedBatch))) {
    throw new Error("WhatsApp not connected");
  }

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
  if (aiCfg.enabled && !aiAuthToken) {
    throw new Error("AI rewrite requires Authorization token. Please log in first.");
  }

  let sent = 0;
  let failed = 0;
  let skipped = 0;

  const batchJob = beginBatchJob("sendPreparedBatch", items.length);
  let stopped = false;
  let stoppedAtIndex = 0;
  let stopEventSent = false;
  const emitStopped = (indexHint) => {
    if (stopEventSent) return;
    stopEventSent = true;
    win?.webContents.send("batch:progress", {
      ts: nowIsoShort(),
      index: Math.max(1, Math.min(items.length || 1, Number(indexHint || 1))),
      total: items.length,
      phone: "",
      status: "stopped",
      error: "Stopped by user"
    });
  };

  try {
    for (let i = 0; i < items.length; i++) {
      if (isBatchJobCancelRequested(batchJob)) {
        stopped = true;
        stoppedAtIndex = i + 1;
        emitStopped(stoppedAtIndex);
        break;
      }

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

      if (rowName) {
        rememberRecipientNameForProfile(activeProfileIdForPreparedBatch, msisdn, rowName);
      }

      try {
        let text = baseText;
        if (aiCfg.enabled) {
          try {
            text = await rewriteMessageViaBackend({
              endpoint: aiCfg.endpoint,
              authToken: aiAuthToken,
              message: baseText,
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

        if (isBatchJobCancelRequested(batchJob)) {
          stopped = true;
          stoppedAtIndex = i + 1;
          emitStopped(stoppedAtIndex);
          break;
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
        const completedDelay = await waitDelayOrBatchCancel(batchJob, ms);
        if (!completedDelay) {
          stopped = true;
          stoppedAtIndex = i + 2;
          emitStopped(stoppedAtIndex);
          break;
        }
      }
    }
  } finally {
    finishBatchJob(batchJob);
  }

  return { ok: true, sent, failed, skipped, stopped, stoppedAtIndex, total: items.length };
});

ipcMain.handle("wa:stopBatch", async () => {
  return requestStopActiveBatchJob();
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
  schedulePersistWaChatCache();

  startGoBackend(); // START GO SIDECAR

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

  killGoBackend(); // KILL GO SIDECAR
});

app.on("window-all-closed", () => {
  if (process.platform !== "darwin") app.quit();
});



