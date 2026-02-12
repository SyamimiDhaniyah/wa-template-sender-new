
const TIMEZONE = "Asia/Kuala_Lumpur";
const AI_VARIATION_PROMPT =
  "Rewrite this WhatsApp clinic message naturally for Malaysian audience, keep meaning and all facts unchanged, keep it polite and concise: {message}";
const DEFAULT_CLINIC_SETTINGS = { timezone: TIMEZONE, gapMinSec: 7, gapMaxSec: 45, marketingMonthsAgoDefault: 6 };
const DEFAULT_APPOINTMENT_TEMPLATES = {
  remindAppointment: { bahasa: "", english: "" },
  followUp: { bahasa: "", english: "" },
  requestReview: { bahasa: "", english: "" }
};

const MARKETING_PLACEHOLDERS = [
  { token: "{name}", key: "name", description: "Patient name from recipient list." },
  { token: "{branch}", key: "branch", description: "Branch selected in Marketing tab." },
  { token: "{my_branch}", key: "my_branch", description: "Your login branch from account profile." },
  { token: "{date}", key: "date", description: "Recipient appointment date, or today if unavailable." },
  { token: "{day}", key: "day", description: "Day name from appointment date, or today." },
  { token: "{time}", key: "time", description: "Recipient appointment time if available." },
  { token: "{dentist}", key: "dentist", description: "Dentist name from recipient row if available." },
  { token: "{phone}", key: "phone", description: "Normalized phone number of recipient." }
];

const WA_EMOJI_GROUPS = [
  {
    id: "smileys",
    icon: "😀",
    label: "Smileys",
    emojis:
      "😀 😃 😄 😁 😆 😅 😂 🤣 😊 😇 🙂 🙃 😉 😌 😍 🥰 😘 😗 😙 😚 😋 😛 😝 😜 🤪 🤨 🧐 🤓 😎 🥳 😏 😒 😞 😔 😟 😕 🙁 ☹️ 😣 😖 😫 😩 🥺 😢 😭 😤 😠 😡 🤬 😱 😨 😰 😥 😓 🤗 🤔 🫡 🤭 🤫 🤥 😶 🫠 😐 😑 😬 🙄 😯 😦 😧 😮 😲 🥱 😴 🤤 😪 😵 🤐 🥴 🤢 🤮 🤧 😷 🤒 🤕".split(
        " "
      )
  },
  {
    id: "people",
    icon: "👋",
    label: "People",
    emojis:
      "👋 🤚 🖐️ ✋ 🖖 🫶 🙌 🤲 🤝 👍 👎 👊 ✊ 🤛 🤜 👏 🙏 ✍️ 💪 🦾 🦿 🦵 🦶 👂 🦻 👃 🧠 🫀 🫁 🦷 👀 👁️ 👅 👄 🫦 👶 🧒 👦 👧 🧑 👱 👨 👩 🧔 👴 👵 🙍 🙎 🙅 🙆 💁 🙋 🧏 🙇 🤦 🤷 👮 🕵️ 💂 🥷 👷 🤴 👸 👳 👲 🧕 🤵 👰 🤰 🤱 👼 🎅 🤶 🧑‍⚕️ 🧑‍💻 🧑‍🏫".split(
        " "
      )
  },
  {
    id: "animals",
    icon: "🐶",
    label: "Animals",
    emojis:
      "🐶 🐱 🐭 🐹 🐰 🦊 🐻 🐼 🐻‍❄️ 🐨 🐯 🦁 🐮 🐷 🐸 🐵 🙈 🙉 🙊 🐔 🐧 🐦 🐤 🦆 🦅 🦉 🦇 🐺 🐗 🐴 🦄 🐝 🪲 🐛 🦋 🐌 🐞 🐜 🕷️ 🦂 🐢 🐍 🦎 🐙 🦑 🦐 🦀 🐠 🐟 🐬 🐳 🐋 🦈 🐊 🐅 🐆 🦓 🦍 🦧 🐘 🦛 🦏 🐪 🦒 🦬 🐃 🐂 🐄 🐎 🐖 🐏 🐑 🦙 🐐 🦌 🐕 🐈 🐓 🦃 🦤 🦢 🕊️ 🐇 🦝 🦨 🦡 🦫".split(
        " "
      )
  },
  {
    id: "food",
    icon: "🍔",
    label: "Food",
    emojis:
      "🍏 🍎 🍐 🍊 🍋 🍌 🍉 🍇 🍓 🫐 🍈 🍒 🍑 🥭 🍍 🥥 🥝 🍅 🫒 🥑 🍆 🥔 🥕 🌽 🌶️ 🫑 🥒 🥬 🥦 🧄 🧅 🍄 🥜 🫘 🌰 🍞 🥐 🥖 🫓 🥨 🧀 🥚 🍳 🧈 🥞 🧇 🥓 🥩 🍗 🍖 🌭 🍔 🍟 🍕 🫔 🌮 🌯 🥙 🧆 🥪 🥫 🍝 🍜 🍲 🍛 🍣 🍱 🥟 🦪 🍤 🍙 🍚 🍘 🍥 🥠 🥮 🍢 🍡 🍧 🍨 🍦 🥧 🧁 🍰 🎂 🍮 🍭 🍬 🍫 🍿 🍩 🍪 🥤 🧋 ☕ 🍵 🧃 🥛 🍺 🍻 🍷 🥂 🥃 🍸 🍹 🍾 🧊".split(
        " "
      )
  },
  {
    id: "travel",
    icon: "🚗",
    label: "Travel",
    emojis:
      "🚗 🚕 🚙 🚌 🚎 🏎️ 🚓 🚑 🚒 🚐 🛻 🚚 🚛 🚜 🛵 🚲 🛴 🛹 🛼 🚁 ✈️ 🛩️ 🛫 🛬 🚀 🛸 🚢 ⛵ 🚤 🛥️ 🚂 🚆 🚇 🚊 🚉 🚞 🚋 🗺️ 🧭 🗽 🗼 🏰 🏯 🏟️ 🎡 🎢 🏖️ 🏝️ 🏜️ 🌋 ⛰️ 🏔️ 🏕️ 🛖 🏠 🏡 🏢 🏣 🏥 🏦 🏨 🏩 💒 ⛪ 🕌 🕍 🛕 🕋 ⛲ 🌁 🌃 🌆 🌇 🌉 🌌 🎑 🏙️".split(
        " "
      )
  },
  {
    id: "activity",
    icon: "⚽",
    label: "Activity",
    emojis:
      "⚽ 🏀 🏈 ⚾ 🥎 🎾 🏐 🏉 🎱 🏓 🏸 🏒 🏑 🥍 🏏 🥅 ⛳ 🏹 🎣 🤿 🥊 🥋 🎽 🛹 🛼 🛷 ⛸️ 🥌 🎿 ⛷️ 🏂 🪂 🏋️ 🤼 🤸 ⛹️ 🤺 🤾 🏌️ 🧘 🏄 🏊 🤽 🚣 🧗 🚵 🚴 🏆 🥇 🥈 🥉 🏅 🎖️ 🏵️ 🎗️ 🎫 🎟️ 🎪 🤹 🎭 🎨 🎬 🎤 🎧 🎼 🎹 🥁 🪘 🎷 🎺 🪗 🎸 🪕 🎻 🎲 ♟️ 🎯 🎳 🎮 🕹️ 🧩".split(
        " "
      )
  },
  {
    id: "objects",
    icon: "💡",
    label: "Objects",
    emojis:
      "⌚ 📱 📲 💻 ⌨️ 🖥️ 🖨️ 🖱️ 💽 💾 💿 📀 📷 📸 📹 🎥 📽️ 🎞️ 📞 ☎️ 📟 📠 📺 📻 🎙️ ⏱️ ⏲️ ⏰ 🕰️ ⌛ ⏳ 📡 🔋 🪫 🔌 💡 🔦 🕯️ 🪔 🧯 🛢️ 💸 💵 💴 💶 💷 🪙 💰 💳 💎 ⚖️ 🪜 🧰 🔧 🔨 ⚒️ 🛠️ ⛏️ 🪓 🔩 ⚙️ 🧱 ⛓️ 🧲 🔫 💣 🧨 🔪 🗡️ ⚔️ 🛡️ 🚬 ⚰️ 🪦 ⚱️ 🧿 🔮 📿 🧸 🪆 🪄".split(
        " "
      )
  },
  {
    id: "symbols",
    icon: "❤️",
    label: "Symbols",
    emojis:
      "❤️ 🧡 💛 💚 💙 💜 🖤 🤍 🤎 💔 ❣️ 💕 💞 💓 💗 💖 💘 💝 💟 ☮️ ✝️ ☪️ 🕉️ ☸️ ✡️ 🔯 🕎 ☯️ ☦️ 🛐 ⛎ ♈ ♉ ♊ ♋ ♌ ♍ ♎ ♏ ♐ ♑ ♒ ♓ 🆔 ⚛️ 🉑 ☢️ ☣️ 📴 📳 🈶 🈚 🈸 🈺 🈷️ ✴️ 🆚 🉐 ㊙️ ㊗️ 🈴 🈵 🈹 🈲 🅰️ 🅱️ 🆎 🆑 🅾️ 🆘 ❌ ⭕ 🛑 ⛔ 📛 🚫 💯 💢 ♨️ 🚷 🚯 🚳 🚱 🔞 📵 🚭 ❗ ❕ ❓ ❔ ‼️ ⁉️ 🔅 🔆 ⚠️ 🚸 🔱 ⚜️ ♻️ ✅ 🈯 💹 ❇️ ✳️ ❎ 🌐 💠 🌀 💤 🏧 🚾 ♿ 🅿️ 🛗 🈳 🈂️ 🛂 🛃 🛄 🛅 🚹 🚺 🚻 🚼 🚰 🚮 🎦 🛜 📶 🈁 🔣 ℹ️ 🔤 🔡 🔠 🆖 🆗 🆙 🆒 🆕 🆓 0️⃣ 1️⃣ 2️⃣ 3️⃣ 4️⃣ 5️⃣ 6️⃣ 7️⃣ 8️⃣ 9️⃣ 🔟".split(
        " "
      )
  }
];

const state = {
  session: { authToken: "", user: {} },
  settings: { ...DEFAULT_CLINIC_SETTINGS },
  templates: [],
  appointmentTemplates: { ...DEFAULT_APPOINTMENT_TEMPLATES },
  branches: [],
  appointments: [],
  selectedAppointmentIds: new Set(),
  marketingRecipients: [],
  waConnected: false,
  waConnecting: false,
  waConnToggleBusy: false,
  waQrDataUrl: "",
  waPairingCode: "",
  activeTab: "whatsapp",
  waChats: [],
  waActiveChatJid: "",
  waExplicitOpenChatJid: "",
  waMessages: [],
  waChatSearch: "",
  waPendingAttachments: [],
  waComposerSending: false,
  waEmojiPickerOpen: false,
  waEmojiCategoryId: WA_EMOJI_GROUPS[0]?.id || "smileys",
  waDropDepth: 0,
  waSyncTimer: null,
  waLoadingChats: false,
  waLoadingMessages: false,
  waRefreshQueued: false,
  waForceHistoryRefreshOnConnected: false,
  waPresenceByChat: {},
  waPresenceExpiryTimers: {},
  waTypingPauseTimer: null,
  waTypingHeartbeatTimer: null,
  waTypingChatJid: "",
  waTypingActive: false,
  waChatsReqSeq: 0,
  waMessagesReqSeq: 0,
  profiles: [],
  activeProfileId: null,
  settingsProfileId: "",
  activityRows: [],
  currentTemplateId: null,
  confirmResolver: null,
  templateBodyCaretPos: 0
};

function el(id) {
  return document.getElementById(id);
}

function escapeHtml(text) {
  return String(text || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function toast(title, body) {
  const wrap = el("toastWrap");
  if (!wrap) return;
  const node = document.createElement("div");
  node.className = "toast";
  node.innerHTML = `<div class="toastTitle">${escapeHtml(title)}</div><div class="toastBody">${escapeHtml(body || "")}</div>`;
  wrap.appendChild(node);
  setTimeout(() => {
    node.style.opacity = "0";
    node.style.transition = "opacity 180ms ease";
    setTimeout(() => node.remove(), 220);
  }, 2600);
}

function toInt(v, fallback = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? Math.round(n) : fallback;
}

function clamp(v, min, max, fallback) {
  const n = Number(v);
  if (!Number.isFinite(n)) return fallback;
  const r = Math.round(n);
  return Math.min(max, Math.max(min, r));
}

function normalizePhone(input) {
  let s = String(input || "").trim();
  s = s.replace(/\s+/g, "").replace(/-/g, "");
  if (s.startsWith("+")) s = s.slice(1);
  s = s.replace(/\D/g, "");
  if (s.startsWith("01")) s = `6${s}`;
  if (s.startsWith("0") && !s.startsWith("01")) s = `60${s.slice(1)}`;
  return s;
}

function isValidPhone(input) {
  return /^\d{8,}$/.test(normalizePhone(input));
}

function templateSnippet(text) {
  const s = String(text || "").replace(/\s+/g, " ").trim();
  return s.length > 90 ? `${s.slice(0, 90)}...` : s;
}

function renderTemplate(template, vars) {
  return String(template || "").replace(/\{(\w+)\}/g, (_m, key) => {
    const value = vars && Object.prototype.hasOwnProperty.call(vars, key) ? vars[key] : "";
    return String(value ?? "");
  });
}

function extractTemplateVariables(body) {
  const set = new Set();
  const re = /\{(\w+)\}/g;
  const text = String(body || "");
  let m = null;
  while ((m = re.exec(text))) set.add(String(m[1]));
  return Array.from(set);
}

function getPlaceholderMetaByKey(key) {
  return MARKETING_PLACEHOLDERS.find((x) => x.key === key) || null;
}

function ymdToKlMidnightTs(ymd) {
  const m = String(ymd || "").match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) return 0;
  const y = Number(m[1]);
  const month = Number(m[2]);
  const d = Number(m[3]);
  if (!y || !month || !d) return 0;
  return Date.UTC(y, month - 1, d, 0, 0, 0, 0) - 8 * 60 * 60 * 1000;
}

function getTodayYmdKl() {
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: TIMEZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit"
  }).format(new Date());
}

function formatDateForMessage(ts, lang) {
  const locale = lang === "bahasa" ? "ms-MY" : "en-GB";
  return new Intl.DateTimeFormat(locale, {
    timeZone: TIMEZONE,
    day: "2-digit",
    month: "2-digit",
    year: "numeric"
  }).format(new Date(Number(ts || Date.now())));
}

function formatWeekdayForMessage(ts, lang) {
  const locale = lang === "bahasa" ? "ms-MY" : "en-US";
  return new Intl.DateTimeFormat(locale, { timeZone: TIMEZONE, weekday: "long" }).format(
    new Date(Number(ts || Date.now()))
  );
}

function formatTimeForMessage(ts) {
  return new Intl.DateTimeFormat("en-US", {
    timeZone: TIMEZONE,
    hour: "2-digit",
    minute: "2-digit",
    hour12: true
  })
    .format(new Date(Number(ts || Date.now())))
    .toLowerCase();
}

function formatTimeRange(startTs, endTs) {
  return `${formatTimeForMessage(startTs)} - ${formatTimeForMessage(endTs)}`;
}

function statusPill(status) {
  const s = String(status || "failed");
  return `<span class="statusPill status-${escapeHtml(s)}">${escapeHtml(s)}</span>`;
}

function refreshConnectionControls() {
  const toggleBtn = el("btnConnToggle");
  if (toggleBtn) {
    const actionTitle = state.waConnected ? "Disconnect WhatsApp" : "Connect WhatsApp";
    toggleBtn.title = actionTitle;
    toggleBtn.setAttribute("aria-label", actionTitle);
    toggleBtn.disabled = state.waConnToggleBusy || state.waConnecting;
  }

  const connectBtn = el("btnHandshake");
  if (connectBtn) connectBtn.disabled = state.waConnToggleBusy || state.waConnecting;

  const disconnectBtn = el("btnSoftDisconnect");
  if (disconnectBtn) disconnectBtn.disabled = state.waConnToggleBusy || !state.waConnected;

  updateSettingsProfileControls();
}

function setConnectionBusy(busy) {
  state.waConnToggleBusy = !!busy;
  refreshConnectionControls();
}

function updateConnectProfileSummary() {
  const current = (Array.isArray(state.profiles) ? state.profiles : []).find((x) => x.id === state.activeProfileId) || null;
  const label = current?.name || current?.id || "-";
  const target = el("connectActiveProfileText");
  if (target) target.textContent = `Active profile: ${label}`;
}

function getProfileById(profileId) {
  const id = String(profileId || "").trim();
  if (!id) return null;
  return (Array.isArray(state.profiles) ? state.profiles : []).find((p) => p.id === id) || null;
}

function getProfileLabelById(profileId) {
  const profile = getProfileById(profileId);
  return profile?.name || profile?.id || "-";
}

function getSelectedSettingsProfileId() {
  return String(el("settingProfileSelect")?.value || state.settingsProfileId || "").trim();
}

function updateSettingsProfileControls() {
  const selectedId = getSelectedSettingsProfileId();
  const selected = getProfileById(selectedId);
  const disableManage = !selected || state.waConnToggleBusy || state.waConnecting;
  const disableCreate = state.waConnToggleBusy || state.waConnecting;

  const terminateBtn = el("btnSettingTerminateProfile");
  if (terminateBtn) terminateBtn.disabled = disableManage;

  const deleteBtn = el("btnSettingDeleteProfile");
  if (deleteBtn) deleteBtn.disabled = disableManage;

  const createBtn = el("btnCreateProfileSetting");
  if (createBtn) createBtn.disabled = disableCreate;

  const hint = el("settingProfileHint");
  if (hint) {
    hint.textContent = selected
      ? `Selected: ${selected.name || selected.id}. Terminate removes WhatsApp session auth for this profile only.`
      : "Choose a profile to terminate or delete.";
  }
}

async function syncConnectionStateFromBackend() {
  const status = await window.api.waGetConnectionState();
  setConnectionBadge(
    !!status?.connected,
    status?.text || "Not connected",
    isConnectionStatusConnecting(status?.text, status?.connecting)
  );
}

function setQrPreview(dataUrl) {
  const next = String(dataUrl || "").trim();
  state.waQrDataUrl = next;
  const img = el("qrImg");
  if (!img) return;
  if (next) {
    img.src = next;
  } else {
    img.removeAttribute("src");
  }
}

function setPairingPreview(code) {
  state.waPairingCode = String(code || "").trim();
  const textNode = el("pairingCodeText");
  if (textNode) textNode.textContent = state.waPairingCode || "-";
}

function updateConnectPreviewUi() {
  const method = el("connectMethod")?.value === "pairing" ? "pairing" : "qr";
  const qrImg = el("qrImg");
  const qrEmpty = el("qrEmptyState");
  const pairingBox = el("pairingCodeBox");
  const previewHint = el("connectPreviewHint");

  if (!qrImg || !qrEmpty || !pairingBox) return;

  if (method === "pairing") {
    pairingBox.classList.remove("hidden");
    qrImg.classList.add("hidden");
    qrEmpty.classList.add("hidden");
    if (previewHint) {
      previewHint.textContent = state.waPairingCode
        ? "Enter this code in WhatsApp to finish linking."
        : "Click Connect to request a pairing code.";
    }
    return;
  }

  pairingBox.classList.add("hidden");
  if (state.waQrDataUrl) {
    qrImg.classList.remove("hidden");
    qrEmpty.classList.add("hidden");
    if (previewHint) previewHint.textContent = "Scan this QR code in WhatsApp.";
  } else {
    qrImg.classList.add("hidden");
    qrEmpty.classList.remove("hidden");
    if (previewHint) previewHint.textContent = "Click Connect to generate a QR code.";
  }
}

function clearConnectPreview({ clearPairing = true } = {}) {
  setQrPreview("");
  if (clearPairing) setPairingPreview("");
  updateConnectPreviewUi();
}

function isConnectionStatusConnecting(statusText, connectingFlag) {
  if (connectingFlag === true) return true;
  const text = String(statusText || "").toLowerCase();
  return text.includes("connecting");
}

function setConnectionBadge(connected, text, connecting = false) {
  state.waConnected = !!connected;
  state.waConnecting = !connected && !!connecting;
  const dot = el("connDot");
  if (dot) {
    dot.classList.remove("online", "offline");
    dot.classList.add(connected ? "online" : "offline");
  }
  const msg = text || (connected ? "Connected" : "Not connected");
  el("connText").textContent = msg;
  const waStatusText = el("waStatusText");
  if (waStatusText) waStatusText.textContent = msg;
  refreshConnectionControls();
}

function setActiveTab(tabName) {
  const tab = String(tabName || "whatsapp");
  if (tab !== "whatsapp") {
    stopWaOutgoingTyping({ sendPaused: true });
    closeWaEmojiPicker({ restoreFocus: false });
  }
  state.activeTab = tab;
  document.querySelectorAll(".tabBtn").forEach((btn) => btn.classList.toggle("active", btn.dataset.tab === tab));
  document.querySelectorAll(".tabPanel").forEach((panel) => panel.classList.toggle("hidden", panel.id !== `tab-${tab}`));
  const wrap = document.querySelector(".tabWrap");
  if (wrap) wrap.classList.toggle("is-whatsapp", tab === "whatsapp");
}

function normalizeTemplate(raw, idx) {
  const src = raw && typeof raw === "object" ? raw : {};
  const body = String(src.body || "");
  const vars = new Set(Array.isArray(src.variables) ? src.variables.map((v) => String(v)) : []);
  for (const key of extractTemplateVariables(body)) vars.add(key);
  return {
    id: String(src.id || `t_${idx + 1}`),
    name: String(src.name || "Untitled"),
    body,
    variables: Array.from(vars),
    sendPolicy: src.sendPolicy === "multiple" ? "multiple" : "once"
  };
}

function getSelectedMarketingTemplate() {
  return state.templates.find((t) => t.id === state.currentTemplateId) || null;
}

function getBranchByName(branchName) {
  const key = String(branchName || "").trim().toLowerCase();
  if (!key) return null;
  return state.branches.find((b) => String(b.label || "").trim().toLowerCase() === key) || null;
}

function getGreetingName() {
  const user = state.session.user || {};
  return String(user.nickname || user.name || user.email || "Staff");
}

function updateHeaderGreeting() {
  const user = state.session.user || {};
  const role = String(user.Role || "").trim();
  const branch = String(user.Branch || "").trim();
  el("helloText").textContent = `Hello, ${getGreetingName()}`;
  el("helloMeta").textContent = [branch, role, TIMEZONE].filter(Boolean).join(" | ");
}

function formatWaTimeShort(ts) {
  const n = Number(ts || 0);
  if (!n) return "";
  return new Intl.DateTimeFormat("en-GB", {
    hour: "2-digit",
    minute: "2-digit"
  }).format(new Date(n));
}

function formatWaChatTime(ts) {
  const n = Number(ts || 0);
  if (!n) return "";
  const dt = new Date(n);
  const now = new Date();
  const sameDay = dt.toDateString() === now.toDateString();
  if (sameDay) return formatWaTimeShort(n);

  const deltaDays = Math.floor((now.getTime() - dt.getTime()) / (24 * 60 * 60 * 1000));
  if (deltaDays >= 0 && deltaDays < 6) {
    return new Intl.DateTimeFormat("en-US", { weekday: "short" }).format(dt);
  }
  return new Intl.DateTimeFormat("en-GB", { day: "2-digit", month: "2-digit" }).format(dt);
}

function formatWaBytes(bytesRaw) {
  const bytes = Number(bytesRaw || 0);
  if (!Number.isFinite(bytes) || bytes <= 0) return "";
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function waChatInitial(chat) {
  const t = String(chat?.title || "").trim();
  return t ? t.slice(0, 1).toUpperCase() : "#";
}

function getActiveWaChat() {
  return state.waChats.find((x) => x.jid === state.waActiveChatJid) || null;
}

function canMarkReadForChat(chatJid) {
  const jid = String(chatJid || "");
  if (!jid) return false;
  if (state.activeTab !== "whatsapp") return false;
  return jid === String(state.waActiveChatJid || "") && jid === String(state.waExplicitOpenChatJid || "");
}

function normalizeWaJid(input) {
  return String(input || "")
    .trim()
    .toLowerCase();
}

function waDisplayNameFromJid(jid) {
  const raw = String(jid || "").trim();
  if (!raw) return "Someone";
  const user = raw.split("@")[0] || raw;
  if (!user) return "Someone";
  return user;
}

function clearWaPresenceExpiryTimer(timerKey) {
  const key = String(timerKey || "");
  if (!key) return;
  const timer = state.waPresenceExpiryTimers[key];
  if (timer) clearTimeout(timer);
  delete state.waPresenceExpiryTimers[key];
}

function removeWaPresenceParticipant(chatJid, participantJid, options = {}) {
  const opts = options && typeof options === "object" ? options : {};
  const chatKey = normalizeWaJid(chatJid);
  const participantKey = normalizeWaJid(participantJid);
  if (!chatKey || !participantKey) return;

  const map = state.waPresenceByChat[chatKey];
  if (!map || typeof map !== "object") return;

  const timerKey = `${chatKey}|${participantKey}`;
  clearWaPresenceExpiryTimer(timerKey);
  if (!Object.prototype.hasOwnProperty.call(map, participantKey)) return;

  delete map[participantKey];
  if (Object.keys(map).length === 0) delete state.waPresenceByChat[chatKey];
  if (opts.render !== false && chatKey === normalizeWaJid(state.waActiveChatJid)) {
    renderWaConversationHead();
  }
}

function clearWaPresenceForChat(chatJid, options = {}) {
  const opts = options && typeof options === "object" ? options : {};
  const chatKey = normalizeWaJid(chatJid);
  if (!chatKey) return;
  const map = state.waPresenceByChat[chatKey];
  if (!map || typeof map !== "object") return;
  for (const participantKey of Object.keys(map)) {
    clearWaPresenceExpiryTimer(`${chatKey}|${participantKey}`);
  }
  delete state.waPresenceByChat[chatKey];
  if (opts.render !== false && chatKey === normalizeWaJid(state.waActiveChatJid)) {
    renderWaConversationHead();
  }
}

function clearAllWaPresenceState(options = {}) {
  const opts = options && typeof options === "object" ? options : {};
  for (const timerKey of Object.keys(state.waPresenceExpiryTimers || {})) {
    clearWaPresenceExpiryTimer(timerKey);
  }
  state.waPresenceByChat = {};
  if (opts.render !== false) renderWaConversationHead();
}

function resolveWaPresenceSenderName(chatJid, participantJid, fallbackName) {
  const fallback = String(fallbackName || "").trim();
  if (fallback) return fallback;

  const participantKey = normalizeWaJid(participantJid);
  const chatKey = normalizeWaJid(chatJid);
  if (participantKey && chatKey && participantKey === chatKey) {
    const activeChat = state.waChats.find((x) => normalizeWaJid(x?.jid) === chatKey);
    const title = String(activeChat?.title || "").trim();
    if (title) return title;
  }

  for (let i = state.waMessages.length - 1; i >= 0; i--) {
    const msg = state.waMessages[i];
    const participantFromMsg = normalizeWaJid(msg?.key?.participant || "");
    if (participantFromMsg && participantFromMsg === participantKey) {
      const sender = String(msg?.senderName || "").trim();
      if (sender && sender.toLowerCase() !== "you") return sender;
    }
  }

  const byChat = state.waChats.find((x) => normalizeWaJid(x?.jid) === participantKey);
  const chatTitle = String(byChat?.title || "").trim();
  if (chatTitle) return chatTitle;

  return waDisplayNameFromJid(participantJid);
}

function setWaPresenceParticipant(chatJid, participantJid, entry, ttlMs = 9000) {
  const chatKey = normalizeWaJid(chatJid);
  const participantKey = normalizeWaJid(participantJid);
  if (!chatKey || !participantKey) return;

  const bucket =
    state.waPresenceByChat[chatKey] && typeof state.waPresenceByChat[chatKey] === "object"
      ? state.waPresenceByChat[chatKey]
      : {};
  state.waPresenceByChat[chatKey] = bucket;
  bucket[participantKey] = {
    participantJid: participantKey,
    presenceType: String(entry?.presenceType || "").toLowerCase(),
    name: String(entry?.name || "").trim() || waDisplayNameFromJid(participantKey),
    updatedAt: Number(entry?.updatedAt || Date.now()) || Date.now()
  };

  const timerKey = `${chatKey}|${participantKey}`;
  clearWaPresenceExpiryTimer(timerKey);
  state.waPresenceExpiryTimers[timerKey] = setTimeout(() => {
    removeWaPresenceParticipant(chatKey, participantKey, { render: true });
  }, Math.max(1000, Number(ttlMs || 9000)));

  if (chatKey === normalizeWaJid(state.waActiveChatJid)) {
    renderWaConversationHead();
  }
}

function getWaTypingEntriesForChat(chatJid) {
  const chatKey = normalizeWaJid(chatJid);
  if (!chatKey) return [];
  const map = state.waPresenceByChat[chatKey];
  if (!map || typeof map !== "object") return [];
  return Object.values(map)
    .filter((x) => x && typeof x === "object" && ["composing", "recording"].includes(String(x.presenceType || "")))
    .sort((a, b) => Number(b?.updatedAt || 0) - Number(a?.updatedAt || 0));
}

function formatWaTypingMeta(activeChat, typingEntries) {
  const list = Array.isArray(typingEntries) ? typingEntries : [];
  if (list.length === 0) return "";

  const first = list[0];
  if (!activeChat?.isGroup) {
    return first.presenceType === "recording" ? "recording audio..." : "typing...";
  }

  if (list.length === 1) {
    return first.presenceType === "recording" ? `${first.name} is recording audio...` : `${first.name} is typing...`;
  }

  const names = list
    .slice(0, 2)
    .map((x) => String(x?.name || "").trim())
    .filter(Boolean);
  const moreCount = Math.max(0, list.length - names.length);
  const base = names.join(", ");
  return moreCount > 0 ? `${base} +${moreCount} typing...` : `${base} typing...`;
}

function applyWaPresenceUpdate(payload) {
  const src = payload && typeof payload === "object" ? payload : {};
  if (src.profileId && state.activeProfileId && src.profileId !== state.activeProfileId) return;

  const chatJid = String(src.id || src.chatJid || "").trim();
  if (!chatJid) return;

  const presences = src.presences && typeof src.presences === "object" ? src.presences : {};
  for (const [participantJidRaw, presenceRaw] of Object.entries(presences)) {
    const participantJid = String(participantJidRaw || "").trim();
    if (!participantJid) continue;
    const presenceType = String(presenceRaw?.lastKnownPresence || "")
      .trim()
      .toLowerCase();
    if (presenceType === "composing" || presenceType === "recording") {
      const senderName = resolveWaPresenceSenderName(chatJid, participantJid, presenceRaw?.senderName || "");
      setWaPresenceParticipant(chatJid, participantJid, {
        presenceType,
        name: senderName,
        updatedAt: Date.now()
      });
    } else {
      removeWaPresenceParticipant(chatJid, participantJid, { render: true });
    }
  }
}

function clearWaTypingPauseTimer() {
  if (!state.waTypingPauseTimer) return;
  clearTimeout(state.waTypingPauseTimer);
  state.waTypingPauseTimer = null;
}

function clearWaTypingHeartbeatTimer() {
  if (!state.waTypingHeartbeatTimer) return;
  clearInterval(state.waTypingHeartbeatTimer);
  state.waTypingHeartbeatTimer = null;
}

async function sendWaChatPresence(type, chatJid) {
  const presenceType = String(type || "").trim().toLowerCase();
  const jid = String(chatJid || "").trim();
  if (!presenceType || !jid) return;
  try {
    await window.api.waSendPresence({
      chatJid: jid,
      type: presenceType
    });
  } catch {
    // ignore transient presence send failures
  }
}

function stopWaOutgoingTyping(options = {}) {
  const opts = options && typeof options === "object" ? options : {};
  const chatJid = String(state.waTypingChatJid || "").trim();
  const wasActive = !!state.waTypingActive && !!chatJid;

  clearWaTypingPauseTimer();
  clearWaTypingHeartbeatTimer();
  state.waTypingActive = false;
  state.waTypingChatJid = "";

  if (opts.sendPaused !== false && wasActive) {
    sendWaChatPresence("paused", chatJid).catch(() => {});
  }
}

function scheduleWaOutgoingTypingPause(chatJid) {
  clearWaTypingPauseTimer();
  state.waTypingPauseTimer = setTimeout(() => {
    if (normalizeWaJid(state.waTypingChatJid) !== normalizeWaJid(chatJid)) return;
    stopWaOutgoingTyping({ sendPaused: true });
  }, 1400);
}

function startWaOutgoingTyping(chatJid) {
  const jid = String(chatJid || "").trim();
  if (!jid || !state.waConnected) return;
  const changedChat = normalizeWaJid(state.waTypingChatJid) !== normalizeWaJid(jid);
  if (changedChat) {
    stopWaOutgoingTyping({ sendPaused: true });
  }

  state.waTypingChatJid = jid;
  if (!state.waTypingActive) {
    state.waTypingActive = true;
    sendWaChatPresence("composing", jid).catch(() => {});
    clearWaTypingHeartbeatTimer();
    state.waTypingHeartbeatTimer = setInterval(() => {
      if (!state.waTypingActive) return;
      if (!state.waTypingChatJid) return;
      sendWaChatPresence("composing", state.waTypingChatJid).catch(() => {});
    }, 7000);
  }
  scheduleWaOutgoingTypingPause(jid);
}

function handleWaComposerInputTyping() {
  const chatJid = String(state.waActiveChatJid || "").trim();
  if (!chatJid) return;
  const text = String(el("waComposerInput").value || "");
  if (!text.trim()) {
    stopWaOutgoingTyping({ sendPaused: true });
    return;
  }
  startWaOutgoingTyping(chatJid);
}

function waAttachmentKindFromMimeOrPath(mimeType, filePath) {
  const mime = String(mimeType || "")
    .trim()
    .toLowerCase();
  if (mime.startsWith("image/")) return "image";
  if (mime.startsWith("video/")) return "video";
  if (mime.startsWith("audio/")) return "audio";

  const ext = String(filePath || "")
    .trim()
    .toLowerCase()
    .match(/\.([a-z0-9]+)$/);
  const value = ext ? `.${ext[1]}` : "";
  if ([".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp"].includes(value)) return "image";
  if ([".mp4", ".mov", ".avi", ".mkv"].includes(value)) return "video";
  if ([".mp3", ".ogg", ".wav", ".m4a"].includes(value)) return "audio";
  return "document";
}

function normalizeWaAttachment(raw) {
  const src = raw && typeof raw === "object" ? raw : {};
  const filePath = String(src.path || src.filePath || "").trim();
  if (!filePath) return null;

  const fileNameRaw = String(src.fileName || src.name || "").trim();
  const fileName =
    fileNameRaw ||
    String(filePath)
      .split(/[\\/]/)
      .filter(Boolean)
      .pop() ||
    "Attachment";
  const mimeType = String(src.mimeType || src.type || "").trim();
  const size = Math.max(0, Number(src.size || 0) || 0);
  const kind = waAttachmentKindFromMimeOrPath(mimeType, filePath);

  return {
    path: filePath,
    fileName,
    mimeType,
    kind,
    size
  };
}

function normalizeWaAttachmentList(list) {
  const rows = Array.isArray(list) ? list : [];
  const out = [];
  const seen = new Set();
  for (const item of rows) {
    const normalized = normalizeWaAttachment(item);
    if (!normalized) continue;
    const key = normalized.path.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(normalized);
  }
  return out;
}

function waPathFromFileUri(uriText) {
  const raw = String(uriText || "").trim();
  if (!raw || !raw.toLowerCase().startsWith("file://")) return "";
  try {
    const url = new URL(raw);
    let p = decodeURIComponent(url.pathname || "");
    if (/^\/[A-Za-z]:\//.test(p)) p = p.slice(1);
    if (url.host) p = `//${url.host}${p}`;
    return p;
  } catch {
    return "";
  }
}

function waParseDroppedPathCandidates(dataTransfer) {
  const dt = dataTransfer || null;
  if (!dt || typeof dt.getData !== "function") return [];

  const out = [];
  const seen = new Set();
  const pushPath = (value) => {
    const raw = String(value || "").trim().replace(/^"(.*)"$/, "$1");
    if (!raw) return;
    const normalized = raw.replaceAll("/", "\\");
    const key = normalized.toLowerCase();
    if (seen.has(key)) return;
    seen.add(key);
    out.push(normalized);
  };

  const uriList = String(dt.getData("text/uri-list") || "");
  for (const lineRaw of uriList.split(/\r?\n/)) {
    const line = String(lineRaw || "").trim();
    if (!line || line.startsWith("#")) continue;
    if (line.toLowerCase().startsWith("file://")) {
      const fromUri = waPathFromFileUri(line);
      if (fromUri) pushPath(fromUri);
      continue;
    }
    pushPath(line);
  }

  const plain = String(dt.getData("text/plain") || "");
  for (const lineRaw of plain.split(/\r?\n/)) {
    const line = String(lineRaw || "").trim();
    if (!line) continue;
    if (line.toLowerCase().startsWith("file://")) {
      const fromUri = waPathFromFileUri(line);
      if (fromUri) pushPath(fromUri);
      continue;
    }
    if (/^[A-Za-z]:\\/.test(line) || /^\\\\/.test(line) || /^\//.test(line)) {
      pushPath(line);
    }
  }

  return out;
}

function waResolveDroppedFilePath(file, fallbackPath) {
  const directPath = String(file?.path || "").trim();
  if (directPath) return directPath;

  try {
    if (window.api && typeof window.api.waGetPathForDroppedFile === "function") {
      const resolved = String(window.api.waGetPathForDroppedFile(file) || "").trim();
      if (resolved) return resolved;
    }
  } catch {
    // ignore path resolution failure and continue with fallback
  }

  return String(fallbackPath || "").trim();
}

function waDroppedFilesToAttachments(fileList, fallbackPaths = []) {
  const files = Array.from(fileList || []);
  const fromFiles = normalizeWaAttachmentList(
    files.map((file, idx) => ({
      path: waResolveDroppedFilePath(file, fallbackPaths[idx] || ""),
      fileName: file?.name || "",
      mimeType: file?.type || "",
      size: Number(file?.size || 0) || 0
    }))
  );
  if (fromFiles.length > 0) return fromFiles;

  return normalizeWaAttachmentList(
    (Array.isArray(fallbackPaths) ? fallbackPaths : []).map((filePath) => ({
      path: filePath,
      fileName: "",
      mimeType: "",
      size: 0
    }))
  );
}

function waAttachmentSupportsCaption(attachment) {
  const kind = String(attachment?.kind || "").toLowerCase();
  return kind !== "audio";
}

function removeWaPendingAttachmentAt(indexToRemove) {
  const next = (state.waPendingAttachments || []).filter((_item, idx) => idx !== indexToRemove);
  setWaPendingAttachments(next);
}

function renderWaAttachmentRow() {
  const row = el("waAttachmentRow");
  const label = el("waAttachmentMeta");
  const listWrap = el("waAttachmentList");
  const clearBtn = el("btnWaClearAttachment");
  if (!row || !label || !listWrap) return;

  const list = Array.isArray(state.waPendingAttachments) ? state.waPendingAttachments : [];
  if (list.length === 0) {
    row.classList.add("hidden");
    label.textContent = "Attachment";
    listWrap.innerHTML = "";
    if (clearBtn) clearBtn.disabled = state.waComposerSending;
    return;
  }

  const totalSize = list.reduce((sum, item) => sum + Math.max(0, Number(item?.size || 0) || 0), 0);
  const sizeText = formatWaBytes(totalSize);
  label.textContent = sizeText ? `${list.length} file${list.length > 1 ? "s" : ""} | ${sizeText}` : `${list.length} file${
    list.length > 1 ? "s" : ""
  }`;

  listWrap.innerHTML = "";
  for (let i = 0; i < list.length; i++) {
    const item = list[i];
    const chip = document.createElement("div");
    chip.className = "waAttachmentChip";

    const name = document.createElement("span");
    name.className = "waAttachmentChipName";
    const readableName = String(item.fileName || "Attachment").trim() || "Attachment";
    const chipSizeText = formatWaBytes(item.size);
    name.textContent = chipSizeText ? `${readableName} (${chipSizeText})` : readableName;
    name.title = readableName;

    const removeBtn = document.createElement("button");
    removeBtn.type = "button";
    removeBtn.className = "waAttachmentChipRemove";
    removeBtn.textContent = "x";
    removeBtn.title = `Remove ${readableName}`;
    removeBtn.disabled = state.waComposerSending;
    removeBtn.addEventListener("click", () => removeWaPendingAttachmentAt(i));

    chip.append(name, removeBtn);
    listWrap.appendChild(chip);
  }

  if (clearBtn) clearBtn.disabled = state.waComposerSending;
  row.classList.remove("hidden");
}

function setWaPendingAttachments(attachments) {
  state.waPendingAttachments = normalizeWaAttachmentList(attachments);
  renderWaAttachmentRow();
}

function appendWaPendingAttachments(attachments) {
  const merged = [...(state.waPendingAttachments || []), ...(Array.isArray(attachments) ? attachments : [])];
  setWaPendingAttachments(merged);
}

function setWaComposerSending(sending) {
  state.waComposerSending = !!sending;
  const busy = state.waComposerSending;
  const sendBtn = el("btnWaSend");
  const attachBtn = el("btnWaAttach");
  const emojiBtn = el("btnWaEmoji");
  const input = el("waComposerInput");
  if (sendBtn) sendBtn.disabled = busy;
  if (attachBtn) attachBtn.disabled = busy;
  if (emojiBtn) emojiBtn.disabled = busy;
  if (input) input.disabled = busy;
  if (busy) closeWaEmojiPicker({ restoreFocus: false });
  renderWaAttachmentRow();
}

function focusWaComposerInput() {
  const input = el("waComposerInput");
  if (!input || input.disabled) return;
  try {
    input.focus({ preventScroll: true });
  } catch {
    input.focus();
  }
  const len = String(input.value || "").length;
  try {
    input.setSelectionRange(len, len);
  } catch {
    // Ignore browsers/input types that do not support selection ranges.
  }
}

function getWaEmojiCategory() {
  const activeId = String(state.waEmojiCategoryId || "");
  return WA_EMOJI_GROUPS.find((group) => group.id === activeId) || WA_EMOJI_GROUPS[0] || null;
}

function renderWaEmojiPicker() {
  const tabsWrap = el("waEmojiTabs");
  const gridWrap = el("waEmojiGrid");
  if (!tabsWrap || !gridWrap) return;

  const active = getWaEmojiCategory();
  if (!active) {
    tabsWrap.innerHTML = "";
    gridWrap.innerHTML = "";
    return;
  }

  tabsWrap.innerHTML = "";
  for (const group of WA_EMOJI_GROUPS) {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = `waEmojiTab${group.id === active.id ? " active" : ""}`;
    btn.textContent = group.icon || "•";
    btn.title = group.label || group.id;
    btn.setAttribute("aria-label", group.label || group.id);
    btn.dataset.emojiGroup = group.id;
    tabsWrap.appendChild(btn);
  }

  gridWrap.innerHTML = "";
  const frag = document.createDocumentFragment();
  for (const emoji of active.emojis || []) {
    const value = String(emoji || "").trim();
    if (!value) continue;
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "waEmojiBtn";
    btn.textContent = value;
    btn.title = value;
    btn.setAttribute("aria-label", `Insert ${value}`);
    btn.dataset.emojiValue = value;
    frag.appendChild(btn);
  }
  gridWrap.appendChild(frag);
}

function openWaEmojiPicker() {
  const picker = el("waEmojiPicker");
  if (!picker) return;
  renderWaEmojiPicker();
  picker.classList.remove("hidden");
  state.waEmojiPickerOpen = true;
}

function closeWaEmojiPicker(options = {}) {
  const opts = options && typeof options === "object" ? options : {};
  const picker = el("waEmojiPicker");
  if (picker) picker.classList.add("hidden");
  const wasOpen = state.waEmojiPickerOpen;
  state.waEmojiPickerOpen = false;
  if (wasOpen && opts.restoreFocus !== false) focusWaComposerInput();
}

function toggleWaEmojiPicker() {
  if (state.waComposerSending) return;
  if (state.waEmojiPickerOpen) {
    closeWaEmojiPicker({ restoreFocus: true });
    return;
  }
  openWaEmojiPicker();
  focusWaComposerInput();
}

function insertEmojiIntoWaComposer(rawEmoji) {
  const emoji = String(rawEmoji || "").trim();
  if (!emoji) return;
  const input = el("waComposerInput");
  if (!input || input.disabled) return;

  const value = String(input.value || "");
  const start = Number.isFinite(input.selectionStart) ? Number(input.selectionStart) : value.length;
  const end = Number.isFinite(input.selectionEnd) ? Number(input.selectionEnd) : start;
  const safeStart = Math.max(0, Math.min(value.length, start));
  const safeEnd = Math.max(safeStart, Math.min(value.length, end));
  const nextValue = `${value.slice(0, safeStart)}${emoji}${value.slice(safeEnd)}`;

  input.value = nextValue;
  const caret = safeStart + emoji.length;
  try {
    input.setSelectionRange(caret, caret);
  } catch {
    // Ignore selection failures for unsupported platforms.
  }
  input.dispatchEvent(new Event("input", { bubbles: true }));
  focusWaComposerInput();
}

function renderWaChatList() {
  const wrap = el("waChatList");
  if (!wrap) return;
  wrap.innerHTML = "";

  if (state.waChats.length === 0) {
    const empty = document.createElement("div");
    empty.className = "smallText";
    empty.style.padding = "12px";
    empty.textContent = state.waConnected
      ? "No recent chats yet. Open a conversation in WhatsApp first."
      : "Connect WhatsApp to load conversations.";
    wrap.appendChild(empty);
    return;
  }

  for (const chat of state.waChats) {
    const item = document.createElement("div");
    item.className = `waChatItem${chat.jid === state.waActiveChatJid ? " active" : ""}`;

    let avatar = document.createElement(chat.avatarUrl ? "img" : "div");
    avatar.className = "waChatAvatar";
    if (chat.avatarUrl) {
      avatar.src = chat.avatarUrl;
      avatar.alt = chat.title || "Avatar";
      avatar.loading = "lazy";
      avatar.addEventListener("error", () => {
        const fallback = document.createElement("div");
        fallback.className = "waChatAvatar";
        fallback.textContent = waChatInitial(chat);
        try {
          item.replaceChild(fallback, avatar);
          avatar = fallback;
        } catch {
          // ignore replacement race
        }
      });
    } else {
      avatar.textContent = waChatInitial(chat);
    }

    const main = document.createElement("div");
    main.className = "waChatMain";
    const previewPrefix = chat.lastMessageFromMe ? "You: " : "";
    main.innerHTML = `
      <div class="waChatName">${escapeHtml(chat.title || chat.jid)}</div>
      <div class="waChatPreview">${escapeHtml(`${previewPrefix}${chat.preview || ""}` || "(no messages)")}</div>
    `;

    const meta = document.createElement("div");
    meta.className = "waChatMeta";
    meta.innerHTML = `
      <div class="waChatTime">${escapeHtml(formatWaChatTime(chat.lastMessageTimestampMs))}</div>
      ${
        Number(chat.unreadCount || 0) > 0
          ? `<div class="waUnreadBadge">${escapeHtml(String(Math.min(99, Number(chat.unreadCount || 0))))}</div>`
          : ""
      }
    `;

    item.append(avatar, main, meta);
    item.addEventListener("click", async () => {
      try {
        await openWaChat(chat.jid);
      } catch (e) {
        toast("WhatsApp", String(e?.message || e));
      }
    });

    wrap.appendChild(item);
  }
}

function renderWaConversationHead() {
  const titleEl = document.querySelector("#waConversationHead .waHeadTitle");
  const activeChat = getActiveWaChat();
  if (!activeChat) {
    if (titleEl) titleEl.textContent = "WhatsApp";
    el("waHeadMeta").textContent = "Select a conversation";
    return;
  }
  if (titleEl) titleEl.textContent = activeChat.title || activeChat.jid || "WhatsApp";
  const bits = [];
  const typingMeta = formatWaTypingMeta(activeChat, getWaTypingEntriesForChat(activeChat.jid));
  if (typingMeta) bits.push(typingMeta);
  if (activeChat.isGroup) bits.push("Group");
  if (activeChat.unreadCount) bits.push(`${activeChat.unreadCount} unread`);
  bits.push(activeChat.jid);
  el("waHeadMeta").textContent = bits.join(" | ");
}

async function handleWaMediaDownload(msg) {
  try {
    const res = await window.api.waDownloadMedia({
      chatJid: msg.chatJid,
      key: msg.key
    });
    if (!res?.ok && res?.canceled) return;
    if (!res?.ok) throw new Error("Download failed");
    toast("Download", `Saved to ${res.filePath}`);
  } catch (e) {
    toast("Download", String(e?.message || e));
  }
}

function openWaImageLightbox(src, altText) {
  const wrap = el("waImageLightbox");
  const img = el("waImageLightboxImg");
  if (!wrap || !img) return;
  const dataUrl = String(src || "").trim();
  if (!dataUrl) return;
  img.src = dataUrl;
  img.alt = String(altText || "Image preview");
  wrap.classList.remove("hidden");
}

function closeWaImageLightbox() {
  const wrap = el("waImageLightbox");
  const img = el("waImageLightboxImg");
  if (!wrap || !img) return;
  wrap.classList.add("hidden");
  img.removeAttribute("src");
  img.alt = "Image preview";
}

async function openWaImageForMessage(msg) {
  const media = msg && msg.media && typeof msg.media === "object" ? msg.media : null;
  if (!media) return;
  const altText = media.fileName || "Image";
  const thumb = String(media.thumbnailDataUrl || "").trim();

  if (thumb) {
    openWaImageLightbox(thumb, altText);
  }

  try {
    const res = await window.api.waResolveImagePreview({
      chatJid: msg.chatJid,
      key: msg.key
    });
    const fullDataUrl = String(res?.dataUrl || "").trim();
    if (!res?.ok || !fullDataUrl) throw new Error("Image preview unavailable");

    if (!thumb) {
      openWaImageLightbox(fullDataUrl, altText);
      return;
    }

    const wrap = el("waImageLightbox");
    const img = el("waImageLightboxImg");
    if (!wrap || !img || wrap.classList.contains("hidden")) return;
    img.src = fullDataUrl;
  } catch (e) {
    if (!thumb) {
      toast("Image preview", String(e?.message || e));
    }
  }
}

function renderWaMessages(options = {}) {
  const opts = options && typeof options === "object" ? options : {};
  const viewport = el("waMessageViewport");
  if (!viewport) return;
  const prevScrollTop = Number(viewport.scrollTop || 0);
  const prevScrollHeight = Number(viewport.scrollHeight || 0);
  const prevClientHeight = Number(viewport.clientHeight || 0);
  const prevDistanceFromBottom = Math.max(0, prevScrollHeight - (prevScrollTop + prevClientHeight));
  const wasNearBottom = prevDistanceFromBottom <= 120;
  viewport.innerHTML = "";

  if (!state.waActiveChatJid) {
    viewport.innerHTML = '<div class="waEmptyState">Select a conversation to start replying.</div>';
    return;
  }

  if (state.waLoadingMessages) {
    viewport.innerHTML = '<div class="waEmptyState">Loading messages...</div>';
    return;
  }

  if (state.waMessages.length === 0) {
    viewport.innerHTML = '<div class="waEmptyState">No messages in this chat yet.</div>';
    return;
  }

  for (const msg of state.waMessages) {
    const row = document.createElement("div");
    row.className = `waMessageRow${msg.fromMe ? " me" : ""}`;

    const bubble = document.createElement("div");
    bubble.className = "waBubble";
    let hasVisibleContent = false;

    if (!msg.fromMe && msg.senderName) {
      const sender = document.createElement("div");
      sender.className = "waSender";
      sender.textContent = msg.senderName;
      bubble.appendChild(sender);
    }

    if (msg.hasMedia && msg.media) {
      const isImageMedia = String(msg.media.kind || "").toLowerCase() === "image";
      if (msg.media.thumbnailDataUrl) {
        const img = document.createElement("img");
        img.className = "waMediaThumb";
        img.src = msg.media.thumbnailDataUrl;
        img.alt = msg.media.fileName || msg.media.kind || "media";
        if (isImageMedia) {
          img.classList.add("clickable");
          img.addEventListener("click", async () => {
            await openWaImageForMessage(msg);
          });
        }
        bubble.appendChild(img);
      } else if (isImageMedia) {
        const openBtn = document.createElement("button");
        openBtn.className = "waMediaDownloadBtn";
        openBtn.textContent = "Open image";
        openBtn.addEventListener("click", async () => {
          await openWaImageForMessage(msg);
        });
        bubble.appendChild(openBtn);
      }

      const mediaMeta = document.createElement("div");
      mediaMeta.className = "waMediaMeta";
      const mediaText = msg.media.fileName || `[${msg.media.kind || "media"}]`;
      const sizeText = formatWaBytes(msg.media.fileLength);
      const textNode = document.createElement("span");
      textNode.textContent = sizeText ? `${mediaText} (${sizeText})` : mediaText;
      mediaMeta.appendChild(textNode);

      const dlBtn = document.createElement("button");
      dlBtn.className = "waMediaDownloadBtn";
      dlBtn.textContent = "Download";
      dlBtn.addEventListener("click", async () => {
        await handleWaMediaDownload(msg);
      });
      mediaMeta.appendChild(dlBtn);
      bubble.appendChild(mediaMeta);
      hasVisibleContent = true;
    }

    if (msg.text) {
      const text = document.createElement("div");
      text.className = "waMessageText";
      text.textContent = msg.text;
      bubble.appendChild(text);
      hasVisibleContent = true;
    } else {
      const previewText = String(msg.preview || "").trim();
      if (previewText) {
        const preview = document.createElement("div");
        preview.className = "waMessageText";
        preview.textContent = previewText;
        bubble.appendChild(preview);
        hasVisibleContent = true;
      }
    }

    if (!hasVisibleContent) {
      continue;
    }

    const ts = document.createElement("div");
    ts.className = "waMsgTime";
    ts.textContent = msg.optimistic === true ? "sending..." : formatWaTimeShort(msg.timestampMs);
    bubble.appendChild(ts);

    row.appendChild(bubble);
    viewport.appendChild(row);
  }
  if (opts.forceBottom || wasNearBottom) {
    viewport.scrollTop = viewport.scrollHeight;
    return;
  }
  if (prevScrollHeight > 0) {
    viewport.scrollTop = Math.max(0, viewport.scrollHeight - prevDistanceFromBottom);
  }
}

async function refreshWaMessages(options = {}) {
  const opts = options && typeof options === "object" ? options : {};
  const activeChatJid = String(state.waActiveChatJid || "");
  if (!activeChatJid) {
    state.waMessages = [];
    state.waLoadingMessages = false;
    renderWaConversationHead();
    renderWaMessages({ forceBottom: false });
    return;
  }

  const requestId = ++state.waMessagesReqSeq;
  const shouldShowLoading = opts.showLoading !== false && state.waMessages.length === 0;
  if (shouldShowLoading) {
    state.waLoadingMessages = true;
    renderWaConversationHead();
    renderWaMessages({ forceBottom: false });
  } else {
    state.waLoadingMessages = false;
  }

  try {
    const res = await window.api.waGetChatMessages({
      chatJid: activeChatJid,
      limit: 180
    });
    if (requestId !== state.waMessagesReqSeq) return;
    state.waMessages = Array.isArray(res?.messages) ? res.messages : [];
  } finally {
    if (requestId !== state.waMessagesReqSeq) return;
    state.waLoadingMessages = false;
    renderWaConversationHead();
    renderWaMessages({ forceBottom: opts.forceBottom === true });
  }

  const allowMarkRead = opts.markRead !== false && canMarkReadForChat(activeChatJid);
  if (allowMarkRead) {
    try {
      await window.api.waMarkChatRead({ chatJid: activeChatJid });
      if (requestId !== state.waMessagesReqSeq) return;
      state.waChats = state.waChats.map((chat) =>
        chat.jid === activeChatJid
          ? {
              ...chat,
              unreadCount: 0
            }
          : chat
      );
      renderWaChatList();
      renderWaConversationHead();
    } catch {
      // ignore mark-read failure
    }
  }
}

async function openWaChat(chatJid) {
  const next = String(chatJid || "");
  if (!next) return;
  stopWaOutgoingTyping({ sendPaused: true });
  state.waActiveChatJid = next;
  state.waExplicitOpenChatJid = next;
  renderWaChatList();
  renderWaConversationHead();
  await refreshWaMessages({ markRead: true, forceBottom: true, showLoading: true });
}

async function refreshWaChats(options = {}) {
  const opts = options && typeof options === "object" ? options : {};
  if (state.waLoadingChats) {
    state.waRefreshQueued = true;
    return;
  }
  state.waLoadingChats = true;
  const requestId = ++state.waChatsReqSeq;

  try {
    const prevActiveChatJid = state.waActiveChatJid;
    const res = await window.api.waGetRecentChats({
      search: state.waChatSearch || "",
      limit: 220,
      includePhotos: opts.includePhotos !== false,
      ensureHistory: opts.ensureHistory === true,
      forceHistory: opts.forceHistory === true,
      forceNameSync: opts.forceNameSync === true,
      maxPhotoFetch: Number.isFinite(Number(opts.maxPhotoFetch)) ? Number(opts.maxPhotoFetch) : 35,
      minMinutesBetweenPhotoChecks: Number.isFinite(Number(opts.minMinutesBetweenPhotoChecks))
        ? Number(opts.minMinutesBetweenPhotoChecks)
        : 120
    });
    if (requestId !== state.waChatsReqSeq) return;
    state.waChats = Array.isArray(res?.chats) ? res.chats : [];
    const activeStillExists = state.waChats.some((x) => x.jid === state.waActiveChatJid);
    if (!activeStillExists) {
      if (prevActiveChatJid) clearWaPresenceForChat(prevActiveChatJid, { render: false });
      stopWaOutgoingTyping({ sendPaused: true });
      state.waActiveChatJid = "";
      state.waMessages = [];
    }
    const explicitStillExists = state.waChats.some((x) => x.jid === state.waExplicitOpenChatJid);
    if (!explicitStillExists) {
      state.waExplicitOpenChatJid = "";
    }
  } finally {
    if (requestId !== state.waChatsReqSeq) return;
    state.waLoadingChats = false;
  }

  if (requestId !== state.waChatsReqSeq) return;
  renderWaChatList();
  renderWaConversationHead();

  if (!state.waActiveChatJid) {
    renderWaMessages({ forceBottom: false });
  } else if (opts.refreshMessages !== false) {
    await refreshWaMessages({
      markRead: opts.markRead !== false,
      showLoading: opts.showLoadingMessages !== false
    });
  }

  if (state.waRefreshQueued) {
    state.waRefreshQueued = false;
    await refreshWaChats({ ...opts, markRead: false, showLoadingMessages: false });
  }
}

function scheduleWaSyncRefresh(options = {}) {
  const opts = options && typeof options === "object" ? options : {};
  if (state.waSyncTimer) clearTimeout(state.waSyncTimer);
  state.waSyncTimer = setTimeout(() => {
    state.waSyncTimer = null;
    const shouldMarkRead = canMarkReadForChat(state.waActiveChatJid);
    refreshWaChats({
      refreshMessages: true,
      markRead: shouldMarkRead,
      showLoadingMessages: false,
      includePhotos: opts.includePhotos === true,
      maxPhotoFetch: Number.isFinite(Number(opts.maxPhotoFetch)) ? Number(opts.maxPhotoFetch) : 24,
      minMinutesBetweenPhotoChecks: Number.isFinite(Number(opts.minMinutesBetweenPhotoChecks))
        ? Number(opts.minMinutesBetweenPhotoChecks)
        : 90
    }).catch(() => {});
  }, 160);
}

async function pickWaAttachment() {
  const res = await window.api.waPickAttachment();
  if (!res?.ok && res?.canceled) return;
  if (!res?.ok) throw new Error("Attachment selection failed");
  const next = Array.isArray(res?.attachments)
    ? res.attachments
    : res?.attachment
      ? [res.attachment]
      : [];
  appendWaPendingAttachments(next);
}

function buildWaComposerSendQueue(trimmedText, queuedAttachments) {
  const text = String(trimmedText || "");
  const queued = normalizeWaAttachmentList(queuedAttachments);
  if (queued.length === 0) {
    return [{ text, attachment: null }];
  }

  const out = [];
  let textSent = !text;

  if (!textSent && !waAttachmentSupportsCaption(queued[0])) {
    out.push({ text, attachment: null });
    textSent = true;
  }

  for (const attachment of queued) {
    const withText = !textSent && waAttachmentSupportsCaption(attachment) ? text : "";
    out.push({ text: withText, attachment });
    if (withText) textSent = true;
  }

  if (!textSent && text) {
    out.push({ text, attachment: null });
  }

  return out;
}

function optimisticPreviewTextForSendItem(item) {
  const src = item && typeof item === "object" ? item : {};
  const text = String(src.text || "").trim();
  if (text) return text;
  const attachment = src.attachment && typeof src.attachment === "object" ? src.attachment : null;
  if (!attachment) return "";
  const kind = String(attachment.kind || "attachment")
    .trim()
    .toLowerCase();
  const readableKind =
    kind === "image" ? "image" : kind === "video" ? "video" : kind === "audio" ? "audio" : "document";
  const name = String(attachment.fileName || "").trim();
  return name ? `[Sending ${readableKind}] ${name}` : `[Sending ${readableKind}]`;
}

function buildOptimisticWaMessage(chatJid, item, idx) {
  const nowMs = Date.now();
  const previewText = optimisticPreviewTextForSendItem(item);
  return {
    key: {
      remoteJid: String(chatJid || ""),
      id: `local_${nowMs}_${idx}_${Math.random().toString(16).slice(2, 8)}`,
      fromMe: true,
      participant: ""
    },
    chatJid: String(chatJid || ""),
    timestampMs: nowMs + idx,
    fromMe: true,
    senderName: "You",
    type: "text",
    text: previewText,
    preview: previewText,
    hasMedia: false,
    media: null,
    status: 0,
    optimistic: true
  };
}

function removeOptimisticWaMessagesByKeys(keys) {
  const keySet = new Set((Array.isArray(keys) ? keys : []).map((x) => String(x || "")).filter(Boolean));
  if (keySet.size === 0) return;
  state.waMessages = (Array.isArray(state.waMessages) ? state.waMessages : []).filter((msg) => {
    const id = String(msg?.key?.id || "");
    return !keySet.has(id);
  });
}

async function sendWaComposerMessage() {
  if (!state.waActiveChatJid) throw new Error("Please select a chat");
  if (state.waComposerSending) return;

  const textRaw = String(el("waComposerInput").value || "");
  const trimmedText = textRaw.trim();
  const queued = normalizeWaAttachmentList(state.waPendingAttachments);
  const hasText = trimmedText.length > 0;
  const hasAttachment = queued.length > 0;
  if (!hasText && !hasAttachment) return;

  const sendQueue = buildWaComposerSendQueue(trimmedText, queued);
  if (sendQueue.length === 0) return;

  stopWaOutgoingTyping({ sendPaused: true });

  const activeChatJid = state.waActiveChatJid;
  const optimisticMessages = sendQueue.map((item, idx) => buildOptimisticWaMessage(activeChatJid, item, idx));
  const optimisticIds = optimisticMessages.map((msg) => String(msg?.key?.id || "")).filter(Boolean);
  state.waMessages = [...(Array.isArray(state.waMessages) ? state.waMessages : []), ...optimisticMessages];
  renderWaMessages({ forceBottom: true });
  const composerInput = el("waComposerInput");
  if (composerInput) {
    composerInput.value = "";
    composerInput.dispatchEvent(new Event("input", { bubbles: true }));
  }
  setWaPendingAttachments([]);

  setWaComposerSending(true);
  try {
    for (const item of sendQueue) {
      const attachment = item && typeof item === "object" ? item.attachment || null : null;
      const text = String(item?.text || "");
      await window.api.waSendChatMessage({
        chatJid: activeChatJid,
        text,
        attachment
      });
    }
  } catch (e) {
    removeOptimisticWaMessagesByKeys(optimisticIds);
    if (composerInput && !String(composerInput.value || "").trim()) {
      composerInput.value = textRaw;
      composerInput.dispatchEvent(new Event("input", { bubbles: true }));
    }
    if ((state.waPendingAttachments || []).length === 0 && queued.length > 0) {
      setWaPendingAttachments(queued);
    }
    renderWaMessages({ forceBottom: true });
    await refreshWaMessages({ markRead: false, forceBottom: true, showLoading: false }).catch(() => {});
    await refreshWaChats({ refreshMessages: false, markRead: false }).catch(() => {});
    throw e;
  } finally {
    await refreshWaMessages({ markRead: false, forceBottom: true, showLoading: false }).catch(() => {});
    await refreshWaChats({ refreshMessages: false, markRead: false }).catch(() => {});
    setWaComposerSending(false);
    focusWaComposerInput();
  }
}

function isWaFileDragEvent(evt) {
  const types = evt?.dataTransfer?.types;
  if (!types) return false;
  try {
    return Array.from(types).includes("Files");
  } catch {
    return false;
  }
}

function setWaDropActive(active) {
  const panel = document.querySelector(".waConversation");
  if (!panel) return;
  panel.classList.toggle("waDropActive", !!active);
}

async function handleWaFileDrop(evt) {
  if (!state.waActiveChatJid) {
    toast("WhatsApp", "Please select a chat before dropping files");
    return;
  }
  if (state.waComposerSending) {
    toast("WhatsApp", "Upload in progress. Please wait.");
    return;
  }

  const fileList = evt?.dataTransfer?.files || [];
  const fallbackPaths = waParseDroppedPathCandidates(evt?.dataTransfer);
  const attachments = waDroppedFilesToAttachments(fileList, fallbackPaths);
  if (attachments.length === 0) {
    toast("WhatsApp", "No valid files were dropped");
    return;
  }

  appendWaPendingAttachments(attachments);
  toast("WhatsApp", `Attached ${attachments.length} file${attachments.length > 1 ? "s" : ""}. Press Send when ready.`);
}
function renderActivity() {
  const tbody = el("activityBody");
  tbody.innerHTML = "";
  for (const row of state.activityRows) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${escapeHtml(row.ts || "")}</td>
      <td>${escapeHtml(row.phone || "")}</td>
      <td>${statusPill(row.status)}</td>
      <td>${escapeHtml(row.error || "")}</td>
    `;
    tbody.appendChild(tr);
  }
}

function pushActivity(row) {
  state.activityRows.unshift(row);
  if (state.activityRows.length > 500) state.activityRows = state.activityRows.slice(0, 500);
  renderActivity();
}

function renderBranchesToSelect(selectId, defaultBranch) {
  const select = el(selectId);
  if (!select) return;
  select.innerHTML = "";
  for (const branch of state.branches) {
    const opt = document.createElement("option");
    opt.value = branch.label;
    opt.textContent = branch.label;
    select.appendChild(opt);
  }
  if (state.branches.length === 0) return;
  const preferred = defaultBranch && state.branches.some((b) => b.label === defaultBranch) ? defaultBranch : state.branches[0].label;
  select.value = preferred;
}

function getAppointmentById(id) {
  return state.appointments.find((a) => String(a.id) === String(id)) || null;
}

function refreshAppointmentSummary() {
  el("apptSummary").textContent = `${state.selectedAppointmentIds.size} selected / ${state.appointments.length} records`;
}

function waJidFromAppointmentPhone(input) {
  const phone = normalizePhone(input);
  if (!isValidPhone(phone)) return "";
  return `${phone}@s.whatsapp.net`;
}

function upsertLocalWaChatStub(chatJid, patientName) {
  const jid = String(chatJid || "").trim();
  if (!jid) return;
  const name = String(patientName || "").trim();
  const existingIdx = state.waChats.findIndex((x) => String(x?.jid || "") === jid);
  const fallbackTitle = name || String(jid.split("@")[0] || jid);

  if (existingIdx < 0) {
    state.waChats.unshift({
      jid,
      title: fallbackTitle,
      preview: "",
      lastMessageType: "",
      lastMessageFromMe: false,
      lastMessageTimestampMs: 0,
      unreadCount: 0,
      archived: false,
      pinned: false,
      avatarUrl: "",
      isGroup: false
    });
    return;
  }

  const row = state.waChats[existingIdx] || {};
  if (name && (!row.title || String(row.title || "").trim() === String(row.jid || "").trim())) {
    state.waChats[existingIdx] = {
      ...row,
      title: name
    };
  }
}

async function openAppointmentPatientWaChat(appt) {
  const src = appt && typeof appt === "object" ? appt : {};
  const chatJid = waJidFromAppointmentPhone(src.Patient_Phone_No || src.phone || "");
  if (!chatJid) throw new Error("Invalid or missing patient phone");

  const patientName = String(src.Patient_Name || src.name || "").trim();
  setActiveTab("whatsapp");
  upsertLocalWaChatStub(chatJid, patientName);
  await openWaChat(chatJid);
  refreshWaChats({ refreshMessages: false, markRead: false, includePhotos: true }).catch(() => {});
}

function renderAppointmentTable() {
  const body = el("apptTableBody");
  body.innerHTML = "";
  const sorted = [...state.appointments].sort((a, b) => toInt(a.Appt_Start_Time) - toInt(b.Appt_Start_Time));
  for (const appt of sorted) {
    const id = String(appt.id);
    const tr = document.createElement("tr");
    tr.className = state.selectedAppointmentIds.has(id) ? "rowSelected" : "";

    const cb = document.createElement("input");
    cb.type = "checkbox";
    cb.checked = state.selectedAppointmentIds.has(id);
    cb.addEventListener("change", () => {
      if (cb.checked) state.selectedAppointmentIds.add(id);
      else state.selectedAppointmentIds.delete(id);
      renderAppointmentTable();
      refreshAppointmentSummary();
    });

    const tdCb = document.createElement("td");
    tdCb.appendChild(cb);
    const tdTime = document.createElement("td");
    tdTime.textContent = formatTimeRange(appt.Appt_Start_Time, appt.Appt_End_Time);
    const tdPatient = document.createElement("td");
    tdPatient.innerHTML = `<strong>${escapeHtml(appt.Patient_Name)}</strong><br/><span class="smallText">${escapeHtml(
      appt.Patient_Phone_No || "-"
    )}</span>`;
    const tdDentist = document.createElement("td");
    tdDentist.textContent = appt.Dentist_Name || "-";
    const tdTreatment = document.createElement("td");
    tdTreatment.textContent = appt.Treatment || "-";
    const tdWa = document.createElement("td");
    tdWa.className = "apptWaCell";
    const waBtn = document.createElement("button");
    waBtn.type = "button";
    waBtn.className = "apptWaBtn";
    waBtn.title = "Open WhatsApp chat";
    waBtn.setAttribute("aria-label", `Open WhatsApp chat with ${String(appt.Patient_Name || "patient")}`);
    const waIcon = document.createElement("img");
    waIcon.className = "apptWaIcon";
    waIcon.src = "./assets/whatsapp-logo.svg";
    waIcon.alt = "WhatsApp";
    waIcon.loading = "lazy";
    waBtn.appendChild(waIcon);
    const canOpenWa = !!waJidFromAppointmentPhone(appt.Patient_Phone_No || appt.phone || "");
    waBtn.disabled = !canOpenWa;
    waBtn.addEventListener("click", async () => {
      try {
        await openAppointmentPatientWaChat(appt);
      } catch (e) {
        toast("WhatsApp", String(e?.message || e));
      }
    });
    tdWa.appendChild(waBtn);
    const tdStatus = document.createElement("td");
    tdStatus.innerHTML = appt.Status ? '<span class="badgeYes">Yes</span>' : '<span class="badgeNo">No</span>';
    tr.append(tdCb, tdTime, tdPatient, tdDentist, tdTreatment, tdWa, tdStatus);
    body.appendChild(tr);
  }

  if (sorted.length === 0) {
    const tr = document.createElement("tr");
    tr.innerHTML = '<td colspan="7" class="smallText">No appointments found for this branch/date.</td>';
    body.appendChild(tr);
  }

  refreshAppointmentSummary();
}

function appointmentIsRemindEligible(appt) {
  return toInt(appt.Appt_Start_Time) >= Date.now();
}

function appointmentIsFollowEligible(appt) {
  return toInt(appt.Appt_Start_Time) < Date.now() && appt.Status === true;
}

function selectAppointmentsByRule(ruleFn) {
  state.selectedAppointmentIds = new Set(
    state.appointments
      .filter((appt) => {
        try {
          return !!ruleFn(appt);
        } catch {
          return false;
        }
      })
      .map((appt) => String(appt.id))
  );
  renderAppointmentTable();
}

function monthRangeMonthsAgo(monthsAgoRaw) {
  const monthsAgo = clamp(monthsAgoRaw, 1, 24, state.settings.marketingMonthsAgoDefault || 6);
  const now = new Date();
  const nowParts = new Intl.DateTimeFormat("en-CA", {
    timeZone: TIMEZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit"
  })
    .formatToParts(now)
    .reduce((acc, p) => {
      acc[p.type] = p.value;
      return acc;
    }, {});

  const currentYear = Number(nowParts.year);
  const currentMonthIndex = Number(nowParts.month) - 1;
  const targetIndex = currentYear * 12 + currentMonthIndex - monthsAgo;
  const targetYear = Math.floor(targetIndex / 12);
  const targetMonth = (targetIndex % 12) + 1;

  const startTs = Date.UTC(targetYear, targetMonth - 1, 1, 0, 0, 0, 0) - 8 * 60 * 60 * 1000;
  const endTs = Date.UTC(targetYear, targetMonth, 1, 0, 0, 0, 0) - 8 * 60 * 60 * 1000 - 1;
  const monthName = new Intl.DateTimeFormat("en-US", { timeZone: TIMEZONE, month: "long", year: "numeric" }).format(
    new Date(startTs)
  );

  return { monthsAgo, startTs, endTs, label: monthName };
}

function upsertMarketingRecipient(input) {
  const src = input && typeof input === "object" ? input : {};
  const phone = normalizePhone(src.phone);
  if (!isValidPhone(phone)) return false;

  const existing = state.marketingRecipients.find((x) => x.phone === phone);
  if (existing) {
    if (!existing.name && src.name) existing.name = String(src.name);
    if (!existing.dentist && src.dentist) existing.dentist = String(src.dentist);
    if (!existing.apptDate && src.apptDate) existing.apptDate = Number(src.apptDate);
    return false;
  }

  state.marketingRecipients.push({
    id: `m_${phone}_${Date.now().toString(16)}_${Math.random().toString(16).slice(2, 7)}`,
    phone,
    name: String(src.name || "").trim(),
    dentist: String(src.dentist || "").trim(),
    apptDate: toInt(src.apptDate, 0),
    selected: src.selected !== false
  });
  return true;
}

function getSelectedMarketingRecipients() {
  return state.marketingRecipients.filter((x) => x.selected);
}

function refreshMarketingSummary() {
  el("marketingSummary").textContent = `${getSelectedMarketingRecipients().length} selected / ${state.marketingRecipients.length} total`;
}

function syncMarketingSelectAll() {
  const all = el("marketingSelectAll");
  const total = state.marketingRecipients.length;
  const selected = getSelectedMarketingRecipients().length;
  all.checked = total > 0 && selected === total;
  all.indeterminate = selected > 0 && selected < total;
}

function renderMarketingRecipients() {
  const tbody = el("marketingRecipientsBody");
  tbody.innerHTML = "";

  for (const row of state.marketingRecipients) {
    const tr = document.createElement("tr");

    const tdPick = document.createElement("td");
    const cb = document.createElement("input");
    cb.type = "checkbox";
    cb.checked = !!row.selected;
    cb.addEventListener("change", () => {
      row.selected = !!cb.checked;
      refreshMarketingSummary();
      refreshMarketingPreview();
      syncMarketingSelectAll();
    });
    tdPick.appendChild(cb);

    const tdPhone = document.createElement("td");
    tdPhone.textContent = row.phone;
    const tdName = document.createElement("td");
    tdName.textContent = row.name || "-";
    const tdDentist = document.createElement("td");
    tdDentist.textContent = row.dentist || "-";

    const tdAction = document.createElement("td");
    const btnDel = document.createElement("button");
    btnDel.className = "btnGhost";
    btnDel.textContent = "Del";
    btnDel.addEventListener("click", () => {
      state.marketingRecipients = state.marketingRecipients.filter((x) => x.id !== row.id);
      renderMarketingRecipients();
      refreshMarketingPreview();
      toast("Recipient", "Removed from list");
    });
    tdAction.appendChild(btnDel);

    tr.append(tdPick, tdPhone, tdName, tdDentist, tdAction);
    tbody.appendChild(tr);
  }

  if (state.marketingRecipients.length === 0) {
    const tr = document.createElement("tr");
    tr.innerHTML = '<td colspan="5" class="smallText">No recipients yet.</td>';
    tbody.appendChild(tr);
  }

  refreshMarketingSummary();
  syncMarketingSelectAll();
}

function renderTemplateList() {
  const list = el("templateList");
  list.innerHTML = "";

  for (const t of state.templates) {
    const item = document.createElement("div");
    item.className = `templateItem${state.currentTemplateId === t.id ? " active" : ""}`;
    item.innerHTML = `<div class="templateItemName">${escapeHtml(t.name)}</div><div class="templateItemMeta">${escapeHtml(
      templateSnippet(t.body)
    )}</div>`;
    item.addEventListener("click", () => {
      state.currentTemplateId = t.id;
      renderTemplateList();
      loadTemplateEditor();
      renderMarketingTemplateSelect();
      refreshMarketingPreview();
    });
    list.appendChild(item);
  }

  if (state.templates.length === 0) {
    const empty = document.createElement("div");
    empty.className = "smallText";
    empty.textContent = "No marketing templates.";
    list.appendChild(empty);
  }
}

function loadTemplateEditor() {
  const t = getSelectedMarketingTemplate();
  if (!t) {
    el("templateName").value = "";
    el("templateBody").value = "";
    el("templateSendPolicy").value = "once";
    el("templateVariablesText").textContent = "Variables: -";
    el("templatePlaceholderPreview").textContent = "Type template to preview placeholders.";
    return;
  }

  el("templateName").value = t.name;
  el("templateBody").value = t.body;
  state.templateBodyCaretPos = Number((t.body || "").length);
  el("templateSendPolicy").value = t.sendPolicy;
  const vars = extractTemplateVariables(t.body);
  el("templateVariablesText").textContent = vars.length > 0 ? `Variables: ${vars.join(", ")}` : "Variables: -";
  renderTemplatePlaceholderPreview(t.body);
}

function renderMarketingPlaceholderButtons() {
  const wrap = el("marketingPlaceholderButtons");
  wrap.innerHTML = "";

  for (const item of MARKETING_PLACEHOLDERS) {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "placeholderBtn";
    btn.textContent = item.token;
    btn.title = item.description;
    btn.dataset.token = item.token;
    btn.addEventListener("click", () => {
      insertPlaceholderIntoTemplate(item.token, btn);
    });
    wrap.appendChild(btn);
  }
}

function insertPlaceholderIntoTemplate(token, btn) {
  const textarea = el("templateBody");
  const value = textarea.value || "";
  const liveStart = Number(textarea.selectionStart || 0);
  const isTextareaActive = document.activeElement === textarea;
  const insertPos = isTextareaActive ? liveStart : Number(state.templateBodyCaretPos || 0);
  const safePos = Math.max(0, Math.min(value.length, insertPos));
  const next = `${value.slice(0, safePos)}${token}${value.slice(safePos)}`;
  textarea.value = next;
  const caretPos = safePos + token.length;
  state.templateBodyCaretPos = caretPos;
  textarea.focus();
  textarea.setSelectionRange(caretPos, caretPos);

  readMarketingTemplateEditorToState();
  const vars = extractTemplateVariables(next);
  el("templateVariablesText").textContent = vars.length > 0 ? `Variables: ${vars.join(", ")}` : "Variables: -";
  renderTemplatePlaceholderPreview(next);
  renderTemplateList();
  refreshMarketingPreview();

  if (btn) {
    btn.classList.add("inserted");
    setTimeout(() => btn.classList.remove("inserted"), 480);
  }
}

function rememberTemplateCaretPosition() {
  const textarea = el("templateBody");
  if (!textarea) return;
  const start = Number(textarea.selectionStart || 0);
  state.templateBodyCaretPos = Math.max(0, start);
}

function renderTemplatePlaceholderPreview(text) {
  const raw = String(text || "");
  const target = el("templatePlaceholderPreview");
  if (!raw.trim()) {
    target.textContent = "Type template to preview placeholders.";
    return;
  }

  const parts = [];
  const re = /\{(\w+)\}/g;
  let cursor = 0;
  let m = null;
  while ((m = re.exec(raw))) {
    const full = String(m[0] || "");
    const key = String(m[1] || "");
    const start = Number(m.index);
    if (start > cursor) {
      parts.push(escapeHtml(raw.slice(cursor, start)).replace(/\n/g, "<br/>"));
    }
    const meta = getPlaceholderMetaByKey(key);
    const cls = meta ? "placeholderInline" : "placeholderInline unknown";
    const title = meta ? meta.description : "Unknown placeholder. Not in ready variable list.";
    parts.push(`<span class="${cls}" title="${escapeHtml(title)}">${escapeHtml(full)}</span>`);
    cursor = start + full.length;
  }
  if (cursor < raw.length) {
    parts.push(escapeHtml(raw.slice(cursor)).replace(/\n/g, "<br/>"));
  }
  target.innerHTML = parts.join("");
}

function renderMarketingTemplateSelect() {
  const select = el("marketingTemplateSelect");
  select.innerHTML = "";

  for (const t of state.templates) {
    const opt = document.createElement("option");
    opt.value = t.id;
    opt.textContent = t.name;
    select.appendChild(opt);
  }

  if (state.templates.length === 0) {
    const opt = document.createElement("option");
    opt.value = "";
    opt.textContent = "No template";
    select.appendChild(opt);
    select.value = "";
    return;
  }

  if (!state.currentTemplateId || !state.templates.some((x) => x.id === state.currentTemplateId)) {
    state.currentTemplateId = state.templates[0].id;
  }

  select.value = state.currentTemplateId;
}

function buildMarketingTemplateVars(recipient) {
  const row = recipient && typeof recipient === "object" ? recipient : {};
  const selectedBranch = el("marketingBranchSelect").value || state.session.user?.Branch || "";
  const myBranch = state.session.user?.Branch || selectedBranch || "";
  const dateTs = row.apptDate ? Number(row.apptDate) : Date.now();
  const weekday = formatWeekdayForMessage(dateTs, "english");

  return {
    name: row.name || "Patient",
    branch: selectedBranch,
    my_branch: myBranch,
    dentist: row.dentist || "",
    date: formatDateForMessage(dateTs, "english"),
    day: weekday,
    weekday,
    time: row.apptDate ? formatTimeForMessage(row.apptDate) : "",
    phone: normalizePhone(row.phone || "")
  };
}

function refreshMarketingPreview() {
  const template = getSelectedMarketingTemplate();
  const recipient = getSelectedMarketingRecipients()[0];
  if (!template) return (el("marketingPreview").textContent = "No marketing template selected.");
  if (!recipient) return (el("marketingPreview").textContent = "Select at least one recipient to preview.");

  el("marketingPreview").textContent = renderTemplate(template.body, buildMarketingTemplateVars(recipient));
}
function applySettingsToUi() {
  const s = state.settings;
  el("settingGapMin").value = String(s.gapMinSec || 7);
  el("settingGapMax").value = String(s.gapMaxSec || 45);
  el("settingMarketingMonths").value = String(s.marketingMonthsAgoDefault || 6);
  el("marketingMonthsAgo").value = String(s.marketingMonthsAgoDefault || 6);
}

function applyAppointmentTemplatesToUi() {
  const t = state.appointmentTemplates;
  el("tplRemindBahasa").value = t.remindAppointment?.bahasa || "";
  el("tplRemindEnglish").value = t.remindAppointment?.english || "";
  el("tplFollowBahasa").value = t.followUp?.bahasa || "";
  el("tplFollowEnglish").value = t.followUp?.english || "";
  el("tplReviewBahasa").value = t.requestReview?.bahasa || "";
  el("tplReviewEnglish").value = t.requestReview?.english || "";
}

function readAppointmentTemplatesFromUi() {
  return {
    remindAppointment: { bahasa: el("tplRemindBahasa").value, english: el("tplRemindEnglish").value },
    followUp: { bahasa: el("tplFollowBahasa").value, english: el("tplFollowEnglish").value },
    requestReview: { bahasa: el("tplReviewBahasa").value, english: el("tplReviewEnglish").value }
  };
}

function normalizeGenderTitles(genderRaw) {
  const g = String(genderRaw || "").toLowerCase();
  if (g.includes("male") || g === "m" || g === "lelaki") return { title_bm: "Encik", title_en: "Mr" };
  if (g.includes("female") || g === "f" || g === "perempuan") return { title_bm: "Cik", title_en: "Ms" };
  return { title_bm: "Encik / Cik", title_en: "Mr / Mrs" };
}

async function mapWithConcurrency(items, limit, mapper) {
  const arr = Array.isArray(items) ? items : [];
  const lim = Math.max(1, Number(limit || 4));
  const out = new Array(arr.length);
  let cursor = 0;

  async function worker() {
    while (cursor < arr.length) {
      const idx = cursor++;
      try {
        out[idx] = await mapper(arr[idx], idx);
      } catch (e) {
        out[idx] = { error: e };
      }
    }
  }

  const jobs = [];
  for (let i = 0; i < lim; i++) jobs.push(worker());
  await Promise.all(jobs);
  return out;
}

async function prepareAppointmentSendItems(purposeKey, language) {
  const selected = Array.from(state.selectedAppointmentIds).map((id) => getAppointmentById(id)).filter(Boolean);
  if (selected.length === 0) throw new Error("Please select at least one appointment first");

  const templateText = state.appointmentTemplates?.[purposeKey]?.[language] || "";
  if (!String(templateText || "").trim()) throw new Error("Template text is empty. Update it in Template tab.");

  const prepared = await mapWithConcurrency(selected, 5, async (appt) => {
    let patient = null;
    if (appt.ic_number) {
      try {
        const res = await window.api.clinicGetPatient({ ic_number: appt.ic_number });
        patient = res?.patient || null;
      } catch {
        patient = null;
      }
    }

    const branchInfo = getBranchByName(appt.Branch_Name) || {};
    const titles = normalizeGenderTitles(patient?.gender);
    const name = String(patient?.name || appt.Patient_Name || "Patient");
    const phone = normalizePhone(patient?.phone || appt.Patient_Phone_No || "");

    if (!isValidPhone(phone)) return { skip: true, reason: "Invalid or missing phone", name, phone };

    const vars = {
      name,
      branch: appt.Branch_Name || branchInfo.label || "",
      dentist: appt.Dentist_Name || "",
      date: formatDateForMessage(appt.Appt_Date || appt.Appt_Start_Time, language),
      weekday: formatWeekdayForMessage(appt.Appt_Date || appt.Appt_Start_Time, language),
      time: formatTimeForMessage(appt.Appt_Start_Time),
      address: branchInfo.address || "",
      branch_phone: branchInfo.branch_phone || "",
      google_direction: branchInfo.google_direction || "",
      waze_direction: branchInfo.waze_direction || "",
      google_review_link: branchInfo.Google_Review || "",
      title_bm: titles.title_bm,
      title_en: titles.title_en
    };

    return {
      skip: false,
      name,
      phone,
      text: renderTemplate(templateText, vars),
      aiVariables: vars,
      templateId: `appointment_${purposeKey}_${language}`
    };
  });

  return prepared.filter((x) => x && !x.skip && x.phone && x.text);
}

function openConfirmModal({ title, subtitle, recipientsText, sampleText }) {
  el("confirmTitle").textContent = title || "Confirm Send";
  el("confirmSubtitle").textContent = subtitle || "";
  el("confirmRecipients").textContent = recipientsText || "-";
  el("confirmSample").textContent = sampleText || "-";
  el("confirmModal").classList.remove("hidden");

  return new Promise((resolve) => {
    state.confirmResolver = resolve;
  });
}

function closeConfirmModal(confirmed) {
  el("confirmModal").classList.add("hidden");
  if (state.confirmResolver) {
    state.confirmResolver(!!confirmed);
    state.confirmResolver = null;
  }
}

function openCreateProfileModal(defaultName = "") {
  el("createProfileModal").classList.remove("hidden");
  const input = el("createProfileNameInput");
  if (input) {
    input.value = String(defaultName || "");
    input.focus();
    input.select();
  }
}

function closeCreateProfileModal() {
  el("createProfileModal").classList.add("hidden");
}

async function createProfileFromModal() {
  const rawName = String(el("createProfileNameInput")?.value || "").trim();
  const fallbackName = `Profile ${Math.max(1, (Array.isArray(state.profiles) ? state.profiles.length : 0) + 1)}`;
  const name = rawName || fallbackName;

  const res = await window.api.createProfile(name);
  await refreshProfiles();
  const createdId = String(res?.profile?.id || "").trim();
  if (createdId && getProfileById(createdId)) {
    state.settingsProfileId = createdId;
    const settingsSelect = el("settingProfileSelect");
    if (settingsSelect) settingsSelect.value = createdId;
    updateSettingsProfileControls();
  }
  closeCreateProfileModal();
  toast("Profile", `Created ${name}`);
}

async function sendPreparedItems(items, batchLabel, aiEnabled) {
  const sendItems = (Array.isArray(items) ? items : []).map((x) => ({
    phone: x.phone,
    name: x.name,
    text: x.text,
    aiVariables: x.aiVariables || {},
    templateId: x.templateId || batchLabel
  }));

  if (sendItems.length === 0) throw new Error("No valid recipients to send");
  if (!state.waConnected) throw new Error("WhatsApp is not connected");

  const confirmRecipients = sendItems
    .slice(0, 40)
    .map((x, i) => `${i + 1}. ${x.name || "-"} (${x.phone})`)
    .join("\n");
  const suffix = sendItems.length > 40 ? `\n... and ${sendItems.length - 40} more` : "";

  const confirmed = await openConfirmModal({
    title: "Confirm Send",
    subtitle: `${sendItems.length} messages will be sent one-by-one`,
    recipientsText: `${confirmRecipients}${suffix}`,
    sampleText: sendItems[0].text
  });

  if (!confirmed) {
    toast("Canceled", "Send canceled by user");
    return null;
  }

  return await window.api.waSendPreparedBatch({
    batchLabel,
    items: sendItems,
    pacing: { pattern: "random", minSec: state.settings.gapMinSec, maxSec: state.settings.gapMaxSec },
    aiRewrite: aiEnabled
      ? { enabled: true, prompt: AI_VARIATION_PROMPT, fallbackToOriginal: true }
      : { enabled: false },
    safety: { maxRecipients: 500 }
  });
}

async function doAppointmentSend(purposeKey, langId, aiId) {
  if (!state.waConnected) {
    toast("WhatsApp", "Please connect WhatsApp first");
    setActiveTab("connect");
    return;
  }

  const items = await prepareAppointmentSendItems(purposeKey, el(langId).value);
  if (items.length === 0) {
    toast("No recipients", "No valid recipients were prepared.");
    return;
  }

  const res = await sendPreparedItems(items, `appointment_${purposeKey}`, !!el(aiId).checked);
  if (!res) return;
  toast("Appointment send done", `Sent: ${res.sent}, Failed: ${res.failed}, Skipped: ${res.skipped}`);
}

function readMarketingTemplateEditorToState() {
  const t = getSelectedMarketingTemplate();
  if (!t) return;
  t.name = el("templateName").value.trim() || "Untitled";
  t.body = el("templateBody").value || "";
  t.sendPolicy = el("templateSendPolicy").value === "multiple" ? "multiple" : "once";
  t.variables = extractTemplateVariables(t.body);
}

async function loadAppointments() {
  const branch = el("apptBranchSelect").value;
  const dateTs = ymdToKlMidnightTs(el("apptDateInput").value);
  if (!branch) throw new Error("Please select a branch");
  if (!dateTs) throw new Error("Please select a valid date");

  const res = await window.api.clinicGetAppointmentList({ branch, date: dateTs });
  state.appointments = Array.isArray(res?.appointments) ? res.appointments : [];
  state.selectedAppointmentIds = new Set();
  renderAppointmentTable();
}

async function loadPastPatients() {
  const branch = el("marketingBranchSelect").value;
  if (!branch) throw new Error("Please select branch");

  const range = monthRangeMonthsAgo(el("marketingMonthsAgo").value);
  el("marketingMonthsAgo").value = String(range.monthsAgo);
  el("pastPatientRangeText").textContent = `Loading ${range.label} (${formatDateForMessage(
    range.startTs,
    "english"
  )} - ${formatDateForMessage(range.endTs, "english")})`;

  const res = await window.api.clinicGetPastPatients({
    branch,
    start_day: range.startTs,
    end_day: range.endTs
  });
  const rows = Array.isArray(res?.patients) ? res.patients : [];

  let added = 0;
  for (const row of rows) {
    if (
      upsertMarketingRecipient({
        phone: row.Patient_Phone_No,
        name: row.Patient_Name,
        dentist: row.Dentist_Name,
        apptDate: row.Appt_Date
      })
    ) {
      added++;
    }
  }

  renderMarketingRecipients();
  refreshMarketingPreview();
  el("pastPatientRangeText").textContent = `Loaded ${rows.length} rows from ${range.label} (added ${added})`;
}

async function sendMarketing() {
  if (!state.waConnected) {
    toast("WhatsApp", "Please connect WhatsApp first");
    setActiveTab("connect");
    return;
  }

  const template = getSelectedMarketingTemplate();
  if (!template) throw new Error("Please select a template");

  const selected = getSelectedMarketingRecipients();
  if (selected.length === 0) throw new Error("Please select recipients");

  const items = selected
    .map((row) => {
      const vars = buildMarketingTemplateVars(row);

      return {
        phone: row.phone,
        name: row.name || "Patient",
        text: renderTemplate(template.body, vars),
        aiVariables: vars,
        templateId: `marketing_${template.id}`
      };
    })
    .filter((x) => isValidPhone(x.phone) && String(x.text || "").trim());

  if (items.length === 0) throw new Error("No valid messages to send");

  const res = await sendPreparedItems(items, `marketing_${template.id}`, !!el("aiMarketing").checked);
  if (!res) return;
  toast("Marketing send done", `Sent: ${res.sent}, Failed: ${res.failed}, Skipped: ${res.skipped}`);
}

async function saveSettings() {
  const next = {
    gapMinSec: clamp(el("settingGapMin").value, 7, 45, 7),
    gapMaxSec: clamp(el("settingGapMax").value, 7, 45, 45),
    marketingMonthsAgoDefault: clamp(el("settingMarketingMonths").value, 1, 24, 6)
  };
  if (next.gapMaxSec < next.gapMinSec) next.gapMaxSec = next.gapMinSec;

  const res = await window.api.saveClinicSettings(next);
  state.settings = { ...DEFAULT_CLINIC_SETTINGS, ...(res?.settings || {}) };
  applySettingsToUi();
  toast("Settings", "Saved");
}
function renderProfiles() {
  const profiles = Array.isArray(state.profiles) ? state.profiles : [];
  const fillSelect = (selectEl, selectedId) => {
    if (!selectEl) return;
    const prev = String(selectedId || "").trim();
    selectEl.innerHTML = "";
    for (const p of profiles) {
      const opt = document.createElement("option");
      opt.value = p.id;
      opt.textContent = p.name;
      selectEl.appendChild(opt);
    }
    const fallbackId = profiles[0]?.id || "";
    const nextValue = profiles.some((p) => p.id === prev) ? prev : fallbackId;
    if (nextValue) selectEl.value = nextValue;
  };

  fillSelect(el("profileSelect"), state.activeProfileId);
  const preferredSettingsProfileId = state.settingsProfileId || state.activeProfileId;
  fillSelect(el("settingProfileSelect"), preferredSettingsProfileId);
  state.settingsProfileId = String(el("settingProfileSelect")?.value || state.settingsProfileId || "").trim();

  updateConnectProfileSummary();
  updateSettingsProfileControls();
}

async function refreshProfiles() {
  const res = await window.api.getProfiles();
  state.profiles = Array.isArray(res?.profiles) ? res.profiles : [];
  state.activeProfileId = res?.activeProfileId || null;
  renderProfiles();
  scheduleWaSyncRefresh();
}

async function refreshWaChatsWithHistoryWarmup() {
  await refreshWaChats({
    refreshMessages: true,
    markRead: true,
    ensureHistory: true,
    forceHistory: true,
    includePhotos: true
  });
}

async function activateProfileAndSync(profileId) {
  const id = String(profileId || "").trim();
  if (!id) return;

  stopWaOutgoingTyping({ sendPaused: true });
  clearAllWaPresenceState({ render: false });
  state.waActiveChatJid = "";
  state.waExplicitOpenChatJid = "";
  state.waMessages = [];
  state.waChats = [];
  state.waEmojiPickerOpen = false;
  closeWaEmojiPicker({ restoreFocus: false });
  state.waForceHistoryRefreshOnConnected = true;

  await window.api.setActiveProfile(id);
  await refreshProfiles();
  const status = await window.api.waGetConnectionState();
  setConnectionBadge(
    !!status?.connected,
    status?.text || "Not connected",
    isConnectionStatusConnecting(status?.text, status?.connecting)
  );

  if (status?.connected) {
    state.waForceHistoryRefreshOnConnected = false;
    await refreshWaChatsWithHistoryWarmup();
  } else {
    renderWaChatList();
    renderWaConversationHead();
    renderWaMessages({ forceBottom: false });
  }
}

function setConnectModeUi() {
  const pairing = el("connectMethod").value === "pairing";
  el("pairingPhoneWrap").classList.toggle("hidden", !pairing);
  if (!pairing) {
    setPairingPreview("");
  }
  updateConnectPreviewUi();
}

async function doHandshake(methodOverride) {
  const method = methodOverride === "pairing" || methodOverride === "qr" ? methodOverride : el("connectMethod").value;
  const phoneNumber = el("pairingPhone").value.trim();
  clearConnectPreview({ clearPairing: true });

  await window.api.waHandshake({ method, phoneNumber });
  toast("Connect", method === "pairing" ? "Requesting pairing code..." : "Waiting for QR...");
}

async function doSoftDisconnect() {
  await window.api.waDisconnect();
  clearConnectPreview({ clearPairing: true });
  toast("Connect", "Disconnected");
}

async function connectFromHeaderToggle() {
  const reconnectRes = await window.api.waAutoReconnect();
  if (reconnectRes?.ok) {
    toast("Connect", "Reconnecting...");
    return;
  }

  // If no saved session exists, fall back to QR connect flow.
  const methodSelect = el("connectMethod");
  if (methodSelect) methodSelect.value = "qr";
  setConnectModeUi();
  await doHandshake("qr");
}

async function toggleConnectionFromHeader() {
  if (state.waConnToggleBusy) return;
  setConnectionBusy(true);
  try {
    if (state.waConnected) {
      await doSoftDisconnect();
    } else {
      await connectFromHeaderToggle();
    }
  } finally {
    setConnectionBusy(false);
  }
}

async function reloadTemplateData() {
  const [templatesRaw, appointmentTplRes] = await Promise.all([
    window.api.getTemplates(),
    window.api.getAppointmentTemplates()
  ]);

  state.templates = (Array.isArray(templatesRaw) ? templatesRaw : []).map((t, i) => normalizeTemplate(t, i));
  state.currentTemplateId = state.currentTemplateId || state.templates[0]?.id || null;
  if (state.currentTemplateId && !state.templates.some((x) => x.id === state.currentTemplateId)) {
    state.currentTemplateId = state.templates[0]?.id || null;
  }

  state.appointmentTemplates = {
    ...DEFAULT_APPOINTMENT_TEMPLATES,
    ...(appointmentTplRes?.templates || {})
  };

  renderTemplateList();
  loadTemplateEditor();
  renderMarketingTemplateSelect();
  applyAppointmentTemplatesToUi();
  refreshMarketingPreview();
}

async function loadInitialDataAfterLogin() {
  const userBranch = String(state.session.user?.Branch || "").trim();

  const [settingsRes, branchRes, profileRes, connRes] = await Promise.all([
    window.api.getClinicSettings(),
    window.api.clinicGetBranchList(),
    window.api.getProfiles(),
    window.api.waGetConnectionState()
  ]);

  state.settings = { ...DEFAULT_CLINIC_SETTINGS, ...(settingsRes?.settings || {}) };
  state.branches = Array.isArray(branchRes?.branches) ? branchRes.branches : [];
  state.profiles = Array.isArray(profileRes?.profiles) ? profileRes.profiles : [];
  state.activeProfileId = profileRes?.activeProfileId || null;
  state.settingsProfileId = state.settingsProfileId || state.activeProfileId || "";

  applySettingsToUi();
  renderBranchesToSelect("apptBranchSelect", userBranch);
  renderBranchesToSelect("marketingBranchSelect", userBranch);

  el("apptDateInput").value = getTodayYmdKl();
  const range = monthRangeMonthsAgo(el("marketingMonthsAgo").value);
  el("pastPatientRangeText").textContent = `Range: ${range.label}`;

  renderProfiles();
  setConnectModeUi();
  setConnectionBadge(
    !!connRes?.connected,
    connRes?.text || "Not connected",
    isConnectionStatusConnecting(connRes?.text, connRes?.connecting)
  );
  state.waChatSearch = "";
  state.waActiveChatJid = "";
  state.waExplicitOpenChatJid = "";
  state.waMessages = [];
  state.waChats = [];
  state.waLoadingChats = false;
  state.waLoadingMessages = false;
  state.waRefreshQueued = false;
  state.waForceHistoryRefreshOnConnected = false;
  state.waChatsReqSeq = 0;
  state.waMessagesReqSeq = 0;
  state.waDropDepth = 0;
  state.waEmojiPickerOpen = false;
  setWaDropActive(false);
  stopWaOutgoingTyping({ sendPaused: false });
  clearAllWaPresenceState({ render: false });
  setWaComposerSending(false);
  setWaPendingAttachments([]);
  closeWaEmojiPicker({ restoreFocus: false });
  el("waChatSearchInput").value = "";

  await reloadTemplateData();
  await refreshWaChats({
    refreshMessages: true,
    markRead: true,
    ensureHistory: true,
    forceHistory: true,
    includePhotos: true
  });
  await loadAppointments();
  renderMarketingRecipients();
  renderActivity();
}

function showLoginScreen() {
  state.session = { authToken: "", user: {} };
  state.waConnected = false;
  state.waConnecting = false;
  state.waConnToggleBusy = false;
  state.waQrDataUrl = "";
  state.waPairingCode = "";
  state.settingsProfileId = "";
  state.appointments = [];
  state.selectedAppointmentIds = new Set();
  state.marketingRecipients = [];
  state.waChats = [];
  state.waActiveChatJid = "";
  state.waExplicitOpenChatJid = "";
  state.waMessages = [];
  state.waChatSearch = "";
  state.waLoadingChats = false;
  state.waLoadingMessages = false;
  state.waRefreshQueued = false;
  state.waForceHistoryRefreshOnConnected = false;
  state.waChatsReqSeq = 0;
  state.waMessagesReqSeq = 0;
  state.waDropDepth = 0;
  state.waEmojiPickerOpen = false;
  setWaDropActive(false);
  stopWaOutgoingTyping({ sendPaused: false });
  clearAllWaPresenceState({ render: false });
  setWaComposerSending(false);
  setWaPendingAttachments([]);
  el("waChatSearchInput").value = "";
  el("waComposerInput").value = "";
  if (state.waSyncTimer) {
    clearTimeout(state.waSyncTimer);
    state.waSyncTimer = null;
  }
  closeWaImageLightbox();
  closeWaEmojiPicker({ restoreFocus: false });
  setConnectionBadge(false, "Not connected", false);
  clearConnectPreview({ clearPairing: true });
  renderWaChatList();
  renderWaConversationHead();
  renderWaMessages();

  el("appShell").classList.add("hidden");
  el("loginScreen").classList.remove("hidden");
  el("loginPassword").value = "";
  el("loginStatus").textContent = "Use your clinic account to continue.";
}

function showAppShell() {
  el("loginScreen").classList.add("hidden");
  el("appShell").classList.remove("hidden");
}

async function afterLoginLoad() {
  showAppShell();
  updateHeaderGreeting();
  setActiveTab("whatsapp");
  await loadInitialDataAfterLogin();
}

async function tryRestoreSession() {
  try {
    const res = await window.api.clinicGetSession();
    const session = res?.session || { authToken: "", user: {} };
    if (!session.authToken) return showLoginScreen();

    state.session = session;
    try {
      const refresh = await window.api.clinicRefreshMe();
      state.session = refresh?.session || session;
    } catch {
      return showLoginScreen();
    }

    await afterLoginLoad();
  } catch {
    showLoginScreen();
  }
}
function bindEvents() {
  renderWaEmojiPicker();

  document.querySelectorAll(".tabBtn").forEach((btn) => {
    btn.addEventListener("click", async () => {
      setActiveTab(btn.dataset.tab);
      if (btn.dataset.tab === "whatsapp") {
        try {
          await refreshWaChats({
            refreshMessages: true,
            markRead: true,
            ensureHistory: true,
            includePhotos: true
          });
        } catch (e) {
          toast("WhatsApp", String(e?.message || e));
        }
      }
    });
  });

  el("btnWaRefreshChats").addEventListener("click", async () => {
    try {
      await refreshWaChats({
        refreshMessages: true,
        markRead: true,
        ensureHistory: true,
        forceHistory: true,
        forceNameSync: true,
        includePhotos: true
      });
    } catch (e) {
      toast("WhatsApp", String(e?.message || e));
    }
  });

  el("waChatSearchInput").addEventListener("input", () => {
    state.waChatSearch = el("waChatSearchInput").value.trim();
    scheduleWaSyncRefresh();
  });

  el("btnWaAttach").addEventListener("click", async () => {
    try {
      await pickWaAttachment();
    } catch (e) {
      toast("WhatsApp", String(e?.message || e));
    }
  });

  el("btnWaEmoji").addEventListener("click", (evt) => {
    evt.preventDefault();
    toggleWaEmojiPicker();
  });

  el("waEmojiTabs").addEventListener("click", (evt) => {
    const target = evt.target?.closest?.("button[data-emoji-group]");
    const nextId = String(target?.dataset?.emojiGroup || "");
    if (!nextId || nextId === state.waEmojiCategoryId) return;
    state.waEmojiCategoryId = nextId;
    renderWaEmojiPicker();
  });

  el("waEmojiGrid").addEventListener("click", (evt) => {
    const target = evt.target?.closest?.("button[data-emoji-value]");
    const value = String(target?.dataset?.emojiValue || "");
    if (!value) return;
    insertEmojiIntoWaComposer(value);
  });

  el("btnWaClearAttachment").addEventListener("click", () => {
    setWaPendingAttachments([]);
  });

  el("btnWaSend").addEventListener("click", async () => {
    try {
      await sendWaComposerMessage();
    } catch (e) {
      toast("WhatsApp", String(e?.message || e));
    }
  });

  el("waComposerInput").addEventListener("keydown", async (evt) => {
    if (evt.key !== "Enter" || evt.shiftKey) return;
    evt.preventDefault();
    try {
      await sendWaComposerMessage();
    } catch (e) {
      toast("WhatsApp", String(e?.message || e));
    }
  });

  el("waComposerInput").addEventListener("input", () => {
    handleWaComposerInputTyping();
  });

  el("waComposerInput").addEventListener("blur", () => {
    stopWaOutgoingTyping({ sendPaused: true });
  });

  const waConversation = document.querySelector(".waConversation");
  if (waConversation) {
    waConversation.addEventListener("dragenter", (evt) => {
      if (!isWaFileDragEvent(evt)) return;
      evt.preventDefault();
      evt.stopPropagation();
      state.waDropDepth += 1;
      setWaDropActive(true);
    });

    waConversation.addEventListener("dragover", (evt) => {
      if (!isWaFileDragEvent(evt)) return;
      evt.preventDefault();
      evt.stopPropagation();
      evt.dataTransfer.dropEffect = state.waActiveChatJid ? "copy" : "none";
      setWaDropActive(true);
    });

    waConversation.addEventListener("dragleave", (evt) => {
      evt.preventDefault();
      evt.stopPropagation();
      state.waDropDepth = Math.max(0, state.waDropDepth - 1);
      if (state.waDropDepth === 0) setWaDropActive(false);
    });

    waConversation.addEventListener("drop", async (evt) => {
      if (!isWaFileDragEvent(evt)) return;
      evt.preventDefault();
      evt.stopPropagation();
      state.waDropDepth = 0;
      setWaDropActive(false);
      try {
        await handleWaFileDrop(evt);
      } catch (e) {
        toast("WhatsApp", String(e?.message || e));
      }
    });
  }

  document.addEventListener("dragover", (evt) => {
    if (!isWaFileDragEvent(evt)) return;
    evt.preventDefault();
  });

  document.addEventListener("drop", (evt) => {
    if (!isWaFileDragEvent(evt)) return;
    evt.preventDefault();
    state.waDropDepth = 0;
    setWaDropActive(false);
  });

  document.addEventListener("dragend", () => {
    state.waDropDepth = 0;
    setWaDropActive(false);
  });

  document.addEventListener("visibilitychange", () => {
    if (document.hidden) stopWaOutgoingTyping({ sendPaused: true });
  });

  window.addEventListener("blur", () => {
    stopWaOutgoingTyping({ sendPaused: true });
  });

  el("loginForm").addEventListener("submit", async (evt) => {
    evt.preventDefault();
    const email = el("loginEmail").value.trim();
    const password = el("loginPassword").value;

    if (!email || !password) {
      el("loginStatus").textContent = "Email and password are required.";
      return;
    }

    try {
      el("btnLogin").disabled = true;
      el("loginStatus").textContent = "Signing in...";
      const res = await window.api.clinicLogin({ email, password });
      state.session = res?.session || { authToken: "", user: {} };
      await afterLoginLoad();
      el("loginStatus").textContent = "";
      toast("Login", "Logged in successfully");
    } catch (e) {
      el("loginStatus").textContent = `Login failed: ${String(e?.message || e)}`;
    } finally {
      el("btnLogin").disabled = false;
    }
  });

  el("btnLogout").addEventListener("click", async () => {
    try {
      await window.api.clinicLogout();
    } catch {
      // ignore logout error
    }
    showLoginScreen();
    toast("Session", "Logged out");
  });

  el("btnLoadAppointments").addEventListener("click", async () => {
    try {
      await loadAppointments();
    } catch (e) {
      toast("Appointments", String(e?.message || e));
    }
  });

  el("btnApptSelectAll").addEventListener("click", () => {
    state.selectedAppointmentIds = new Set(state.appointments.map((a) => String(a.id)));
    renderAppointmentTable();
  });

  el("btnApptClearSelection").addEventListener("click", () => {
    state.selectedAppointmentIds = new Set();
    renderAppointmentTable();
  });

  el("btnSelectRemind").addEventListener("click", () => selectAppointmentsByRule(appointmentIsRemindEligible));
  el("btnSelectFollow").addEventListener("click", () => selectAppointmentsByRule(appointmentIsFollowEligible));
  el("btnSelectReview").addEventListener("click", () => selectAppointmentsByRule(appointmentIsFollowEligible));

  el("btnSendRemind").addEventListener("click", async () => {
    try {
      await doAppointmentSend("remindAppointment", "langRemind", "aiRemind");
    } catch (e) {
      toast("Send error", String(e?.message || e));
    }
  });

  el("btnSendFollow").addEventListener("click", async () => {
    try {
      await doAppointmentSend("followUp", "langFollow", "aiFollow");
    } catch (e) {
      toast("Send error", String(e?.message || e));
    }
  });

  el("btnSendReview").addEventListener("click", async () => {
    try {
      await doAppointmentSend("requestReview", "langReview", "aiReview");
    } catch (e) {
      toast("Send error", String(e?.message || e));
    }
  });

  el("btnAddMarketingFromText").addEventListener("click", () => {
    const lines = (el("marketingPasteInput").value || "")
      .split(/\r?\n/)
      .map((x) => x.trim())
      .filter(Boolean);

    let added = 0;
    for (const line of lines) {
      const [phoneRaw, nameRaw] = line.split(",");
      if (upsertMarketingRecipient({ phone: phoneRaw, name: (nameRaw || "").trim(), selected: true })) added++;
    }

    renderMarketingRecipients();
    refreshMarketingPreview();
    toast("Recipients", `Added ${added} entries`);
  });

  el("btnClearMarketingRecipients").addEventListener("click", () => {
    state.marketingRecipients = [];
    renderMarketingRecipients();
    refreshMarketingPreview();
  });

  el("marketingSelectAll").addEventListener("change", () => {
    const checked = !!el("marketingSelectAll").checked;
    state.marketingRecipients.forEach((row) => {
      row.selected = checked;
    });
    renderMarketingRecipients();
    refreshMarketingPreview();
  });

  el("marketingTemplateSelect").addEventListener("change", () => {
    state.currentTemplateId = el("marketingTemplateSelect").value;
    renderTemplateList();
    loadTemplateEditor();
    refreshMarketingPreview();
  });

  el("btnLoadPastPatients").addEventListener("click", async () => {
    try {
      await loadPastPatients();
      refreshMarketingPreview();
    } catch (e) {
      toast("Existing patients", String(e?.message || e));
    }
  });

  el("marketingMonthsAgo").addEventListener("input", () => {
    const range = monthRangeMonthsAgo(el("marketingMonthsAgo").value);
    el("pastPatientRangeText").textContent = `Range: ${range.label}`;
  });

  el("btnImportMarketingCsv").addEventListener("click", async () => {
    try {
      const mapping = { hasHeader: true, phoneCol: 0, varCols: { name: 1, dentist: 2, date: 3 } };
      const res = await window.api.importCsv(mapping);
      if (!res?.ok && res?.canceled) return;
      if (!res?.ok) throw new Error(res?.error || "CSV import failed");

      let added = 0;
      for (const rawPhone of res.recipients || []) {
        const vars = res.varsByPhone?.[rawPhone] || {};
        if (
          upsertMarketingRecipient({
            phone: rawPhone,
            name: vars.name || "",
            dentist: vars.dentist || "",
            apptDate: vars.date ? Date.parse(vars.date) : 0,
            selected: true
          })
        ) {
          added++;
        }
      }

      renderMarketingRecipients();
      refreshMarketingPreview();
      toast("CSV", `Imported ${added} recipients`);
    } catch (e) {
      toast("CSV import", String(e?.message || e));
    }
  });

  el("btnSendMarketing").addEventListener("click", async () => {
    try {
      await sendMarketing();
    } catch (e) {
      toast("Marketing send", String(e?.message || e));
    }
  });

  el("templateName").addEventListener("input", () => {
    readMarketingTemplateEditorToState();
    renderTemplateList();
    renderMarketingTemplateSelect();
  });

  el("templateBody").addEventListener("input", () => {
    rememberTemplateCaretPosition();
    readMarketingTemplateEditorToState();
    const t = getSelectedMarketingTemplate();
    const vars = t ? extractTemplateVariables(t.body) : [];
    el("templateVariablesText").textContent = vars.length > 0 ? `Variables: ${vars.join(", ")}` : "Variables: -";
    renderTemplatePlaceholderPreview(el("templateBody").value || "");
    renderTemplateList();
    refreshMarketingPreview();
  });
  el("templateBody").addEventListener("click", rememberTemplateCaretPosition);
  el("templateBody").addEventListener("keyup", rememberTemplateCaretPosition);
  el("templateBody").addEventListener("select", rememberTemplateCaretPosition);
  el("templateBody").addEventListener("focus", rememberTemplateCaretPosition);

  el("templateSendPolicy").addEventListener("change", () => {
    readMarketingTemplateEditorToState();
  });

  el("btnNewTemplate").addEventListener("click", () => {
    const id = `t_${Date.now().toString(16)}_${Math.random().toString(16).slice(2, 8)}`;
    state.templates.push(
      normalizeTemplate(
        {
          id,
          name: "New template",
          body: "Hello {name},",
          variables: ["name"],
          sendPolicy: "once"
        },
        state.templates.length
      )
    );
    state.currentTemplateId = id;
    renderTemplateList();
    loadTemplateEditor();
    renderMarketingTemplateSelect();
  });

  el("btnDeleteTemplate").addEventListener("click", () => {
    const selected = getSelectedMarketingTemplate();
    if (!selected) return;

    if (!window.confirm(`Delete template \"${selected.name}\"?`)) return;

    state.templates = state.templates.filter((t) => t.id !== selected.id);
    state.currentTemplateId = state.templates[0]?.id || null;
    renderTemplateList();
    loadTemplateEditor();
    renderMarketingTemplateSelect();
    refreshMarketingPreview();
  });

  el("btnSaveTemplate").addEventListener("click", async () => {
    try {
      readMarketingTemplateEditorToState();
      const normalized = state.templates.map((t, idx) => normalizeTemplate(t, idx));
      await window.api.saveTemplates(normalized);
      state.templates = normalized;
      renderTemplateList();
      renderMarketingTemplateSelect();
      toast("Templates", "Marketing templates saved");
    } catch (e) {
      toast("Templates", String(e?.message || e));
    }
  });

  el("btnSaveAppointmentTemplates").addEventListener("click", async () => {
    try {
      const payload = readAppointmentTemplatesFromUi();
      const res = await window.api.saveAppointmentTemplates(payload);
      state.appointmentTemplates = {
        ...DEFAULT_APPOINTMENT_TEMPLATES,
        ...(res?.templates || {})
      };
      applyAppointmentTemplatesToUi();
      toast("Templates", "Appointment templates saved");
    } catch (e) {
      toast("Templates", String(e?.message || e));
    }
  });

  el("btnImportTemplates").addEventListener("click", async () => {
    try {
      const res = await window.api.importTemplatesBundle();
      if (!res?.ok && res?.canceled) return;
      if (!res?.ok) throw new Error("Import failed");
      await reloadTemplateData();
      toast("Templates", `Imported. Marketing: ${res.marketingCount}`);
    } catch (e) {
      toast("Templates import", String(e?.message || e));
    }
  });

  el("btnExportTemplates").addEventListener("click", async () => {
    try {
      const res = await window.api.exportTemplatesBundle();
      if (!res?.ok && res?.canceled) return;
      if (!res?.ok) throw new Error("Export failed");
      toast("Templates", `Exported to ${res.filePath}`);
    } catch (e) {
      toast("Templates export", String(e?.message || e));
    }
  });

  el("btnSaveSettings").addEventListener("click", async () => {
    try {
      await saveSettings();
    } catch (e) {
      toast("Settings", String(e?.message || e));
    }
  });

  el("connectMethod").addEventListener("change", setConnectModeUi);
  el("btnHandshake").addEventListener("click", async () => {
    if (state.waConnToggleBusy) return;
    setConnectionBusy(true);
    try {
      await doHandshake();
    } catch (e) {
      toast("Connect", String(e?.message || e));
    } finally {
      setConnectionBusy(false);
    }
  });
  el("btnSoftDisconnect").addEventListener("click", async () => {
    if (state.waConnToggleBusy) return;
    setConnectionBusy(true);
    try {
      await doSoftDisconnect();
    } catch (e) {
      toast("Connect", String(e?.message || e));
    } finally {
      setConnectionBusy(false);
    }
  });
  el("btnConnToggle").addEventListener("click", async () => {
    try {
      await toggleConnectionFromHeader();
    } catch (e) {
      toast("Connect", String(e?.message || e));
    }
  });

  el("profileSelect").addEventListener("change", async () => {
    try {
      const id = el("profileSelect").value;
      if (!id) return;
      await activateProfileAndSync(id);
      toast("Profile", `Active profile set to ${getProfileLabelById(id)}. Click Connect to start.`);
    } catch (e) {
      toast("Profile", String(e?.message || e));
    }
  });

  el("settingProfileSelect").addEventListener("change", () => {
    state.settingsProfileId = getSelectedSettingsProfileId();
    updateSettingsProfileControls();
  });

  el("btnCreateProfileSetting").addEventListener("click", () => {
    const suggestedName = `Profile ${Math.max(1, (Array.isArray(state.profiles) ? state.profiles.length : 0) + 1)}`;
    openCreateProfileModal(suggestedName);
  });

  el("btnSettingTerminateProfile").addEventListener("click", async () => {
    const id = getSelectedSettingsProfileId();
    if (!id) return;
    const name = getProfileLabelById(id);
    if (!window.confirm(`Terminate WhatsApp session for \"${name}\"?`)) return;

    try {
      await window.api.terminateProfileSession(id);
      await refreshProfiles();
      await syncConnectionStateFromBackend().catch(() => {});
      toast("Profile", `Session terminated for ${name}`);
    } catch (e) {
      toast("Profile", String(e?.message || e));
    }
  });

  el("btnSettingDeleteProfile").addEventListener("click", async () => {
    const id = getSelectedSettingsProfileId();
    if (!id) return;
    const name = getProfileLabelById(id);
    if (!window.confirm(`Delete profile \"${name}\"? This removes saved session.`)) return;

    try {
      await window.api.deleteProfile(id);
      await refreshProfiles();
      await syncConnectionStateFromBackend().catch(() => {});
      toast("Profile", `Deleted ${name}`);
    } catch (e) {
      toast("Profile", String(e?.message || e));
    }
  });

  el("btnClearActivity").addEventListener("click", () => {
    state.activityRows = [];
    renderActivity();
  });

  el("btnConfirmSend").addEventListener("click", () => closeConfirmModal(true));
  el("btnCancelConfirm").addEventListener("click", () => closeConfirmModal(false));
  el("confirmModalBackdrop").addEventListener("click", () => closeConfirmModal(false));
  el("btnCreateProfileModalSave").addEventListener("click", async () => {
    try {
      await createProfileFromModal();
    } catch (e) {
      toast("Profile", String(e?.message || e));
    }
  });
  el("btnCreateProfileModalCancel").addEventListener("click", () => closeCreateProfileModal());
  el("createProfileModalBackdrop").addEventListener("click", () => closeCreateProfileModal());
  el("createProfileNameInput").addEventListener("keydown", async (evt) => {
    if (evt.key !== "Enter") return;
    evt.preventDefault();
    try {
      await createProfileFromModal();
    } catch (e) {
      toast("Profile", String(e?.message || e));
    }
  });
  el("waImageLightboxClose").addEventListener("click", () => closeWaImageLightbox());
  el("waImageLightbox").addEventListener("click", (evt) => {
    if (evt.target === el("waImageLightbox")) closeWaImageLightbox();
  });
  document.addEventListener("keydown", (evt) => {
    if (evt.key !== "Escape") return;
    closeCreateProfileModal();
    closeWaImageLightbox();
    closeWaEmojiPicker({ restoreFocus: false });
  });

  document.addEventListener("mousedown", (evt) => {
    if (!state.waEmojiPickerOpen) return;
    const picker = el("waEmojiPicker");
    const emojiBtn = el("btnWaEmoji");
    const target = evt.target;
    if (!picker || !emojiBtn || !target) return;
    if (picker.contains(target) || emojiBtn.contains(target)) return;
    closeWaEmojiPicker({ restoreFocus: false });
  });

  window.api.onQR((dataUrl) => {
    setQrPreview(dataUrl || "");
    updateConnectPreviewUi();
  });

  window.api.onPairingCode((code) => {
    setPairingPreview(code);
    updateConnectPreviewUi();
  });

  window.api.onStatus((status) => {
    if (status?.profileId && state.activeProfileId && status.profileId !== state.activeProfileId) return;
    const prevConnected = state.waConnected;
    const connected = !!status?.connected;
    const statusText = status?.text || "Not connected";
    const connecting = isConnectionStatusConnecting(statusText, status?.connecting);
    setConnectionBadge(connected, statusText, connecting);

    const statusTextLower = String(statusText || "").toLowerCase();
    const keepPreview =
      statusTextLower.includes("scan qr") ||
      statusTextLower.includes("pairing code") ||
      statusTextLower.includes("enter pairing code");
    if (connected) {
      clearConnectPreview({ clearPairing: true });
    } else if (!keepPreview && !connecting) {
      clearConnectPreview({ clearPairing: !statusTextLower.includes("pairing") });
    } else {
      updateConnectPreviewUi();
    }

    if (!connected) {
      stopWaOutgoingTyping({ sendPaused: false });
      clearAllWaPresenceState({ render: true });
    }

    if (!prevConnected && connected && state.waForceHistoryRefreshOnConnected) {
      state.waForceHistoryRefreshOnConnected = false;
      refreshWaChatsWithHistoryWarmup().catch(() => {});
      return;
    }

    if (!prevConnected && connected) {
      scheduleWaSyncRefresh({
        includePhotos: true,
        maxPhotoFetch: 24,
        minMinutesBetweenPhotoChecks: 45
      });
      return;
    }

    if (prevConnected !== !!status?.connected || state.activeTab === "whatsapp") {
      scheduleWaSyncRefresh();
    }
  });

  window.api.onWaChatSync((payload) => {
    if (!payload) return;
    if (payload.profileId && state.activeProfileId && payload.profileId !== state.activeProfileId) return;
    scheduleWaSyncRefresh();
  });

  window.api.onWaPresence((payload) => {
    applyWaPresenceUpdate(payload);
  });

  window.api.onBatchProgress((row) => {
    if (!row) return;
    pushActivity({
      ts: row.ts || "",
      phone: row.phone || "",
      status: row.status || "failed",
      error: row.error || ""
    });
  });
}

async function init() {
  bindEvents();
  renderMarketingPlaceholderButtons();
  await tryRestoreSession();
}

init().catch((e) => {
  toast("Init error", String(e?.message || e));
});
