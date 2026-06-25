
const TIMEZONE = "Asia/Kuala_Lumpur";
const AI_VARIATION_PROMPT =
  "Rewrite this WhatsApp clinic message naturally for Malaysian audience, keep meaning and all facts unchanged, keep it polite and concise: {message}";
const DEFAULT_CLINIC_SETTINGS = {
  timezone: TIMEZONE,
  gapMinSec: 7,
  gapMaxSec: 45,
  templateGapMinSec: 2,
  templateGapMaxSec: 4,
  marketingMonthsAgoDefault: 6,
  marketingPageSizeDefault: 35
};
const DEFAULT_APPOINTMENT_TEMPLATES = {
  remindAppointment: {
    bahasa: "Halo {name}, mau ingatkan janji temu di {branch} pada {date} jam {time}. Ada apa-apa kabari kami ya. Sampai jumpa!",
    english: "Hi {name}, this is a reminder for your appointment at {branch} on {date} at {time}. See you soon!"
  },
  followUp: {
    bahasa: "Halo {name}, harap Anda baik-baik saja setelah kunjungan di {branch}. Jika ada ketidaknyamanan, segera hubungi kami.",
    english: "Hi {name}, hope you are doing well after your visit to {branch}. Let us know if you have any discomfort."
  },
  requestReview: {
    bahasa: "Halo {name}, terima kasih telah mempercayai kami di {branch}. Boleh minta ulasan kunjungan Anda? Link Google Review: {google_review_link}",
    english: "Hi {name}, thank you for choosing us at {branch}. Could you spare a moment to review us? Google Review Link: {google_review_link}"
  }
};
const MARKETING_BLAST_LIMIT = 35;
const MARKETING_BLAST_COOLDOWN_MS = 24 * 60 * 60 * 1000;
const MARKETING_RECENT_WARNING_MS = 30 * 24 * 60 * 60 * 1000;

const MARKETING_PLACEHOLDERS = [
  { token: "{name}", key: "name", description: "Patient nickname from recipient list." },
  { token: "{branch}", key: "branch", description: "Branch selected in Marketing tab." },
  { token: "{my_branch}", key: "my_branch", description: "Your login branch from account profile." },
  { token: "{date}", key: "date", description: "Recipient appointment date, or today if unavailable." },
  { token: "{day}", key: "day", description: "Day name from appointment date, or today." },
  { token: "{time}", key: "time", description: "Recipient appointment time if available." },
  { token: "{dentist}", key: "dentist", description: "Dentist name from recipient row if available." },
  { token: "{salutation}", key: "salutation", description: "Gender-based salutation (Malay by default)." },
  { token: "{salutation_bm}", key: "salutation_bm", description: "Gender-based Malay salutation (Encik/Cik)." },
  { token: "{salutation_en}", key: "salutation_en", description: "Gender-based English salutation (Mr./Ms.)." },
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

const TEMPLATE_EDITOR_EMOJIS =
  "😀 😁 😊 🙂 🙏 👍 👋 🎉 📣 📢 📌 📍 📅 ⏰ 🦷 🪥 💬 📞 📲 ❤️ 💙 💚 ⭐ ✅ ⚠️ ❗ 👨‍⚕️ 👩‍⚕️".split(" ");

const state = {
  session: { authToken: "", user: {} },
  settings: { ...DEFAULT_CLINIC_SETTINGS },
  templates: [],
  marketingCampaigns: [],
  currentCampaignId: "",
  currentCampaignEditorId: "",
  campaignEditorCreating: false,
  campaignDraftKey: "",
  campaignDraftName: "",
  resolvedCampaign: null,
  marketingTemplateStep: 1,
  appointmentTemplates: { ...DEFAULT_APPOINTMENT_TEMPLATES },
  branches: [],
  appointments: [],
  selectedAppointmentIds: new Set(),
  apptReqSeq: 0,
  marketingRecipients: [],
  marketingLoadedPatients: [],
  marketingLoadedPage: 1,
  marketingLoadedPageSize: 35,
  marketingRecipientFilter: "not_sent",
  marketingSentStatusByPhone: {},
  marketingLocalSentAtByPhone: {},
  marketingRecentSentAtByPhone: {},
  marketingDailyLimit: {
    limit: MARKETING_BLAST_LIMIT,
    sent_last_24h: 0,
    remaining: MARKETING_BLAST_LIMIT,
    limit_reached: false,
    loaded: false
  },
  marketingBlastHistoryRows: [],
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
  templateEmojiPickerOpen: false,
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
  batchSending: false,
  batchStopPending: false,
  currentBatchQueueType: "",
  lastMarketingSkippedPhones: [],
  lastMarketingSkippedTemplateId: "",
  marketingBlastGuard: {
    limit: MARKETING_BLAST_LIMIT,
    cooldownMs: MARKETING_BLAST_COOLDOWN_MS,
    cooldownUntil: 0,
    lastBlastAt: 0,
    lastBlastCount: 0,
    remainingMs: 0,
    isLocked: false
  },
  marketingBlastCooldownTimer: null,
  activityRows: [],
  queueStats: { total: 0, byIndex: {} },
  queueActiveView: "marketing",
  currentTemplateId: null,
  currentTemplateMessageId: null,
  templateSearch: "",
  templateStatusFilter: "active",
  confirmResolver: null,
  templateBodyCaretPos: 0,
  templateDataLoadPromise: null,
  appointmentsLoadPromise: null,
  startupWarmupPromise: null,
  initialDataLoadPromise: null
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

function cleanString(input) {
  return String(input ?? "").trim();
}

function userHasDeveloperOrMarketingAccess(user) {
  const src = user && typeof user === "object" ? user : {};
  const role = cleanString(src.Role || src.role).toLowerCase();
  const dept = cleanString(src.dept).toLowerCase();
  const access = cleanString(src.Access || src.access).toLowerCase();
  return src.active === true && (dept === "marketing" || role === "developer" || access.includes("developer"));
}

function canManageMasterMarketingTemplates() {
  const user = state.session?.user || {};
  return userHasDeveloperOrMarketingAccess(user) &&
    user?.permissions?.can_manage_marketing_templates !== false;
}

function canManageMarketingCampaigns() {
  const user = state.session?.user || {};
  return userHasDeveloperOrMarketingAccess(user) &&
    user?.permissions?.can_manage_marketing_campaigns !== false;
}

function canImportMarketingLegacyLogs() {
  const user = state.session?.user || {};
  return userHasDeveloperOrMarketingAccess(user) &&
    user?.permissions?.can_import_marketing_legacy_logs !== false;
}

function canEditBranchMarketingTemplates() {
  const user = state.session?.user || {};
  return user?.permissions?.can_edit_branch_marketing_templates === true ||
    (user.active === true && !!cleanString(user.Branch));
}

function sanitizeUserFacingError(input) {
  let s = String(input ?? "");
  // Strip parser location hints like "(line 3, column 11)" from user-facing errors.
  s = s.replace(/\s*\(line\s+\d+\s*,\s*column\s+\d+\)\s*/gi, " ");
  s = s.replace(/\s*\(line\s+\d+\s*column\s+\d+\)\s*/gi, " ");
  s = s.replace(/\s*line\s+\d+\s*,\s*column\s+\d+\s*/gi, " ");
  s = s.replace(/\s*line\s+\d+\s*column\s+\d+\s*/gi, " ");
  s = s.replace(/^SyntaxError:\s*/i, "");
  s = s.replace(/\s{2,}/g, " ").trim();
  return s || "Something went wrong";
}

function toast(title, body) {
  const wrap = el("toastWrap");
  if (!wrap) return;
  const node = document.createElement("div");
  node.className = "toast";
  const safeTitle = sanitizeUserFacingError(title);
  const safeBody = sanitizeUserFacingError(body || "");
  node.innerHTML = `<div class="toastTitle">${escapeHtml(safeTitle)}</div><div class="toastBody">${escapeHtml(safeBody)}</div>`;
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

function normalizeIcNumber(input) {
  return String(input || "").trim();
}

function queueSilentPatientProfileUpdates(rows) {
  const list = Array.isArray(rows) ? rows : [];
  if (list.length === 0) return;

  const byIc = new Map();
  for (const raw of list) {
    const row = raw && typeof raw === "object" ? raw : {};
    const icNumber = normalizeIcNumber(row.ic_number || row.icNumber || row.ic);
    if (!icNumber) continue;
    const nickname = String(row.name || row.nickname || "").trim();
    const gender = normalizeGenderKey(row.gender);
    if (!nickname && !gender) continue;
    byIc.set(icNumber, {
      ic_number: icNumber,
      nickname,
      gender
    });
  }
  if (byIc.size === 0) return;

  Promise.allSettled(
    Array.from(byIc.values()).map((payload) => {
      return window.api.clinicEditPatient(payload);
    })
  ).catch(() => { });
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

function normalizeTemplateMessageType(typeRaw) {
  const type = String(typeRaw || "")
    .trim()
    .toLowerCase();
  if (type === "image" || type === "video" || type === "document") return type;
  return "text";
}

function templateMessageTypeLabel(typeRaw) {
  const type = normalizeTemplateMessageType(typeRaw);
  if (type === "image") return "Image";
  if (type === "video") return "Video";
  if (type === "document") return "Document";
  return "Text";
}

function templateAttachmentFileNameFromPath(filePath) {
  const raw = String(filePath || "").trim();
  if (!raw) return "";
  return (
    raw
      .split(/[\\/]/)
      .filter(Boolean)
      .pop() || ""
  );
}

function normalizeTemplateAttachment(raw, forcedKind = "") {
  const src = raw && typeof raw === "object" ? raw : {};
  const filePath = String(src.path || src.filePath || "").trim();
  const url = String(src.url || src.fileUrl || "").trim();
  const fileName = String(src.fileName || src.name || "").trim() || templateAttachmentFileNameFromPath(filePath);
  const mimeType = String(src.mimeType || src.type || "").trim();
  const inferredKind = waAttachmentKindFromMimeOrPath(mimeType, filePath || url);
  const kindCandidate = normalizeTemplateMessageType(forcedKind || src.kind || inferredKind);
  const kind = kindCandidate === "text" ? inferredKind : kindCandidate;
  const size = Math.max(0, Number(src.size || 0) || 0);
  const assetId = src.asset_id ?? src.assetId ?? "";
  if (!filePath && !url && !fileName && !assetId) return null;
  return {
    path: filePath,
    url,
    fileName: fileName || "Attachment",
    mimeType,
    kind,
    size,
    ...(assetId ? { asset_id: assetId, assetId } : {})
  };
}

function buildDefaultTemplateMessage(typeRaw = "text") {
  const type = normalizeTemplateMessageType(typeRaw);
  return {
    id: `tm_${Date.now().toString(16)}_${Math.random().toString(16).slice(2, 8)}`,
    type,
    text: "",
    attachment: null
  };
}

function normalizeTemplateMessage(raw, idx) {
  const src = raw && typeof raw === "object" ? raw : {};
  const type = normalizeTemplateMessageType(src.type);
  const text = String(src.text ?? src.body ?? src.caption ?? "");
  const attachment = type === "text" ? null : normalizeTemplateAttachment(src.attachment, type);
  const fallbackId = `tm_${idx + 1}`;
  return {
    id: String(src.id || fallbackId),
    order: Math.max(1, Number(src.order || idx + 1) || idx + 1),
    type,
    text,
    attachment
  };
}

function normalizeTemplateMessages(rawMessages, legacyBody = "") {
  const sourceRows = Array.isArray(rawMessages) ? rawMessages : [];
  let rows = sourceRows.map((x, idx) => normalizeTemplateMessage(x, idx)).filter(Boolean);
  if (rows.length === 0) {
    rows = [normalizeTemplateMessage({ type: "text", text: legacyBody || "" }, 0)];
  }
  if (rows.length === 0) rows = [buildDefaultTemplateMessage("text")];

  const seenIds = new Set();
  for (let i = 0; i < rows.length; i++) {
    let nextId = String(rows[i].id || "").trim();
    if (!nextId || seenIds.has(nextId)) nextId = `tm_${Date.now().toString(16)}_${i}_${Math.random().toString(16).slice(2, 6)}`;
    seenIds.add(nextId);
    rows[i].id = nextId;
    rows[i].order = Math.max(1, Number(rows[i].order || i + 1) || i + 1);
  }
  rows.sort((a, b) => Number(a.order || 0) - Number(b.order || 0));
  return rows;
}

function extractTemplateVariablesFromMessages(messages) {
  const out = new Set();
  for (const msg of Array.isArray(messages) ? messages : []) {
    for (const key of extractTemplateVariables(String(msg?.text || ""))) out.add(key);
  }
  return Array.from(out);
}

function getTemplatePrimaryBody(messages, fallback = "") {
  const rows = Array.isArray(messages) ? messages : [];
  for (const msg of rows) {
    if (normalizeTemplateMessageType(msg?.type) !== "text") continue;
    const text = String(msg?.text || "");
    if (text.trim()) return text;
  }
  return String(fallback || rows[0]?.text || "");
}

function marketingTemplateSnippet(template) {
  const t = template && typeof template === "object" ? template : {};
  const messages = normalizeTemplateMessages(t.messages, t.body || "");
  const first = messages[0] || null;
  if (!first) return "-";
  const type = normalizeTemplateMessageType(first.type);
  const text = String(first.text || "").trim();
  let head = "";
  if (type === "text") {
    head = templateSnippet(text || "(empty text)");
  } else {
    const fileLabel = String(first.attachment?.fileName || "").trim() || "(no file)";
    const caption = text ? ` ${templateSnippet(text)}` : "";
    head = `[${templateMessageTypeLabel(type)}] ${fileLabel}${caption ? ` | ${caption}` : ""}`;
  }
  if (messages.length > 1) head += ` (+${messages.length - 1} more)`;
  return head;
}

function formatTemplateTimestamp(value) {
  const raw = Number(value || 0);
  if (!raw || !Number.isFinite(raw)) return "-";
  const ms = raw < 10000000000 ? raw * 1000 : raw;
  return new Intl.DateTimeFormat("en-GB", {
    timeZone: TIMEZONE,
    day: "2-digit",
    month: "short",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false
  }).format(new Date(ms));
}

function templateStatusLabel(template) {
  return template?.active === false ? "Inactive" : "Active";
}

function templateTypeSummary(template) {
  const messages = normalizeTemplateMessages(template?.messages, template?.body || "");
  const counts = messages.reduce((acc, msg) => {
    const type = normalizeTemplateMessageType(msg?.type);
    acc[type] = (acc[type] || 0) + 1;
    return acc;
  }, {});
  const parts = Object.entries(counts).map(([type, count]) => `${count} ${templateMessageTypeLabel(type)}`);
  return parts.join(" · ") || "Text";
}

function getFilteredTemplates() {
  const search = cleanString(state.templateSearch).toLowerCase();
  const filter = cleanString(state.templateStatusFilter || "active");
  return (Array.isArray(state.templates) ? state.templates : []).filter((template) => {
    if (filter === "active" && template.active === false) return false;
    if (filter === "inactive" && template.active !== false) return false;
    if (!search) return true;
    const haystack = [template.name, marketingTemplateSnippet(template), template.created_by, template.updated_by, template.branch]
      .map((x) => cleanString(x).toLowerCase())
      .join(" ");
    return haystack.includes(search);
  });
}

function renderTemplateMetaSummary(template) {
  const target = el("templateMetaSummary");
  if (!target) return;
  if (!template) {
    target.textContent = "Select a template to edit.";
    return;
  }
  const updated = formatTemplateTimestamp(template.updated_at || template.created_at);
  const owner = cleanString(template.updated_by || template.created_by) || "-";
  target.textContent = `${templateStatusLabel(template)} · ${templateTypeSummary(template)} · Updated ${updated} · By ${owner}`;
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

function tsToYmdKl(ts) {
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: TIMEZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit"
  }).format(new Date(Number(ts || Date.now())));
}

function shiftYmdKl(ymd, deltaDays) {
  const base = ymdToKlMidnightTs(ymd);
  const days = Number(deltaDays || 0);
  if (!base || !Number.isFinite(days)) return getTodayYmdKl();
  return tsToYmdKl(base + days * 24 * 60 * 60 * 1000);
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

function formatMarketingBlastSummaryDate(ts) {
  const d = new Date(String(ts || ""));
  if (Number.isNaN(d.getTime())) return String(ts || "-");
  const day = new Intl.DateTimeFormat("en-GB", { timeZone: TIMEZONE, day: "numeric" }).format(d);
  const month = new Intl.DateTimeFormat("en-GB", { timeZone: TIMEZONE, month: "short" }).format(d);
  const year = new Intl.DateTimeFormat("en-GB", { timeZone: TIMEZONE, year: "2-digit" }).format(d);
  return `${day}-${month}-${year}, ${formatTimeForMessage(d.getTime())}`;
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

function setBatchSending(isSending) {
  state.batchSending = !!isSending;
  if (!state.batchSending) state.batchStopPending = false;
  const stopBtn = el("btnStopSending");
  if (stopBtn) {
    stopBtn.textContent = state.batchStopPending && state.batchSending ? "Stopping..." : "Stop";
    stopBtn.disabled = !state.batchSending || state.batchStopPending;
  }
  updateMarketingBlastControls();
}

function setBatchStopPending(pending) {
  state.batchStopPending = !!pending && !!state.batchSending;
  const stopBtn = el("btnStopSending");
  if (!stopBtn) return;
  stopBtn.textContent = state.batchStopPending ? "Stopping..." : "Stop";
  stopBtn.disabled = !state.batchSending || state.batchStopPending;
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

function normalizeQueueView() {
  return "marketing";
}

function setQueueActiveView(view) {
  const next = normalizeQueueView(view);
  state.queueActiveView = next;

  document.querySelectorAll(".queueModeTab").forEach((btn) => {
    const isActive = normalizeQueueView(btn.dataset.queueView) === next;
    btn.classList.toggle("active", isActive);
    btn.setAttribute("aria-selected", isActive ? "true" : "false");
  });

  document.querySelectorAll("[data-queue-panel]").forEach((panel) => {
    panel.classList.toggle("hidden", normalizeQueueView(panel.dataset.queuePanel) !== next);
  });
}

function waitForNextPaint() {
  return new Promise((resolve) => {
    if (typeof window.requestAnimationFrame === "function") {
      window.requestAnimationFrame(() => setTimeout(resolve, 0));
      return;
    }
    setTimeout(resolve, 0);
  });
}

function normalizeTemplate(raw, idx) {
  const src = raw && typeof raw === "object" ? raw : {};
  const messages = normalizeTemplateMessages(src.messages, src.body || "");
  const body = getTemplatePrimaryBody(messages, src.body || "");
  const vars = new Set(Array.isArray(src.variables) ? src.variables.map((v) => String(v)) : []);
  for (const key of extractTemplateVariablesFromMessages(messages)) vars.add(key);
  for (const key of extractTemplateVariables(body)) vars.add(key);
  const id = String(src.id || `t_${idx + 1}`);
  const sendPolicy = src.sendPolicy === "multiple" || src.send_policy === "multiple" ? "multiple" : "once";
  const rootTemplateId = src.root_template_id ?? src.rootTemplateId ?? id;
  return {
    id,
    name: String(src.name || "Untitled"),
    body,
    messages,
    variables: Array.from(vars),
    sendPolicy,
    send_policy: sendPolicy,
    active: src.active !== false,
    scope: String(src.scope || "global"),
    branch: String(src.branch || ""),
    root_template_id: rootTemplateId,
    parent_template_id: src.parent_template_id ?? src.parentTemplateId ?? null,
    version: Math.max(1, Number(src.version || 1) || 1),
    is_branch_override: src.is_branch_override === true,
    created_at: src.created_at ?? src.createdAt ?? null,
    updated_at: src.updated_at ?? src.updatedAt ?? null,
    created_by: String(src.created_by || src.createdBy || ""),
    updated_by: String(src.updated_by || src.updatedBy || ""),
    template_key: String(src.template_key || src.templateKey || ""),
    campaignDraftKey: cleanString(src.campaignDraftKey || src.campaign_draft_key || ""),
    campaignStep: Number(src.campaignStep || src.campaign_step || 0) || 0
  };
}

function preferVisibleMarketingTemplates(templates) {
  const rows = Array.isArray(templates) ? templates : [];
  const userBranch = cleanString(state.session?.user?.Branch).toLowerCase();
  const groups = new Map();
  for (const template of rows) {
    if (!template) continue;
    const rootId = cleanString(template.root_template_id || template.id);
    if (!rootId) continue;
    const list = groups.get(rootId) || [];
    list.push(template);
    groups.set(rootId, list);
  }
  const out = [];
  for (const list of groups.values()) {
    const branchMatch = list.find((template) => {
      const scope = cleanString(template.scope || "").toLowerCase();
      const branch = cleanString(template.branch).toLowerCase();
      return (scope === "branch" || template.is_branch_override === true) && branch && branch === userBranch;
    });
    const master = list.find((template) => cleanString(template.scope || "global").toLowerCase() === "global") || list[0];
    out.push(branchMatch || master);
  }
  return out.sort((a, b) => cleanString(a.name).localeCompare(cleanString(b.name)));
}

function getSelectedMarketingTemplate() {
  return state.templates.find((t) => t.id === state.currentTemplateId) || null;
}

function ensureTemplateMessages(template) {
  const t = template && typeof template === "object" ? template : null;
  if (!t) return [];
  t.messages = normalizeTemplateMessages(t.messages, t.body || "");
  return t.messages;
}

function updateTemplateDerivedFields(template) {
  const t = template && typeof template === "object" ? template : null;
  if (!t) return;
  const messages = ensureTemplateMessages(t);
  t.body = getTemplatePrimaryBody(messages, t.body || "");
  t.variables = extractTemplateVariablesFromMessages(messages);
}

function ensureSelectedTemplateMessage(template) {
  const messages = ensureTemplateMessages(template);
  if (messages.length === 0) {
    state.currentTemplateMessageId = null;
    return null;
  }
  if (!state.currentTemplateMessageId || !messages.some((x) => x.id === state.currentTemplateMessageId)) {
    state.currentTemplateMessageId = messages[0].id;
  }
  return messages.find((x) => x.id === state.currentTemplateMessageId) || messages[0] || null;
}

function getSelectedTemplateMessage(template = null) {
  const t = template || getSelectedMarketingTemplate();
  return ensureSelectedTemplateMessage(t);
}

function templateMessageListMeta(message) {
  const msg = message && typeof message === "object" ? message : {};
  const type = normalizeTemplateMessageType(msg.type);
  const text = String(msg.text || "").trim();
  if (type === "text") return templateSnippet(text || "(empty text)");
  const fileLabel = String(msg.attachment?.fileName || "").trim() || "(no file)";
  if (!text) return fileLabel;
  return `${fileLabel} | ${templateSnippet(text)}`;
}

function renderTemplateMessageList() {
  const list = el("templateMessageList");
  if (!list) return;
  list.innerHTML = "";

  const template = getSelectedMarketingTemplate();
  if (!template) {
    const empty = document.createElement("div");
    empty.className = "smallText";
    empty.textContent = "Select a template.";
    list.appendChild(empty);
    return;
  }

  const messages = ensureTemplateMessages(template);
  const selected = ensureSelectedTemplateMessage(template);
  for (let i = 0; i < messages.length; i++) {
    const msg = messages[i];
    const item = document.createElement("div");
    item.className = `templateMessageItem${selected && selected.id === msg.id ? " active" : ""}`;
    item.innerHTML = `<div class="templateMessageItemTitle">#${i + 1} ${escapeHtml(
      templateMessageTypeLabel(msg.type)
    )}</div><div class="templateMessageItemMeta">${escapeHtml(templateMessageListMeta(msg))}</div>`;
    item.addEventListener("click", () => {
      readMarketingTemplateEditorToState();
      state.currentTemplateMessageId = msg.id;
      renderTemplateMessageList();
      renderTemplateMessageEditor();
      const selectedMessage = getSelectedTemplateMessage(template);
      renderTemplatePlaceholderPreview(selectedMessage?.text || "");
      refreshMarketingPreview();
    });
    list.appendChild(item);
  }
}

function renderTemplateMessageEditor() {
  const typeSelect = el("templateMessageType");
  const emojiBtn = el("btnTemplateEmoji");
  const bodyInput = el("templateBody");
  const bodyLabel = el("templateBodyLabel");
  const attachBtn = el("btnTemplateAttachMedia");
  const clearBtn = el("btnTemplateClearMedia");
  const deleteBtn = el("btnTemplateDeleteMessage");
  const attachmentText = el("templateMessageAttachmentText");
  if (!typeSelect || !emojiBtn || !bodyInput || !bodyLabel || !attachBtn || !clearBtn || !deleteBtn || !attachmentText) return;

  const template = getSelectedMarketingTemplate();
  const message = getSelectedTemplateMessage(template);
  const messages = ensureTemplateMessages(template);
  if (!template || !message) {
    setTemplateEmojiPickerOpen(false);
    typeSelect.value = "text";
    typeSelect.disabled = true;
    emojiBtn.disabled = true;
    bodyInput.value = "";
    bodyInput.disabled = true;
    bodyLabel.textContent = "Message Text";
    bodyInput.placeholder = "Select template.";
    attachBtn.disabled = true;
    attachBtn.title = "Select a template message first";
    clearBtn.disabled = true;
    deleteBtn.disabled = true;
    attachmentText.textContent = "No media attached.";
    state.templateBodyCaretPos = 0;
    return;
  }

  const type = normalizeTemplateMessageType(message.type);
  const needsAttachment = type !== "text";
  const attachment = needsAttachment ? normalizeTemplateAttachment(message.attachment, type) : null;
  message.type = type;
  message.attachment = attachment;

  typeSelect.disabled = false;
  emojiBtn.disabled = false;
  typeSelect.value = type;
  bodyLabel.textContent = needsAttachment ? "Caption (optional)" : "Message Text";
  bodyInput.placeholder = needsAttachment ? "Optional caption for media message" : "Type message";
  bodyInput.disabled = false;
  bodyInput.value = String(message.text || "");
  state.templateBodyCaretPos = Number((message.text || "").length);

  attachBtn.disabled = false;
  attachBtn.title = needsAttachment
    ? `Attach ${templateMessageTypeLabel(type).toLowerCase()} file`
    : "Set Type to Image, Video, or Document first";
  clearBtn.disabled = !needsAttachment || !attachment;
  deleteBtn.disabled = messages.length <= 1;

  if (!needsAttachment) {
    attachmentText.textContent = "Text message. No media required.";
  } else if (attachment?.path || attachment?.url) {
    const name = String(attachment.fileName || "").trim() || templateAttachmentFileNameFromPath(attachment.path || attachment.url) || "Attachment";
    attachmentText.textContent = `${templateMessageTypeLabel(type)} attached: ${name}`;
  } else if (attachment) {
    const name = String(attachment.fileName || "").trim() || "Attachment";
    attachmentText.textContent = `${templateMessageTypeLabel(type)} attached but file path is missing: ${name}`;
  } else {
    attachmentText.textContent = `No ${templateMessageTypeLabel(type).toLowerCase()} attached.`;
  }
}

function refreshTemplateMessageEditingUi(template, message = null) {
  updateTemplateDerivedFields(template);
  renderTemplateMessageList();
  renderTemplateMessageEditor();
  refreshTemplateVariableSummary();
  renderTemplatePlaceholderPreview(message?.text || "");
  renderTemplateList();
  refreshMarketingPreview();
}

function addTemplateMessageToSelectedTemplate(typeRaw = "text") {
  const template = getSelectedMarketingTemplate();
  if (!template) return null;
  readMarketingTemplateEditorToState();
  const msg = buildDefaultTemplateMessage(typeRaw);
  ensureTemplateMessages(template).push(msg);
  state.currentTemplateMessageId = msg.id;
  refreshTemplateMessageEditingUi(template, msg);
  return msg;
}

async function attachMediaToSelectedTemplateMessage() {
  const template = getSelectedMarketingTemplate();
  if (!template) return false;
  const message = getSelectedTemplateMessage(template);
  if (!message) return false;
  const type = normalizeTemplateMessageType(message.type);
  if (type === "text") {
    toast("Template", "Click + Image, + Video, or + Document first");
    return false;
  }
  const res = await window.api.waPickAttachment();
  if (!res?.ok && res?.canceled) return false;
  if (!res?.ok) throw new Error("Attachment selection failed");
  const pickedRows = Array.isArray(res?.attachments)
    ? res.attachments
    : res?.attachment
      ? [res.attachment]
      : [];
  const normalized = normalizeWaAttachmentList(pickedRows);
  const matched = normalized.find((x) => x.kind === type);
  if (!matched) throw new Error(`Please select a ${type} file`);
  let attachment = normalizeTemplateAttachment(matched, type);
  if (attachment?.path && window.api.marketingUploadTemplateAsset) {
    toast("Template", "Uploading media to Xano...");
    const uploadRes = await window.api.marketingUploadTemplateAsset({
      path: attachment.path,
      fileName: attachment.fileName,
      mimeType: attachment.mimeType,
      kind: type,
      size: attachment.size
    });
    const asset = uploadRes?.asset || {};
    attachment = normalizeTemplateAttachment({
      asset_id: asset.id || asset.asset_id,
      fileName: asset.fileName || attachment.fileName,
      mimeType: asset.mimeType || attachment.mimeType,
      kind: asset.kind || type,
      size: asset.size || attachment.size,
      url: asset.url,
      xano_file: asset.xano_file
    }, type);
  }
  message.attachment = attachment;
  refreshTemplateMessageEditingUi(template, message);
  if (message.attachment?.url) toast("Template", "Media uploaded to Xano");
  return true;
}

function refreshTemplateVariableSummary() {
  const template = getSelectedMarketingTemplate();
  const vars = template ? extractTemplateVariablesFromMessages(ensureTemplateMessages(template)) : [];
  el("templateVariablesText").textContent = vars.length > 0 ? `Variables: ${vars.join(", ")}` : "Variables: -";
}

function formatMarketingPreviewFromMessages(messages) {
  const rows = Array.isArray(messages) ? messages : [];
  if (rows.length === 0) return "Template has no sendable messages.";
  const out = [];
  for (let i = 0; i < rows.length; i++) {
    const msg = rows[i];
    const type = normalizeTemplateMessageType(msg.type);
    out.push(`Message ${i + 1} (${templateMessageTypeLabel(type)}):`);
    if (type !== "text") {
      const fileLabel = String(msg.attachment?.fileName || "").trim() || "(no file)";
      out.push(`[${templateMessageTypeLabel(type)}] ${fileLabel}`);
    }
    if (String(msg.text || "").trim()) out.push(String(msg.text || ""));
    out.push("");
  }
  return out.join("\n").trim();
}

function buildRenderedMarketingMessages(template, vars, options = {}) {
  const opts = options && typeof options === "object" ? options : {};
  const rows = normalizeTemplateMessages(template?.messages, template?.body || "");
  const out = [];
  for (const row of rows) {
    const type = normalizeTemplateMessageType(row.type);
    const text = renderTemplate(String(row.text || ""), vars || {});
    if (type === "text") {
      if (!String(text || "").trim() && opts.keepEmptyText !== true) continue;
      out.push({ type: "text", text: String(text || ""), attachment: null });
      continue;
    }
    const attachment = normalizeTemplateAttachment(row.attachment, type);
    if (!attachment?.path && !attachment?.url && opts.includeMissingMedia !== true) continue;
    out.push({
      type,
      text: String(text || ""),
      attachment: attachment ? { ...attachment, kind: type } : null
    });
  }
  return out;
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
    sendWaChatPresence("paused", chatJid).catch(() => { });
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
    sendWaChatPresence("composing", jid).catch(() => { });
    clearWaTypingHeartbeatTimer();
    state.waTypingHeartbeatTimer = setInterval(() => {
      if (!state.waTypingActive) return;
      if (!state.waTypingChatJid) return;
      sendWaChatPresence("composing", state.waTypingChatJid).catch(() => { });
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
  label.textContent = sizeText ? `${list.length} file${list.length > 1 ? "s" : ""} | ${sizeText}` : `${list.length} file${list.length > 1 ? "s" : ""
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
      ${Number(chat.unreadCount || 0) > 0
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
  const mediaUrl = String(media.url || "").trim();

  if (thumb) {
    openWaImageLightbox(thumb, altText);
  } else if (mediaUrl) {
    openWaImageLightbox(mediaUrl, altText);
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
    if (!thumb && !mediaUrl) {
      toast("Image preview", String(e?.message || e));
    }
  }
}

function renderWaMessages(options = {}) {
  const opts = options && typeof options === "object" ? options : {};
  const viewport = el("waMessageViewport");
  if (!viewport) return;
  const activeChatKey = normalizeWaJid(state.waActiveChatJid);
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

  const visibleMessages = (Array.isArray(state.waMessages) ? state.waMessages : []).filter((msg) => {
    const msgChatKey = normalizeWaJid(msg?.chatJid || msg?.key?.remoteJid || "");
    return !!activeChatKey && msgChatKey === activeChatKey;
  });

  if (visibleMessages.length === 0) {
    viewport.innerHTML = '<div class="waEmptyState">No messages in this chat yet.</div>';
    return;
  }

  for (const msg of visibleMessages) {
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
      const mediaUrl = String(msg.media.url || "").trim();
      if (msg.media.thumbnailDataUrl || (isImageMedia && mediaUrl)) {
        const img = document.createElement("img");
        img.className = "waMediaThumb";
        img.src = msg.media.thumbnailDataUrl || mediaUrl;
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
  const changedChat = normalizeWaJid(next) !== normalizeWaJid(state.waActiveChatJid);
  state.waActiveChatJid = next;
  state.waExplicitOpenChatJid = next;
  if (changedChat) {
    state.waMessagesReqSeq += 1;
    state.waMessages = [];
    state.waLoadingMessages = true;
  }
  renderWaChatList();
  renderWaConversationHead();
  if (changedChat) renderWaMessages({ forceBottom: false });
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
    }).catch(() => { });
  }, 160);
}

function resetWaChatUiState(options = {}) {
  const opts = options && typeof options === "object" ? options : {};
  if (state.waSyncTimer) {
    clearTimeout(state.waSyncTimer);
    state.waSyncTimer = null;
  }
  stopWaOutgoingTyping({ sendPaused: false });
  clearAllWaPresenceState({ render: false });
  state.waActiveChatJid = "";
  state.waExplicitOpenChatJid = "";
  state.waMessages = [];
  state.waChats = [];
  state.waLoadingChats = false;
  state.waLoadingMessages = false;
  state.waRefreshQueued = false;
  state.waForceHistoryRefreshOnConnected = true;
  state.waChatsReqSeq += 1;
  state.waMessagesReqSeq += 1;
  if (opts.render !== false) {
    renderWaChatList();
    renderWaConversationHead();
    renderWaMessages({ forceBottom: false });
  }
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
  const src = item && typeof item === "object" ? item : {};
  const text = String(src.text || "").trim();
  const attachment = normalizeTemplateAttachment(src.attachment || null);
  const previewText = optimisticPreviewTextForSendItem(src);
  const mediaUrl = String(attachment?.url || attachment?.path || "").trim();
  const mediaKind = attachment ? normalizeTemplateMessageType(attachment.kind) : "text";
  const mediaName = String(attachment?.fileName || "").trim();
  const mediaMime = String(attachment?.mimeType || "").trim();
  const mediaSize = Number(attachment?.size || 0) || 0;
  const isRemoteMedia = /^https?:\/\//i.test(mediaUrl);
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
    type: attachment ? mediaKind : "text",
    text: text || (!attachment ? previewText : ""),
    preview: previewText,
    hasMedia: !!attachment,
    media: attachment
      ? {
        kind: mediaKind,
        mimeType: mediaMime,
        fileName: mediaName,
        fileLength: mediaSize,
        url: isRemoteMedia ? mediaUrl : "",
        thumbnailDataUrl: mediaKind === "image" && isRemoteMedia ? mediaUrl : "",
        localPath: !isRemoteMedia ? mediaUrl : ""
      }
      : null,
    status: 0,
    optimistic: true
  };
}

function marketingChatJidFromPhone(phone) {
  const msisdn = normalizePhone(phone);
  return isValidPhone(msisdn) ? `${msisdn}@s.whatsapp.net` : "";
}

function hasRecentOutgoingWaText(text) {
  const needle = String(text || "").trim();
  if (!needle) return false;
  const now = Date.now();
  return (Array.isArray(state.waMessages) ? state.waMessages : []).some((msg) => {
    if (msg?.fromMe !== true) return false;
    const msgText = String(msg?.text || msg?.preview || "").trim();
    const ts = Number(msg?.timestampMs || 0);
    return msgText === needle && (!ts || now - ts < 10 * 60 * 1000);
  });
}

async function openMarketingSentChatAfterSend(sendRows, varsByPhone, template, result) {
  const res = result && typeof result === "object" ? result : {};
  if (Number(res.sent || 0) <= 0) return;
  const rows = Array.isArray(sendRows) ? sendRows : [];
  const skippedSet = new Set((Array.isArray(res.skippedAlreadySentPhones) ? res.skippedAlreadySentPhones : []).map((x) => normalizePhone(x)));
  const firstSent = rows.find((row) => {
    const phone = normalizePhone(row?.phone);
    return phone && !skippedSet.has(phone);
  }) || rows[0] || null;
  const chatJid = marketingChatJidFromPhone(firstSent?.phone);
  if (!chatJid) return;

  const phone = normalizePhone(firstSent.phone);
  const vars = (varsByPhone && (varsByPhone[firstSent.phone] || varsByPhone[phone])) || {};
  const renderedMessages = buildRenderedMarketingMessages(template, vars, {
    includeMissingMedia: true,
    keepEmptyText: true
  });
  const optimisticItems = renderedMessages
    .map((msg) => ({
      text: String(msg?.text || ""),
      attachment: msg?.attachment || null
    }))
    .filter((item) => String(item.text || "").trim() || item.attachment);

  setActiveTab("whatsapp");
  await refreshWaChats({
    refreshMessages: false,
    markRead: false,
    ensureHistory: true,
    forceHistory: true,
    includePhotos: false
  }).catch(() => { });
  await openWaChat(chatJid).catch(() => { });

  const missingItems = optimisticItems.filter((item) => {
    const text = String(item.text || "").trim();
    return !text || !hasRecentOutgoingWaText(text);
  });
  if (missingItems.length > 0 && state.waActiveChatJid === chatJid) {
    const optimisticMessages = missingItems.map((item, idx) => buildOptimisticWaMessage(chatJid, item, idx));
    state.waMessages = [...(Array.isArray(state.waMessages) ? state.waMessages : []), ...optimisticMessages];
    renderWaMessages({ forceBottom: true });
  }
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
      }).then((sendRes) => {
        if (sendRes?.ok === false) throw new Error(sendRes.error || "WhatsApp send failed");
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
    await refreshWaMessages({ markRead: false, forceBottom: true, showLoading: false }).catch(() => { });
    await refreshWaChats({ refreshMessages: false, markRead: false }).catch(() => { });
    throw e;
  } finally {
    await refreshWaMessages({ markRead: false, forceBottom: true, showLoading: false }).catch(() => { });
    await refreshWaChats({ refreshMessages: false, markRead: false }).catch(() => { });
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

function makeQueueStats(total = 0) {
  return {
    total: Math.max(0, Number(total || 0) || 0),
    byIndex: {}
  };
}

function getQueueStatsCounts() {
  const stats = state.queueStats && typeof state.queueStats === "object" ? state.queueStats : makeQueueStats(0);
  const byIndex = stats.byIndex && typeof stats.byIndex === "object" ? stats.byIndex : {};
  let tried = 0;
  let success = 0;
  let failed = 0;
  let skipped = 0;

  for (const statusRaw of Object.values(byIndex)) {
    const status = String(statusRaw || "")
      .trim()
      .toLowerCase();
    if (status === "sent") success++;
    else if (status === "failed") failed++;
    else if (status === "skipped") skipped++;
    if (status === "sending" || status === "sent" || status === "failed") tried++;
  }

  return {
    total: Math.max(0, Number(stats.total || 0) || 0),
    tried,
    success,
    failed,
    skipped
  };
}

function renderQueueStats() {
  const node = el("queueSummaryText");
  if (!node) return;
  const stats = getQueueStatsCounts();
  let text = `${stats.tried}/${stats.total} (success: ${stats.success}, failed: ${stats.failed}`;
  if (stats.skipped > 0) text += `, skipped: ${stats.skipped}`;
  text += ")";
  node.textContent = text;
}

function resetQueueStats(total = 0) {
  state.queueStats = makeQueueStats(total);
  renderQueueStats();
}

function updateQueueStatsFromProgress(row) {
  const src = row && typeof row === "object" ? row : {};
  const status = String(src.status || "")
    .trim()
    .toLowerCase();
  const idx = Math.max(0, Number(src.index || 0) || 0);
  const total = Math.max(0, Number(src.total || 0) || 0);

  if (!state.queueStats || typeof state.queueStats !== "object") {
    state.queueStats = makeQueueStats(total);
  }
  if (total > 0 && state.queueStats.total === 0) {
    state.queueStats.total = total;
  }

  const isBatchStart = idx === 1 && status === "sending" && total > 0;
  if (isBatchStart && (state.queueStats.total !== total || Object.keys(state.queueStats.byIndex || {}).length > 0)) {
    state.queueStats = makeQueueStats(total);
  }

  if (idx > 0 && ["sending", "sent", "failed", "skipped"].includes(status)) {
    const key = String(idx);
    const prev = String(state.queueStats.byIndex?.[key] || "")
      .trim()
      .toLowerCase();
    const prevIsFinal = prev === "sent" || prev === "failed" || prev === "skipped";
    if (!(prevIsFinal && status === "sending")) {
      state.queueStats.byIndex[key] = status;
    }
  }

  renderQueueStats();
}

function renderActivity() {
  const tbody = el("activityBody");
  if (!tbody) return;
  tbody.innerHTML = "";
  const rows = state.activityRows.filter((row) => String(row?.queueType || "appointment") === "appointment");
  for (const row of rows) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${escapeHtml(row.ts || "")}</td>
      <td>${escapeHtml(row.phone || "")}</td>
      <td>${statusPill(row.status)}</td>
      <td>${escapeHtml(row.error || "")}</td>
    `;
    tbody.appendChild(tr);
  }
  if (rows.length === 0) {
    const tr = document.createElement("tr");
    tr.innerHTML = '<td colspan="4" class="smallText">No appointment send activity yet.</td>';
    tbody.appendChild(tr);
  }
  renderQueueStats();
}

function renderMarketingBlastHistory() {
  const tbody = el("marketingBlastHistoryBody");
  if (!tbody) return;
  tbody.innerHTML = "";

  const rows = Array.isArray(state.marketingBlastHistoryRows) ? state.marketingBlastHistoryRows : [];
  for (const row of rows) {
    const dateTime = row.date_time || row.dateTime || row.ts || row.completed_at || row.started_at || "-";
    const profile = row.profile || row.profile_name || row.profileName || "Dentabay";
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${escapeHtml(formatMarketingBlastSummaryDate(dateTime))}</td>
      <td>${escapeHtml(row.branch || "-")}</td>
      <td>${escapeHtml(profile || "Dentabay")}</td>
      <td>${escapeHtml(String(row.total ?? 0))}</td>
      <td>${escapeHtml(String(row.sent ?? 0))}</td>
      <td>${escapeHtml(String(row.skipped ?? 0))}</td>
      <td>${escapeHtml(String(row.failed ?? 0))}</td>
    `;
    tbody.appendChild(tr);
  }

  if (rows.length === 0) {
    const tr = document.createElement("tr");
    tr.innerHTML = '<td colspan="7" class="smallText">No marketing blast history yet.</td>';
    tbody.appendChild(tr);
  }
}

function pushActivity(row) {
  const rawError = String(row?.error || "").trim();
  state.activityRows.unshift({
    ...row,
    queueType: String(row?.queueType || state.currentBatchQueueType || "appointment"),
    error: rawError ? sanitizeUserFacingError(rawError) : ""
  });
  if (state.activityRows.length > 500) state.activityRows = state.activityRows.slice(0, 500);
  renderActivity();
}

function renderBranchesToSelect(selectId, defaultBranch) {
  const select = el(selectId);
  if (!select) return;
  select.innerHTML = "";
  const preferredBranch = cleanString(defaultBranch);
  const marketingHqBranch = "AI Venture";
  const isMarketingSelect = selectId === "marketingBranchSelect";
  const appendOption = (value) => {
    const label = cleanString(value);
    if (!label || Array.from(select.options).some((opt) => opt.value === label)) return;
    const opt = document.createElement("option");
    opt.value = label;
    opt.textContent = label;
    select.appendChild(opt);
  };
  if (isMarketingSelect && preferredBranch) appendOption(preferredBranch);
  if (isMarketingSelect) appendOption(marketingHqBranch);
  for (const branch of state.branches) appendOption(branch.label);
  if (select.options.length === 0) return;
  const preferred = preferredBranch && Array.from(select.options).some((opt) => opt.value === preferredBranch)
    ? preferredBranch
    : state.branches.find((branch) => cleanString(branch.label))?.label || select.options[0].value;
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

  const patientName = String(src.nickname || src.Patient_Name || src.name || "").trim();
  setActiveTab("whatsapp");
  upsertLocalWaChatStub(chatJid, patientName);
  // openWaChat handles the message fetch and UI refresh internally.
  await openWaChat(chatJid);
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
    const patientName = String(appt.nickname || appt.Patient_Name || "").trim();
    tdPatient.innerHTML = `<strong>${escapeHtml(patientName || "-")}</strong><br/><span class="smallText">${escapeHtml(
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
    waBtn.setAttribute("aria-label", `Open WhatsApp chat with ${String(patientName || "patient")}`);
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
  const incomingIcNumber = normalizeIcNumber(src.ic_number || src.icNumber);

  const existing = state.marketingRecipients.find((x) => x.phone === phone);
  if (existing) {
    if (!existing.name && src.name) existing.name = String(src.name);
    if (!existing.dentist && src.dentist) existing.dentist = String(src.dentist);
    if (!existing.gender && src.gender) existing.gender = String(src.gender).trim().toLowerCase();
    if (!existing.ic_number && incomingIcNumber) existing.ic_number = incomingIcNumber;
    const nextApptDate = toInt(src.apptDate, 0);
    if (nextApptDate && (!existing.apptDate || nextApptDate > toInt(existing.apptDate, 0))) existing.apptDate = nextApptDate;

    const nextApptStart = toInt(src.apptStartTime, 0);
    if (nextApptStart && (!existing.apptStartTime || nextApptStart > toInt(existing.apptStartTime, 0)))
      existing.apptStartTime = nextApptStart;

    if (src.selected === true) existing.selected = true;
    return false;
  }

  state.marketingRecipients.push({
    id: `m_${phone}_${Date.now().toString(16)}_${Math.random().toString(16).slice(2, 7)}`,
    phone,
    name: String(src.name || "").trim(),
    dentist: String(src.dentist || "").trim(),
    gender: String(src.gender || "").trim().toLowerCase(),
    ic_number: incomingIcNumber,
    apptDate: toInt(src.apptDate, 0),
    apptStartTime: toInt(src.apptStartTime, 0),
    selected: src.selected !== false
  });
  return true;
}

function parseMarketingManualRecipientLine(lineRaw) {
  const line = String(lineRaw || "").trim();
  if (!line) return null;

  const phoneCandidate = findMarketingManualPhoneCandidate(line);
  if (!phoneCandidate) {
    return {
      phoneRaw: "",
      nameRaw: cleanMarketingManualRecipientName(line)
    };
  }

  const nameRaw = cleanMarketingManualRecipientName(
    `${line.slice(0, phoneCandidate.index)} ${line.slice(phoneCandidate.end)}`
  );
  return {
    phoneRaw: phoneCandidate.raw,
    nameRaw
  };
}

function cleanMarketingManualRecipientName(input) {
  return String(input || "")
    .replace(/[\t,;]+/g, " ")
    .replace(/\s+/g, " ")
    .replace(/^[\s:|/\\-]+|[\s:|/\\-]+$/g, "")
    .trim();
}

function scoreMarketingManualPhoneCandidate(raw) {
  const text = String(raw || "").trim();
  const compact = text.replace(/[()\s.-]+/g, "");
  const digits = compact.replace(/\D/g, "");
  if (digits.length < 8 || digits.length > 15) return 0;
  if (!isValidPhone(text)) return 0;
  if (compact.startsWith("+60")) return 100;
  if (digits.startsWith("60")) return 95;
  if (digits.startsWith("01")) return 90;
  if (digits.startsWith("0")) return 80;
  if (compact.startsWith("+")) return 60;
  return 0;
}

function findMarketingManualPhoneCandidate(input) {
  const text = String(input || "");
  const matches = Array.from(text.matchAll(/\+?\d[\d\s().-]{6,}\d/g));
  let best = null;
  for (const match of matches) {
    const raw = String(match[0] || "").trim();
    const score = scoreMarketingManualPhoneCandidate(raw);
    if (score <= 0) continue;
    const index = Number(match.index || 0);
    const candidate = {
      raw,
      index,
      end: index + String(match[0] || "").length,
      score
    };
    if (!best || candidate.score > best.score || (candidate.score === best.score && candidate.index < best.index)) {
      best = candidate;
    }
  }
  return best;
}

function getSelectedMarketingRecipients() {
  return state.marketingRecipients.filter((x) => x.selected);
}

function getSelectedMarketingLoadedPatients() {
  return state.marketingLoadedPatients.filter((x) => x.selected);
}

function normalizeMarketingBlastGuard(rawGuard) {
  const src = rawGuard && typeof rawGuard === "object" ? rawGuard : {};
  const limit = Math.max(1, Number(src.limit || MARKETING_BLAST_LIMIT));
  const cooldownMs = Math.max(0, Number(src.cooldownMs || MARKETING_BLAST_COOLDOWN_MS));
  const cooldownUntil = Math.max(0, Number(src.cooldownUntil || 0));
  const lastBlastAt = Math.max(0, Number(src.lastBlastAt || 0));
  const lastBlastCount = Math.max(0, Number(src.lastBlastCount || 0));
  const remainingMsRaw = Math.max(0, cooldownUntil - Date.now());
  const shouldLock = lastBlastCount >= limit && remainingMsRaw > 0;
  return {
    limit,
    cooldownMs,
    cooldownUntil: shouldLock ? cooldownUntil : 0,
    lastBlastAt,
    lastBlastCount,
    remainingMs: shouldLock ? remainingMsRaw : 0,
    isLocked: shouldLock
  };
}

function formatMarketingBlastRemaining(msRaw) {
  const totalSec = Math.max(0, Math.ceil(Number(msRaw || 0) / 1000));
  const hours = Math.floor(totalSec / 3600);
  const minutes = Math.floor((totalSec % 3600) / 60);
  const seconds = totalSec % 60;
  if (hours > 0) return `${hours}h ${minutes}m`;
  if (minutes > 0) return `${minutes}m ${seconds}s`;
  return `${seconds}s`;
}

function updateMarketingBlastControls() {
  const dailyLimit = normalizeMarketingDailyLimit(state.marketingDailyLimit);
  state.marketingDailyLimit = dailyLimit;
  const selectedCount = getSelectedMarketingRecipients().length;
  const eligibleSelectedCount = getSelectedMarketingRecipients().filter((row) => marketingRecipientStatusInfo(row).kind !== "already_sent").length;
  const overLimit = eligibleSelectedCount > dailyLimit.limit || (dailyLimit.loaded && eligibleSelectedCount > dailyLimit.remaining);
  const btn = el("btnSendMarketing");
  const limitText = el("marketingBlastLimitText");
  const cooldownText = el("marketingBlastCooldownText");
  const hasTemplate = !!getSelectedMarketingTemplate();

  if (limitText) {
    limitText.textContent = `Selected: ${selectedCount} · Eligible: ${eligibleSelectedCount}`;
  }

  if (cooldownText) {
    if (dailyLimit.limit_reached) {
      cooldownText.textContent = `Daily marketing limit reached. Sent last 24h: ${dailyLimit.sent_last_24h} / ${dailyLimit.limit} · Remaining: 0`;
    } else if (eligibleSelectedCount > dailyLimit.limit) {
      cooldownText.textContent = `Maximum marketing blast is ${dailyLimit.limit} contacts. Please reduce the list.`;
    } else if (overLimit) {
      cooldownText.textContent = `Selected exceeds remaining daily limit. Sent last 24h: ${dailyLimit.sent_last_24h} / ${dailyLimit.limit} · Remaining: ${dailyLimit.remaining}`;
    } else if (dailyLimit.loaded) {
      cooldownText.textContent = `Sent last 24h: ${dailyLimit.sent_last_24h} / ${dailyLimit.limit} · Remaining: ${dailyLimit.remaining}`;
    } else {
      cooldownText.textContent = `Select a template and load recipients to check the rolling 24-hour limit.`;
    }
  }

  if (btn) {
    btn.disabled = !!state.batchSending || !hasTemplate || dailyLimit.limit_reached || overLimit || eligibleSelectedCount === 0;
  }
}

function startMarketingBlastCooldownTicker() {
  if (state.marketingBlastCooldownTimer) {
    clearInterval(state.marketingBlastCooldownTimer);
    state.marketingBlastCooldownTimer = null;
  }
  state.marketingBlastCooldownTimer = setInterval(() => {
    state.marketingBlastGuard = normalizeMarketingBlastGuard(state.marketingBlastGuard);
    updateMarketingBlastControls();
  }, 1000);
}

async function refreshMarketingBlastGuard() {
  try {
    const res = await window.api.waGetMarketingBlastGuard();
    state.marketingBlastGuard = normalizeMarketingBlastGuard(res?.guard);
  } catch {
    state.marketingBlastGuard = normalizeMarketingBlastGuard(null);
  }
  updateMarketingBlastControls();
}

function currentMarketingTemplateId() {
  const template = getSelectedMarketingTemplate();
  return template ? `marketing_${template.id}` : "";
}

function selectedMarketingTemplateApiIds(template = getSelectedMarketingTemplate()) {
  const t = template && typeof template === "object" ? template : null;
  if (!t) return { template_id: "", root_template_id: "" };
  const templateId = cleanString(t.id);
  const rootId = cleanString(t.root_template_id || t.rootTemplateId || t.id);
  return {
    template_id: Number(templateId) || templateId,
    root_template_id: Number(rootId) || rootId
  };
}

function isAiVentureMarketingBranch(branch) {
  return cleanString(branch).toLowerCase() === "ai venture";
}

function currentMarketingBranchForLimit() {
  return cleanString(el("marketingBranchSelect")?.value) || cleanString(state.session?.user?.Branch);
}

function currentMarketingProfileForLimit() {
  return "Dentabay";
}

function normalizeMarketingDailyLimit(raw) {
  const src = raw && typeof raw === "object" ? raw : {};
  const limit = Math.max(1, Number(src.limit || MARKETING_BLAST_LIMIT) || MARKETING_BLAST_LIMIT);
  const sent = Math.max(0, Number(src.sent_last_24h ?? src.sentLast24h ?? 0) || 0);
  const remaining = Math.max(0, Number(src.remaining ?? Math.max(0, limit - sent)) || 0);
  return {
    limit,
    sent_last_24h: sent,
    remaining,
    limit_reached: src.limit_reached === true || src.limitReached === true || remaining <= 0,
    loaded: src.loaded === true
  };
}

function marketingRecipientSentAt(phone) {
  const normalized = normalizePhone(phone);
  const localMap = state.marketingLocalSentAtByPhone && typeof state.marketingLocalSentAtByPhone === "object"
    ? state.marketingLocalSentAtByPhone
    : {};
  if (cleanString(localMap[normalized])) return cleanString(localMap[normalized]);
  const map = state.marketingSentStatusByPhone && typeof state.marketingSentStatusByPhone === "object"
    ? state.marketingSentStatusByPhone
    : {};
  return cleanString(map[normalized] || "");
}

function marketingRecipientRecentSentAt(phone) {
  const normalized = normalizePhone(phone);
  const map = state.marketingRecentSentAtByPhone && typeof state.marketingRecentSentAtByPhone === "object"
    ? state.marketingRecentSentAtByPhone
    : {};
  return cleanString(map[normalized] || "");
}

function parseSentAtMs(sentAt) {
  const raw = cleanString(sentAt);
  if (!raw) return 0;
  const withTime = Date.parse(raw);
  if (Number.isFinite(withTime)) return withTime;
  const normalized = Date.parse(raw.replace(" ", "T"));
  return Number.isFinite(normalized) ? normalized : 0;
}

function marketingRecentWarningInfo(phone) {
  const sentAt = marketingRecipientRecentSentAt(phone);
  return marketingRecentWarningInfoFromSentAt(sentAt);
}

function marketingRecentWarningInfoFromSentAt(sentAtRaw) {
  const sentAt = cleanString(sentAtRaw);
  const sentMs = parseSentAtMs(sentAt);
  if (!sentMs) return { isRecent: false, sentAt: "", daysAgo: 0, label: "" };
  const ageMs = Date.now() - sentMs;
  if (ageMs < 0 || ageMs > MARKETING_RECENT_WARNING_MS) {
    return { isRecent: false, sentAt, daysAgo: 0, label: "" };
  }
  const daysAgo = Math.max(0, Math.floor(ageMs / (24 * 60 * 60 * 1000)));
  const label = daysAgo <= 0 ? "Marketing sent today" : `Marketing sent ${daysAgo} day${daysAgo === 1 ? "" : "s"} ago`;
  return { isRecent: true, sentAt, daysAgo, label };
}

async function getMarketingStatusMapsForPhones(phones, options = {}) {
  const opts = options && typeof options === "object" ? options : {};
  const phoneList = Array.isArray(phones) ? phones : [];
  if (phoneList.length === 0) {
    return { sentAtByPhone: {}, latestMarketingSentAtByPhone: {}, statusByPhone: {} };
  }
  const template = getSelectedMarketingTemplate();
  const templateIds = selectedMarketingTemplateApiIds(template);
  if (templateIds.template_id && templateIds.root_template_id && window.api.marketingCheckStatus) {
    try {
      const res = await window.api.marketingCheckStatus({
        template_id: templateIds.template_id,
        root_template_id: templateIds.root_template_id,
        branch: currentMarketingBranchForLimit(),
        profile: currentMarketingProfileForLimit(),
        phones: phoneList
      });
      state.marketingDailyLimit = normalizeMarketingDailyLimit({
        ...(res?.daily_limit && typeof res.daily_limit === "object" ? res.daily_limit : {}),
        loaded: true
      });
      const statusByPhone = {};
      const sentAtByPhone = {};
      const recentByPhone = {};
      for (const raw of Array.isArray(res?.statuses) ? res.statuses : []) {
        const phone = normalizePhone(raw?.phone || "");
        if (!phone) continue;
        statusByPhone[phone] = raw;
        const sentAt = raw?.last_sent_at || raw?.sent_at || raw?.template_sent_at || "";
        if (sentAt) sentAtByPhone[phone] = cleanString(sentAt);
        const recent = raw?.recent_marketing_sent_at || raw?.last_sent_at || "";
        if (recent) recentByPhone[phone] = cleanString(recent);
      }
      return {
        sentAtByPhone,
        latestMarketingSentAtByPhone: recentByPhone,
        statusByPhone
      };
    } catch (e) {
      if (opts.throwOnError) throw e;
      toast("Marketing status", String(e?.message || e));
      return { sentAtByPhone: {}, latestMarketingSentAtByPhone: {}, statusByPhone: {} };
    }
  }
  const templateId = currentMarketingTemplateId();
  if (!templateId) {
    return { sentAtByPhone: {}, latestMarketingSentAtByPhone: {}, statusByPhone: {} };
  }
  try {
    const res = await window.api.waGetSentStatusForTemplate({ templateId, phones: phoneList });
    return {
      sentAtByPhone: res?.sentAtByPhone && typeof res.sentAtByPhone === "object" ? res.sentAtByPhone : {},
      latestMarketingSentAtByPhone:
        res?.latestMarketingSentAtByPhone && typeof res.latestMarketingSentAtByPhone === "object"
          ? res.latestMarketingSentAtByPhone
          : {},
      statusByPhone: {}
    };
  } catch {
    return { sentAtByPhone: {}, latestMarketingSentAtByPhone: {}, statusByPhone: {} };
  }
}

function applyMarketingStatusMapsToRows(rows, statusMaps) {
  const list = Array.isArray(rows) ? rows : [];
  const maps = statusMaps && typeof statusMaps === "object" ? statusMaps : {};
  const sentMap = maps.sentAtByPhone && typeof maps.sentAtByPhone === "object" ? maps.sentAtByPhone : {};
  const localSentMap = state.marketingLocalSentAtByPhone && typeof state.marketingLocalSentAtByPhone === "object"
    ? state.marketingLocalSentAtByPhone
    : {};
  const recentMap =
    maps.latestMarketingSentAtByPhone && typeof maps.latestMarketingSentAtByPhone === "object"
      ? maps.latestMarketingSentAtByPhone
      : {};
  const statusMap = maps.statusByPhone && typeof maps.statusByPhone === "object" ? maps.statusByPhone : {};
  for (const row of list) {
    const phone = normalizePhone(row?.phone || "");
    const backendStatus = statusMap[phone] || null;
    row.marketingStatus = backendStatus;
    row.campaignSentAt = backendStatus ? cleanString(sentMap[phone] || "") : cleanString(localSentMap[phone] || sentMap[phone] || "");
    row.recentMarketingSentAt = cleanString(recentMap[phone] || "");
    const status = cleanString(row.marketingStatus?.status || "");
    const canSend = row.marketingStatus?.can_send === true;
    if (row.campaignSentAt || (status && !canSend)) row.selected = false;
  }
}

function marketingRecipientStatusInfo(row) {
  const forcedSentAt = cleanString(row?.campaignSentAt || "") || marketingRecipientSentAt(row?.phone);
  if (forcedSentAt) {
    return {
      kind: "already_sent",
      label: "Already Sent",
      sentAt: forcedSentAt,
      recent: marketingRecentWarningInfoFromSentAt(cleanString(row?.recentMarketingSentAt || "") || marketingRecipientRecentSentAt(row?.phone))
    };
  }
  const backendStatus = row?.marketingStatus && typeof row.marketingStatus === "object" ? row.marketingStatus : null;
  if (backendStatus) {
    const canSend = backendStatus.can_send === true;
    const status = cleanString(backendStatus.status);
    const sentAt = cleanString(backendStatus.last_sent_at || backendStatus.sent_at || backendStatus.template_sent_at);
    const label = canSend ? (cleanString(backendStatus.label) || "Not Sent Yet") : "Already Sent";
    return {
      kind: canSend && status !== "already_sent" ? "not_sent" : "already_sent",
      label,
      sentAt,
      recent: marketingRecentWarningInfoFromSentAt(backendStatus.recent_marketing_sent_at || "")
    };
  }
  const sentAt = cleanString(row?.campaignSentAt || "") || marketingRecipientSentAt(row?.phone);
  const recentSentAt = cleanString(row?.recentMarketingSentAt || "") || marketingRecipientRecentSentAt(row?.phone);
  const recent = marketingRecentWarningInfoFromSentAt(recentSentAt);
  if (sentAt) {
    return {
      kind: "already_sent",
      label: "Already Sent",
      sentAt,
      recent
    };
  }
  return {
    kind: "not_sent",
    label: "Not Sent Yet",
    sentAt: "",
    recent
  };
}

function getFilteredMarketingRecipients() {
  const mode = String(state.marketingRecipientFilter || "all");
  const rows = Array.isArray(state.marketingRecipients) ? state.marketingRecipients : [];
  if (mode === "already_sent") {
    return rows.filter((row) => marketingRecipientStatusInfo(row).kind === "already_sent");
  }
  if (mode === "not_sent") {
    return rows.filter((row) => marketingRecipientStatusInfo(row).kind !== "already_sent");
  }
  return rows;
}

async function refreshMarketingRecipientStatuses() {
  const templateId = currentMarketingTemplateId();
  const rows = Array.isArray(state.marketingRecipients) ? state.marketingRecipients : [];
  const phones = rows.map((row) => row.phone);
  if (!templateId || phones.length === 0) {
    state.marketingSentStatusByPhone = {};
    state.marketingRecentSentAtByPhone = {};
    renderMarketingRecipients();
    updateMarketingBlastControls();
    return;
  }
  try {
    const maps = await getMarketingStatusMapsForPhones(phones);
    applyMarketingStatusMapsToRows(rows, maps);
    state.marketingSentStatusByPhone = maps.sentAtByPhone && typeof maps.sentAtByPhone === "object" ? maps.sentAtByPhone : {};
    state.marketingRecentSentAtByPhone = maps.latestMarketingSentAtByPhone && typeof maps.latestMarketingSentAtByPhone === "object"
      ? maps.latestMarketingSentAtByPhone
      : {};
  } catch {
    state.marketingSentStatusByPhone = {};
    state.marketingRecentSentAtByPhone = {};
  }
  renderMarketingRecipients();
  updateMarketingBlastControls();
}

async function refreshMarketingLoadedPatientStatuses() {
  const templateId = currentMarketingTemplateId();
  const rows = Array.isArray(state.marketingLoadedPatients) ? state.marketingLoadedPatients : [];
  const phones = rows.map((row) => row.phone);
  if (!templateId || phones.length === 0) {
    renderMarketingLoadedPatients();
    updateMarketingBlastControls();
    return;
  }
  const statusMaps = await getMarketingStatusMapsForPhones(phones);
  applyMarketingStatusMapsToRows(rows, statusMaps);
  renderMarketingLoadedPatients();
  updateMarketingBlastControls();
}

function marketingBlastHistoryRowTime(row) {
  const raw = row?.date_time || row?.dateTime || row?.ts || row?.completed_at || row?.started_at || "";
  const time = new Date(String(raw || "")).getTime();
  return Number.isFinite(time) ? time : 0;
}

function marketingBlastHistoryDedupeKey(row) {
  const minute = Math.floor(marketingBlastHistoryRowTime(row) / 60000);
  return [
    minute || cleanString(row?.date_time || row?.dateTime || row?.ts),
    cleanString(row?.branch).toLowerCase(),
    cleanString(row?.profile || row?.profile_name || row?.profileName || "Dentabay").toLowerCase(),
    String(row?.campaign_id ?? ""),
    String(row?.template_step ?? ""),
    String(row?.total ?? 0),
    String(row?.sent ?? 0),
    String(row?.skipped ?? 0),
    String(row?.failed ?? 0)
  ].join("|");
}

function mergeMarketingBlastHistoryRows(remoteRows, localRows) {
  const out = [];
  const seen = new Set();
  for (const row of [...(remoteRows || []), ...(localRows || [])]) {
    if (!row || typeof row !== "object") continue;
    const key = marketingBlastHistoryDedupeKey(row);
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(row);
  }
  return out
    .sort((a, b) => marketingBlastHistoryRowTime(b) - marketingBlastHistoryRowTime(a))
    .slice(0, 100);
}

function addImmediateMarketingBlastHistoryRow(result) {
  const src = result && typeof result === "object" ? result : {};
  const total = Math.max(0, Number(src.total ?? (Number(src.sent || 0) + Number(src.failed || 0) + Number(src.skipped || 0))) || 0);
  if (total <= 0) return;
  const row = {
    date_time: new Date().toISOString(),
    branch: cleanString(state.session?.user?.Branch) || cleanString(el("marketingBranchSelect")?.value) || "-",
    profile: "Dentabay",
    total,
    sent: Math.max(0, Number(src.sent || 0)),
    skipped: Math.max(0, Number(src.skipped || 0)),
    failed: Math.max(0, Number(src.failed || 0))
  };
  state.marketingBlastHistoryRows = mergeMarketingBlastHistoryRows([row], state.marketingBlastHistoryRows);
  renderMarketingBlastHistory();
}

async function refreshMarketingBlastHistory() {
  let remoteRows = [];
  let localRows = [];
  try {
    const res = await window.api.marketingGetBlastHistory({ limit: 100 });
    remoteRows = Array.isArray(res?.rows) ? res.rows : [];
  } catch {
    remoteRows = [];
  }
  try {
    const localRes = await window.api.waGetMarketingBlastHistory();
    localRows = Array.isArray(localRes?.rows) ? localRes.rows : [];
  } catch {
    localRows = [];
  }
  state.marketingBlastHistoryRows = mergeMarketingBlastHistoryRows(remoteRows, localRows);
  renderMarketingBlastHistory();
}

function refreshMarketingSummary() {
  const filterSelect = el("marketingRecipientFilter");
  if (filterSelect) filterSelect.value = String(state.marketingRecipientFilter || "all");
  const filteredRows = getFilteredMarketingRecipients();
  el("marketingSummary").textContent = `${getSelectedMarketingRecipients().length} selected / ${filteredRows.length} shown / ${state.marketingRecipients.length} total`;
  updateMarketingSkippedControls();
  updateMarketingBlastControls();
}

function refreshMarketingLoadedSummary() {
  const elSummary = el("marketingLoadedSummary");
  if (!elSummary) return;
  const selected = getSelectedMarketingLoadedPatients().length;
  const total = state.marketingLoadedPatients.length;
  elSummary.textContent = `${selected} selected / ${total} loaded`;

  const btnAdd = el("btnMarketingAddLoadedToList");
  if (btnAdd) btnAdd.disabled = selected === 0;
}

function updateMarketingSkippedControls() {
  const btn = el("btnSelectMarketingSkipped");
  if (!btn) return;
  const phones = Array.isArray(state.lastMarketingSkippedPhones) ? state.lastMarketingSkippedPhones : [];
  const count = phones.length;
  btn.disabled = count === 0;
  btn.textContent = count > 0 ? `Select Skipped (${count})` : "Select Skipped";
}

function selectMarketingRecipientsByPhones(phoneList) {
  const phones = Array.isArray(phoneList) ? phoneList : [];
  const set = new Set(phones.map((p) => normalizePhone(p)));
  let selectedCount = 0;
  for (const row of state.marketingRecipients) {
    row.selected = set.has(row.phone);
    if (row.selected) selectedCount++;
  }
  renderMarketingRecipients();
  refreshMarketingPreview();
  toast("Recipients", `Selected ${selectedCount} skipped recipient${selectedCount === 1 ? "" : "s"}`);
}

function syncMarketingSelectAll() {
  const all = el("marketingSelectAll");
  const filteredRows = getFilteredMarketingRecipients();
  const selectableRows = filteredRows.filter((row) => marketingRecipientStatusInfo(row).kind !== "already_sent");
  const total = selectableRows.length;
  const selected = selectableRows.filter((x) => x.selected).length;
  all.checked = total > 0 && selected === total;
  all.indeterminate = selected > 0 && selected < total;
  all.disabled = filteredRows.length === 0 || total === 0;
}

function getMarketingLoadedTotalPages() {
  const size = Math.max(1, toInt(state.marketingLoadedPageSize, 50));
  const total = state.marketingLoadedPatients.length;
  return Math.max(1, Math.ceil(total / size));
}

function clampMarketingLoadedPage() {
  const totalPages = getMarketingLoadedTotalPages();
  state.marketingLoadedPage = clamp(state.marketingLoadedPage, 1, totalPages, 1);
}

function getMarketingLoadedPageRows() {
  clampMarketingLoadedPage();
  const size = Math.max(1, toInt(state.marketingLoadedPageSize, 50));
  const start = (state.marketingLoadedPage - 1) * size;
  return state.marketingLoadedPatients.slice(start, start + size);
}

function syncMarketingLoadedSelectAll() {
  const all = el("marketingLoadedSelectAll");
  if (!all) return;
  const pageRows = getMarketingLoadedPageRows();
  const selectableRows = pageRows.filter((row) => marketingRecipientStatusInfo(row).kind !== "already_sent");
  const total = selectableRows.length;
  const selected = selectableRows.filter((x) => x.selected).length;
  all.checked = total > 0 && selected === total;
  all.indeterminate = selected > 0 && selected < total;
  all.disabled = pageRows.length === 0 || total === 0;
}

function renderMarketingLoadedPatients() {
  const tbody = el("marketingLoadedBody");
  if (!tbody) return;
  tbody.innerHTML = "";

  const pageRows = getMarketingLoadedPageRows();
  for (const row of pageRows) {
    const tr = document.createElement("tr");

    const tdPick = document.createElement("td");
    const cb = document.createElement("input");
    const statusInfo = marketingRecipientStatusInfo(row);
    cb.type = "checkbox";
    cb.checked = !!row.selected;
    cb.disabled = statusInfo.kind === "already_sent";
    cb.addEventListener("change", () => {
      row.selected = !!cb.checked;
      refreshMarketingLoadedSummary();
      syncMarketingLoadedSelectAll();
    });
    tdPick.appendChild(cb);

    const tdPhone = document.createElement("td");
    tdPhone.textContent = row.phone;
    const tdName = document.createElement("td");
    tdName.textContent = row.name || "-";
    const tdDentist = document.createElement("td");
    tdDentist.textContent = row.dentist || "-";
    const tdAppt = document.createElement("td");
    const dateBaseTs = toInt(row.apptDate, 0) || toInt(row.apptStartTime, 0);
    if (dateBaseTs) {
      const dateText = formatDateForMessage(dateBaseTs, "english");
      const timeText = toInt(row.apptStartTime, 0) ? formatTimeForMessage(row.apptStartTime) : "";
      tdAppt.textContent = timeText ? `${dateText} ${timeText}` : dateText;
    } else tdAppt.textContent = "-";

    const tdStatus = document.createElement("td");
    tdStatus.innerHTML = statusInfo.kind === "already_sent"
      ? `<span class="statusPill status-skipped">${escapeHtml(statusInfo.label || "Blocked")}</span>`
      : `<span class="statusPill status-sent">${escapeHtml(statusInfo.label || "Not Sent Yet")}</span>`;
    if (statusInfo.sentAt && statusInfo.kind !== "already_sent") {
      const meta = document.createElement("div");
      meta.className = "smallText";
      meta.textContent = statusInfo.sentAt;
      tdStatus.appendChild(meta);
    }
    if (statusInfo.recent?.isRecent && statusInfo.recent.sentAt !== statusInfo.sentAt) {
      const recent = document.createElement("div");
      recent.className = "smallText statusWarnText";
      recent.textContent = statusInfo.recent.label;
      recent.title = statusInfo.recent.sentAt;
      tdStatus.appendChild(recent);
    }

    tr.append(tdPick, tdPhone, tdName, tdDentist, tdAppt, tdStatus);
    tbody.appendChild(tr);
  }

  if (state.marketingLoadedPatients.length === 0) {
    const tr = document.createElement("tr");
    tr.innerHTML = '<td colspan="6" class="smallText">No loaded patients. Choose range and click Load.</td>';
    tbody.appendChild(tr);
  }

  const totalPages = getMarketingLoadedTotalPages();
  const pageText = el("marketingLoadedPageText");
  if (pageText) pageText.textContent = `Page ${state.marketingLoadedPage} / ${totalPages}`;

  const btnPrev = el("btnMarketingLoadedPrev");
  const btnNext = el("btnMarketingLoadedNext");
  if (btnPrev) btnPrev.disabled = state.marketingLoadedPatients.length === 0 || state.marketingLoadedPage <= 1;
  if (btnNext) btnNext.disabled = state.marketingLoadedPatients.length === 0 || state.marketingLoadedPage >= totalPages;

  refreshMarketingLoadedSummary();
  syncMarketingLoadedSelectAll();
}

function renderMarketingRecipients() {
  const tbody = el("marketingRecipientsBody");
  tbody.innerHTML = "";

  const rows = getFilteredMarketingRecipients();
  for (const row of rows) {
    const tr = document.createElement("tr");
    const vars = buildMarketingTemplateVars(row);
    const statusInfo = marketingRecipientStatusInfo(row);

    const tdPick = document.createElement("td");
    const cb = document.createElement("input");
    cb.type = "checkbox";
    cb.checked = !!row.selected;
    cb.disabled = statusInfo.kind === "already_sent";
    cb.addEventListener("change", () => {
      row.selected = !!cb.checked;
      refreshMarketingSummary();
      refreshMarketingPreview();
      syncMarketingSelectAll();
    });
    tdPick.appendChild(cb);

    const tdName = document.createElement("td");
    tdName.textContent = vars.name || "-";
    const tdPhone = document.createElement("td");
    tdPhone.textContent = row.phone;
    const tdBranch = document.createElement("td");
    tdBranch.textContent = vars.branch || "-";
    const tdStatus = document.createElement("td");
    tdStatus.innerHTML = statusInfo.kind === "already_sent"
      ? `<span class="statusPill status-skipped">${escapeHtml(statusInfo.label || "Blocked")}</span>`
      : `<span class="statusPill status-sent">${escapeHtml(statusInfo.label || "Not Sent Yet")}</span>`;
    if (statusInfo.sentAt && statusInfo.kind !== "already_sent") {
      const meta = document.createElement("div");
      meta.className = "smallText";
      meta.textContent = statusInfo.sentAt;
      tdStatus.appendChild(meta);
    }
    if (statusInfo.recent?.isRecent && statusInfo.recent.sentAt !== statusInfo.sentAt) {
      const recent = document.createElement("div");
      recent.className = "smallText statusWarnText";
      recent.textContent = statusInfo.recent.label;
      recent.title = statusInfo.recent.sentAt;
      tdStatus.appendChild(recent);
    }

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

    tr.append(tdPick, tdName, tdPhone, tdBranch, tdStatus, tdAction);
    tbody.appendChild(tr);
  }

  if (rows.length === 0) {
    const tr = document.createElement("tr");
    tr.innerHTML = '<td colspan="6" class="smallText">No recipients for this filter yet.</td>';
    tbody.appendChild(tr);
  }

  refreshMarketingSummary();
  syncMarketingSelectAll();
}

function renderTemplateList() {
  const list = el("templateList");
  if (!list) return;
  list.innerHTML = "";
  updateMarketingTemplatePermissionControls();

  const rows = getFilteredTemplates();
  const countTarget = el("templateListCount");
  if (countTarget) {
    const total = Array.isArray(state.templates) ? state.templates.length : 0;
    countTarget.textContent = `${rows.length} of ${total} templates`;
  }

  for (const t of rows) {
    const item = document.createElement("button");
    item.type = "button";
    item.className = `templateItem${state.currentTemplateId === t.id ? " active" : ""}`;
    const statusClass = t.active === false ? "inactive" : "active";
    item.innerHTML = `
      <div class="templateItemTop">
        <span class="templateItemName">${escapeHtml(t.name)}</span>
        <span class="templateStatusBadge ${statusClass}">${escapeHtml(templateStatusLabel(t))}</span>
      </div>
      <div class="templateItemMeta">${escapeHtml(templateTypeSummary(t))}</div>
      <div class="templateItemSnippet">${escapeHtml(marketingTemplateSnippet(t))}</div>
      <div class="templateItemFooter">
        <span>${escapeHtml(formatTemplateTimestamp(t.updated_at || t.created_at))}</span>
        <span>${escapeHtml(cleanString(t.updated_by || t.created_by) || "-")}</span>
      </div>`;
    item.addEventListener("click", () => {
      state.currentTemplateId = t.id;
      renderTemplateList();
      loadTemplateEditor();
      if (t.active !== false) renderMarketingTemplateSelect();
      refreshMarketingPreview();
    });
    list.appendChild(item);
  }

  if (rows.length === 0) {
    const empty = document.createElement("div");
    empty.className = "templateEmptyState";
    empty.textContent = "No matching templates.";
    list.appendChild(empty);
  }
}

function loadTemplateEditor() {
  setTemplateEmojiPickerOpen(false);
  updateMarketingTemplatePermissionControls();
  const t = getSelectedMarketingTemplate();
  if (!t) {
    state.currentTemplateMessageId = null;
    el("templateName").value = "";
    el("templateSendPolicy").value = "once";
    el("templateVariablesText").textContent = "Variables: -";
    el("templatePlaceholderPreview").textContent = "Type template to preview placeholders.";
    renderTemplateMetaSummary(null);
    renderTemplateMessageList();
    renderTemplateMessageEditor();
    return;
  }

  updateTemplateDerivedFields(t);
  ensureSelectedTemplateMessage(t);
  el("templateName").value = t.name;
  el("templateSendPolicy").value = t.sendPolicy;
  renderTemplateMetaSummary(t);
  renderTemplateMessageList();
  renderTemplateMessageEditor();
  refreshTemplateVariableSummary();
  const selectedMessage = getSelectedTemplateMessage(t);
  renderTemplatePlaceholderPreview(selectedMessage?.text || "");
}

function updateMarketingTemplatePermissionControls() {
  const canManageMaster = canManageMasterMarketingTemplates();
  const canEditBranch = canEditBranchMarketingTemplates();
  const newBtn = document.getElementById("btnNewTemplate");
  const deleteBtn = document.getElementById("btnDeleteTemplate");
  const importBtn = document.getElementById("btnImportTemplates");
  const saveBtn = document.getElementById("btnSaveTemplate");

  if (newBtn) newBtn.classList.toggle("hidden", !canManageMaster);
  if (deleteBtn) deleteBtn.classList.toggle("hidden", !canManageMaster);
  if (importBtn) importBtn.classList.toggle("hidden", !canManageMaster);
  if (saveBtn) {
    saveBtn.classList.toggle("hidden", !canManageMaster && !canEditBranch);
    saveBtn.textContent = canManageMaster ? "Save Template" : "Save Branch Version";
  }
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
  refreshTemplateVariableSummary();
  renderTemplatePlaceholderPreview(next);
  renderTemplateMessageList();
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

function setTemplateEmojiPickerOpen(open) {
  state.templateEmojiPickerOpen = !!open;
  const picker = el("templateEmojiPicker");
  const btn = el("btnTemplateEmoji");
  if (picker) picker.classList.toggle("hidden", !state.templateEmojiPickerOpen);
  if (btn) btn.classList.toggle("active", state.templateEmojiPickerOpen);
}

function insertEmojiIntoTemplate(rawEmoji) {
  const emoji = String(rawEmoji || "").trim();
  if (!emoji) return;
  const textarea = el("templateBody");
  if (!textarea || textarea.disabled) return;

  const value = String(textarea.value || "");
  const liveStart = Number(textarea.selectionStart || 0);
  const liveEnd = Number(textarea.selectionEnd || liveStart);
  const isActive = document.activeElement === textarea;
  const start = isActive ? liveStart : Number(state.templateBodyCaretPos || 0);
  const end = isActive ? liveEnd : start;
  const safeStart = Math.max(0, Math.min(value.length, start));
  const safeEnd = Math.max(safeStart, Math.min(value.length, end));
  const next = `${value.slice(0, safeStart)}${emoji}${value.slice(safeEnd)}`;
  textarea.value = next;
  const caret = safeStart + emoji.length;
  state.templateBodyCaretPos = caret;
  textarea.focus();
  textarea.setSelectionRange(caret, caret);
  readMarketingTemplateEditorToState();
  refreshTemplateVariableSummary();
  renderTemplatePlaceholderPreview(next);
  renderTemplateMessageList();
  renderTemplateList();
  refreshMarketingPreview();
}

function renderTemplateEmojiPicker() {
  const grid = el("templateEmojiGrid");
  if (!grid) return;
  grid.innerHTML = "";
  for (const emoji of TEMPLATE_EDITOR_EMOJIS) {
    const value = String(emoji || "").trim();
    if (!value) continue;
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "templateEmojiBtn";
    btn.textContent = value;
    btn.dataset.emojiValue = value;
    btn.title = `Insert ${value}`;
    grid.appendChild(btn);
  }
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
  const activeTemplates = (Array.isArray(state.templates) ? state.templates : []).filter((t) => t.active !== false);

  for (const t of activeTemplates) {
    const opt = document.createElement("option");
    opt.value = t.id;
    const scope = cleanString(t.scope || "global").toLowerCase();
    const label = scope === "branch" || t.is_branch_override === true ? "Branch version" : "Master";
    opt.textContent = `${t.name} (${label})`;
    select.appendChild(opt);
  }

  if (activeTemplates.length === 0) {
    const opt = document.createElement("option");
    opt.value = "";
    opt.textContent = "No template";
    select.appendChild(opt);
    select.value = "";
    return;
  }

  if (!state.currentTemplateId || !activeTemplates.some((x) => x.id === state.currentTemplateId)) {
    state.currentTemplateId = activeTemplates[0].id;
  }

  select.value = state.currentTemplateId;
}

function renderMarketingCampaignSelect() {
  const select = el("marketingCampaignSelect");
  if (!select) return;
  select.innerHTML = "";
  const campaigns = (Array.isArray(state.marketingCampaigns) ? state.marketingCampaigns : []).filter((campaign) => {
    return campaign && typeof campaign === "object" && cleanString(campaign.id);
  });
  state.marketingCampaigns = campaigns;
  for (const campaign of campaigns) {
    const opt = document.createElement("option");
    opt.value = String(campaign.id || "");
    opt.textContent = String(campaign.name || `Campaign ${campaign.id || ""}`);
    select.appendChild(opt);
  }
  if (campaigns.length === 0) {
    const opt = document.createElement("option");
    opt.value = "";
    opt.textContent = "No campaign";
    select.appendChild(opt);
    state.currentCampaignId = "";
  } else if (!state.currentCampaignId || !campaigns.some((x) => String(x.id) === String(state.currentCampaignId))) {
    state.currentCampaignId = String(campaigns[0].id || "");
  }
  select.value = String(state.currentCampaignId || "");
}

function getMasterMarketingTemplatesForCampaigns() {
  return state.templates.filter((template) => {
    const scope = cleanString(template.scope || "global").toLowerCase();
    const templateId = cleanString(template.root_template_id || template.id);
    return template.active !== false &&
      scope === "global" &&
      template.is_branch_override !== true &&
      /^\d+$/.test(templateId);
  });
}

function normalizeCampaignTemplateIdForPayload(value) {
  return cleanString(value);
}

function campaignUsesValidXanoTemplateIds(campaign) {
  const src = campaign && typeof campaign === "object" ? campaign : {};
  return /^\d+$/.test(cleanString(src.template_1_id)) && /^\d+$/.test(cleanString(src.template_2_id));
}

function buildAutoCampaignTemplateDraft(name, step) {
  const campaignName = cleanString(name) || "Marketing Campaign";
  const templateName = Number(step || 1) === 2 ? `Follow-up ${campaignName}` : campaignName;
  const defaultMessage = buildDefaultTemplateMessage("text");
  defaultMessage.text = "Hello {name},";
  return normalizeTemplate(
    {
      id: `t_auto_${Date.now().toString(16)}_${step}_${Math.random().toString(16).slice(2, 8)}`,
      name: templateName,
      body: defaultMessage.text,
      messages: [defaultMessage],
      variables: ["name"],
      sendPolicy: "once",
      active: true,
      scope: "global"
    },
    Number(step || 1) - 1
  );
}

async function createAutoCampaignTemplatePair(campaignName) {
  const existingDrafts = getCampaignDraftTemplates();
  const drafts = existingDrafts.length >= 2
    ? existingDrafts
    : [
      buildAutoCampaignTemplateDraft(campaignName, 1),
      buildAutoCampaignTemplateDraft(campaignName, 2)
    ];
  if (existingDrafts.length < 2) {
    drafts[0].campaignDraftKey = cleanString(state.campaignDraftKey);
    drafts[0].campaignStep = 1;
    drafts[1].campaignDraftKey = cleanString(state.campaignDraftKey);
    drafts[1].campaignStep = 2;
  }
  const res = await window.api.saveTemplates(drafts);
  const saved = Array.isArray(res?.templates) ? res.templates.map((t, idx) => normalizeTemplate(t, idx)) : [];
  const template1 = saved[0];
  const template2 = saved[1];
  const id1 = cleanString(template1?.root_template_id || template1?.id);
  const id2 = cleanString(template2?.root_template_id || template2?.id);
  if (!/^\d+$/.test(id1) || !/^\d+$/.test(id2) || id1 === id2) {
    throw new Error("Unable to create campaign templates in Xano. Please save two master templates first.");
  }
  return { template1, template2, template1Id: id1, template2Id: id2 };
}

function getSelectedCampaignEditorCampaign() {
  const id = cleanString(state.currentCampaignEditorId);
  if (!id) return null;
  return state.marketingCampaigns.find((campaign) => String(campaign.id || "") === id) || null;
}

function fillCampaignTemplateSelect(select, selectedId) {
  if (!select) return;
  const selected = cleanString(selectedId);
  const templates = getMasterMarketingTemplatesForCampaigns();
  select.innerHTML = "";
  for (const template of templates) {
    const opt = document.createElement("option");
    opt.value = String(template.root_template_id || template.id || "");
    opt.textContent = template.name;
    select.appendChild(opt);
  }
  if (templates.length === 0) {
    const opt = document.createElement("option");
    opt.value = "";
    opt.textContent = "No master template";
    select.appendChild(opt);
    return;
  }
  if (selected && templates.some((template) => String(template.root_template_id || template.id || "") === selected)) {
    select.value = selected;
    return;
  }
  select.value = String(templates[0]?.root_template_id || templates[0]?.id || "");
}

function slugifyCampaignKeyPart(input) {
  const slug = cleanString(input)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 48);
  return slug || "campaign";
}

function generateCampaignKey(name) {
  const stamp = new Date()
    .toISOString()
    .replace(/\D/g, "")
    .slice(0, 14);
  return `${slugifyCampaignKeyPart(name)}_${stamp}`;
}

function getCampaignDraftTemplates() {
  const key = cleanString(state.campaignDraftKey);
  if (!key) return [];
  return state.templates
    .filter((template) => cleanString(template.campaignDraftKey) === key)
    .sort((a, b) => Number(a.campaignStep || 0) - Number(b.campaignStep || 0));
}

function buildCampaignDraftTemplate(campaignName, step) {
  const defaultMessage = buildDefaultTemplateMessage("text");
  defaultMessage.text = "Hello {name},";
  const name = cleanString(campaignName) || "New Campaign";
  const templateName = Number(step || 1) === 2 ? `Follow-up ${name}` : name;
  return normalizeTemplate(
    {
      id: `t_campaign_${Date.now().toString(16)}_${step}_${Math.random().toString(16).slice(2, 8)}`,
      name: templateName,
      body: defaultMessage.text,
      messages: [defaultMessage],
      variables: ["name"],
      sendPolicy: "once",
      active: true,
      scope: "global",
      campaignDraftKey: state.campaignDraftKey,
      campaignStep: Number(step || 1)
    },
    state.templates.length
  );
}

function ensureCampaignDraftTemplates(campaignName = "") {
  if (!cleanString(state.campaignDraftKey)) state.campaignDraftKey = generateCampaignKey(campaignName || "new_campaign");
  const existing = getCampaignDraftTemplates();
  const byStep = new Map(existing.map((template) => [Number(template.campaignStep || 0), template]));
  const created = [];
  for (const step of [1, 2]) {
    if (byStep.has(step)) {
      created.push(byStep.get(step));
      continue;
    }
    const draft = buildCampaignDraftTemplate(campaignName, step);
    state.templates.push(draft);
    created.push(draft);
  }
  return created;
}

function syncCampaignDraftTemplateNames(campaignName) {
  const name = cleanString(campaignName);
  if (!name) return;
  const drafts = ensureCampaignDraftTemplates(name);
  for (const template of drafts) {
    const step = Number(template.campaignStep || 1);
    const oldDefaultNames = [
      "New Campaign",
      "Follow-up New Campaign",
      "New template"
    ];
    const currentName = cleanString(template.name);
    if (step === 1 && (!currentName || oldDefaultNames.includes(currentName) || cleanString(template._autoCampaignName) === currentName)) {
      template.name = name;
    }
    if (step === 2 && (!currentName || oldDefaultNames.includes(currentName) || currentName.startsWith("Follow-up ") || cleanString(template._autoCampaignName) === currentName)) {
      template.name = `Follow-up ${name}`;
    }
    template._autoCampaignName = template.name;
    updateTemplateDerivedFields(template);
  }
  renderTemplateList();
  if (drafts.some((template) => template.id === state.currentTemplateId)) loadTemplateEditor();
}

function enterNewCampaignMode(name = "") {
  state.campaignEditorCreating = true;
  state.currentCampaignEditorId = "";
  state.campaignDraftName = cleanString(name);
  state.campaignDraftKey = generateCampaignKey(name || "new_campaign");
  const drafts = ensureCampaignDraftTemplates(name || "New Campaign");
  state.currentTemplateId = drafts[0]?.id || state.currentTemplateId;
  state.currentTemplateMessageId = drafts[0]?.messages?.[0]?.id || null;
}

function loadCampaignEditor() {
  const campaign = getSelectedCampaignEditorCampaign();
  const templates = getMasterMarketingTemplatesForCampaigns();
  const firstTemplateId = String(templates[0]?.root_template_id || templates[0]?.id || "");
  const secondTemplateId = String(templates[1]?.root_template_id || templates[1]?.id || firstTemplateId);

  const nameInput = document.getElementById("campaignName");
  const template1 = document.getElementById("campaignTemplate1");
  const template2 = document.getElementById("campaignTemplate2");
  const active = document.getElementById("campaignActive");

  if (nameInput) {
    if (state.campaignEditorCreating) {
      nameInput.value = cleanString(state.campaignDraftName);
    } else if (!state.campaignEditorCreating) {
      nameInput.value = cleanString(campaign?.name || "");
    }
  }
  fillCampaignTemplateSelect(template1, campaign?.template_1_id || firstTemplateId);
  fillCampaignTemplateSelect(template2, campaign?.template_2_id || secondTemplateId);
  if (active) active.checked = campaign ? campaign.active !== false : true;
}

function renderCampaignManager() {
  document.getElementById("campaignManagerCard")?.classList.add("hidden");
  const select = document.getElementById("campaignEditorSelect");
  if (!select) return;
  state.marketingCampaigns = [];
  state.currentCampaignId = "";
  state.currentCampaignEditorId = "";
  state.campaignEditorCreating = false;
  state.resolvedCampaign = null;
  select.innerHTML = "";
  return;

  if (
    state.currentCampaignEditorId &&
    !state.marketingCampaigns.some((campaign) => String(campaign.id || "") === String(state.currentCampaignEditorId))
  ) {
    state.currentCampaignEditorId = "";
  }
  if (!state.campaignEditorCreating && !state.currentCampaignEditorId && state.marketingCampaigns.length > 0) {
    state.currentCampaignEditorId = String(state.marketingCampaigns[0].id || "");
  }

  select.innerHTML = "";
  const newOpt = document.createElement("option");
  newOpt.value = "";
  newOpt.textContent = "New campaign";
  select.appendChild(newOpt);
  for (const campaign of state.marketingCampaigns) {
    const opt = document.createElement("option");
    opt.value = String(campaign.id || "");
    opt.textContent = cleanString(campaign.name) || `Campaign ${campaign.id || ""}`;
    select.appendChild(opt);
  }
  select.value = String(state.currentCampaignEditorId || "");
  loadCampaignEditor();

  const canSave = canManageMarketingCampaigns();
  const isCreateMode = state.campaignEditorCreating || !cleanString(state.currentCampaignEditorId);
  const template1Wrap = document.getElementById("campaignTemplate1")?.closest("div");
  const template2Wrap = document.getElementById("campaignTemplate2")?.closest("div");
  if (template1Wrap) template1Wrap.classList.toggle("hidden", isCreateMode);
  if (template2Wrap) template2Wrap.classList.toggle("hidden", isCreateMode);
  const fields = [
    document.getElementById("campaignName"),
    document.getElementById("campaignTemplate1"),
    document.getElementById("campaignTemplate2"),
    document.getElementById("campaignActive"),
    document.getElementById("btnSaveCampaign")
  ].filter(Boolean);
  fields.forEach((field) => {
    field.disabled = !canSave;
  });
  const newBtn = document.getElementById("btnNewCampaign");
  if (newBtn) newBtn.disabled = !canSave;
  const saveBtn = document.getElementById("btnSaveCampaign");
  if (saveBtn) saveBtn.textContent = isCreateMode ? "Create Campaign & Templates" : "Save Campaign";
  const hint = document.getElementById("campaignManagerHint");
  if (hint) {
    hint.textContent = !canSave
      ? "Only Marketing or Developer users can create or edit campaigns."
      : isCreateMode
        ? "Enter the campaign name, then edit Template 1 and Follow-up in Marketing Templates below. Save Campaign when both are ready."
        : "Editing a campaign changes which master templates this campaign uses.";
  }
}

function readCampaignEditorPayload(templatePair = null) {
  const name = cleanString(document.getElementById("campaignName")?.value);
  const currentCampaign = getSelectedCampaignEditorCampaign();
  const isCreateMode = state.campaignEditorCreating || !cleanString(state.currentCampaignEditorId);
  const campaignKey = isCreateMode
    ? cleanString(state.campaignDraftKey) || generateCampaignKey(name)
    : cleanString(currentCampaign?.campaign_key) || generateCampaignKey(name);
  const template1Id = isCreateMode && templatePair
    ? cleanString(templatePair.template1Id)
    : cleanString(document.getElementById("campaignTemplate1")?.value);
  const template2Id = isCreateMode && templatePair
    ? cleanString(templatePair.template2Id)
    : cleanString(document.getElementById("campaignTemplate2")?.value);
  if (!name) throw new Error("Campaign name is required");
  if (!template1Id || !template2Id) throw new Error("Please select Template 1 and Template 2");
  if (!/^\d+$/.test(template1Id) || !/^\d+$/.test(template2Id)) {
    throw new Error("Campaign templates must be saved Xano master templates. Please reselect Template 1 and Template 2.");
  }
  if (template1Id === template2Id) throw new Error("Template 1 and Template 2 must be different");

  const payload = {
    ...(state.currentCampaignEditorId ? { id: cleanString(state.currentCampaignEditorId) } : {}),
    ...(campaignKey ? { campaign_key: campaignKey } : {}),
    name,
    template_1_id: normalizeCampaignTemplateIdForPayload(template1Id),
    template_2_id: normalizeCampaignTemplateIdForPayload(template2Id),
    active: document.getElementById("campaignActive")?.checked !== false
  };
  return payload;
}

function buildMarketingTemplateVars(recipient) {
  const row = recipient && typeof recipient === "object" ? recipient : {};
  const selectedBranch = el("marketingBranchSelect").value || state.session.user?.Branch || "";
  const displayBranch = isAiVentureMarketingBranch(selectedBranch) ? "Dentabay" : selectedBranch;
  const myBranch = isAiVentureMarketingBranch(state.session.user?.Branch || selectedBranch)
    ? "Dentabay"
    : state.session.user?.Branch || displayBranch || "";
  const dateTs = toInt(row.apptDate, 0) || toInt(row.apptStartTime, 0) || Date.now();
  const weekday = formatWeekdayForMessage(dateTs, "english");
  const titles = normalizeGenderTitles(row.gender);

  return {
    name: row.name || "Patient",
    branch: displayBranch,
    my_branch: myBranch,
    dentist: row.dentist || "",
    date: formatDateForMessage(dateTs, "english"),
    day: weekday,
    weekday,
    time: toInt(row.apptStartTime, 0) ? formatTimeForMessage(row.apptStartTime) : "",
    salutation: titles.salutation_bm,
    salutation_bm: titles.salutation_bm,
    salutation_en: titles.salutation_en,
    title_bm: titles.title_bm,
    title_en: titles.title_en,
    phone: normalizePhone(row.phone || "")
  };
}

function refreshMarketingPreview() {
  const template = getSelectedMarketingTemplate();
  const recipient = getSelectedMarketingRecipients()[0];
  if (!template) return (el("marketingPreview").textContent = "No marketing template selected.");
  if (!recipient) return (el("marketingPreview").textContent = "Select at least one recipient to preview.");
  const vars = buildMarketingTemplateVars(recipient);
  const rendered = buildRenderedMarketingMessages(template, vars, {
    includeMissingMedia: true,
    keepEmptyText: true
  });
  el("marketingPreview").textContent = formatMarketingPreviewFromMessages(rendered);
}

function normalizeGenderKey(genderRaw) {
  const g = String(genderRaw || "").trim().toLowerCase();
  if (g === "female" || g === "f" || g === "perempuan" || g.includes("female")) return "female";
  if (g === "male" || g === "m" || g === "lelaki" || g.includes("male")) return "male";
  return "";
}

function applySettingsToUi() {
  const s = state.settings;
  el("settingGapMin").value = String(s.gapMinSec || 7);
  el("settingGapMax").value = String(s.gapMaxSec || 45);
  el("settingTemplateGapMin").value = String(s.templateGapMinSec || 2);
  el("settingTemplateGapMax").value = String(s.templateGapMaxSec || 4);
  el("settingMarketingMonths").value = String(s.marketingMonthsAgoDefault || 6);
  el("settingMarketingPageSize").value = String(s.marketingPageSizeDefault || 35);
  el("marketingMonthsAgo").value = String(s.marketingMonthsAgoDefault || 6);
  state.marketingLoadedPageSize = clamp(s.marketingPageSizeDefault, 10, 500, state.marketingLoadedPageSize || 35);
  if (el("marketingPageSize")) el("marketingPageSize").value = String(state.marketingLoadedPageSize || 35);
}

function applyAppointmentTemplatesToUi() {
  if (!el("btnSaveAppointmentTemplates")) return;
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
  const g = String(genderRaw || "").trim().toLowerCase();
  if (g === "female" || g === "f" || g === "perempuan" || g.includes("female")) {
    return { title_bm: "Cik", title_en: "Ms", salutation_bm: "Cik", salutation_en: "Ms." };
  }
  if (g === "male" || g === "m" || g === "lelaki" || g.includes("male")) {
    return { title_bm: "Encik", title_en: "Mr", salutation_bm: "Encik", salutation_en: "Mr." };
  }
  return {
    title_bm: "Encik / Cik",
    title_en: "Mr / Ms",
    salutation_bm: "Encik / Cik",
    salutation_en: "Mr. / Ms."
  };
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

function buildAppointmentTemplateVars({ baseVars, language, name, gender }) {
  const varsBase = baseVars && typeof baseVars === "object" ? baseVars : {};
  const safeLanguage = String(language || "").trim().toLowerCase() === "english" ? "english" : "bahasa";
  const safeName = String(name || "").trim() || "Patient";
  const genderKey = normalizeGenderKey(gender);
  const titles = normalizeGenderTitles(genderKey);
  const salutation = safeLanguage === "bahasa" ? titles.salutation_bm : titles.salutation_en;

  return {
    ...varsBase,
    name: safeName,
    salutation,
    salutation_bm: titles.salutation_bm,
    salutation_en: titles.salutation_en,
    title_bm: titles.title_bm,
    title_en: titles.title_en
  };
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
    const gender = normalizeGenderKey(appt?.gender || patient?.gender);
    const name = String(appt?.nickname || patient?.nickname || appt.Patient_Name || patient?.name || "Patient");
    const phone = normalizePhone(patient?.phone || appt.Patient_Phone_No || "");
    const ic_number = normalizeIcNumber(appt?.ic_number || patient?.ic_number);

    if (!isValidPhone(phone)) return { skip: true, reason: "Invalid or missing phone", name, phone };

    const baseVars = {
      branch: appt.Branch_Name || branchInfo.label || "",
      dentist: appt.Dentist_Name || "",
      date: formatDateForMessage(appt.Appt_Date || appt.Appt_Start_Time, language),
      weekday: formatWeekdayForMessage(appt.Appt_Date || appt.Appt_Start_Time, language),
      time: formatTimeForMessage(appt.Appt_Start_Time),
      address: branchInfo.address || "",
      branch_phone: branchInfo.branch_phone || "",
      google_direction: branchInfo.google_direction || "",
      waze_direction: branchInfo.waze_direction || "",
      google_review_link: branchInfo.Google_Review || ""
    };
    const vars = buildAppointmentTemplateVars({ baseVars, language, name, gender });

    return {
      skip: false,
      name,
      gender,
      ic_number,
      phone,
      text: renderTemplate(templateText, vars),
      aiVariables: vars,
      templateId: `appointment_${purposeKey}_${language}`,
      language,
      templateText,
      baseVars
    };
  });

  return prepared.filter((x) => x && !x.skip && x.phone && x.text);
}

function openConfirmModal({ title, subtitle, recipientsText, sampleText, recipientsEditable, renderSampleFromRecipient }) {
  const recipientsPre = el("confirmRecipients");
  const editorWrap = el("confirmRecipientsEditor");
  const editorRows = el("confirmRecipientsEditorRows");
  const samplePre = el("confirmSample");
  const editableRows = Array.isArray(recipientsEditable) ? recipientsEditable : [];
  const editorRowEls = [];
  const hasEditable = editableRows.length > 0;
  let selectedRowIdx = 0;

  el("confirmTitle").textContent = title || "Confirm Send";
  el("confirmSubtitle").textContent = subtitle || "";
  recipientsPre.textContent = recipientsText || "-";

  recipientsPre.classList.toggle("hidden", hasEditable);
  editorWrap.classList.toggle("hidden", !hasEditable);
  editorRows.innerHTML = "";

  const updateSampleText = (idx = selectedRowIdx) => {
    if (hasEditable && typeof renderSampleFromRecipient === "function") {
      const safeIdx = clamp(idx, 0, Math.max(0, editableRows.length - 1), 0);
      selectedRowIdx = safeIdx;
      const row = editableRows[safeIdx];
      samplePre.textContent = String(renderSampleFromRecipient(row) || sampleText || "-");
      editorRowEls.forEach((node, nodeIdx) => {
        node.classList.toggle("active", nodeIdx === safeIdx);
      });
      return;
    }
    samplePre.textContent = sampleText || "-";
  };

  if (hasEditable) {
    editableRows.forEach((row, idx) => {
      const item = row && typeof row === "object" ? row : {};
      item.phone = normalizePhone(item.phone || "");
      item.name = String(item.name || "").trim() || "Patient";
      item.gender = normalizeGenderKey(item.gender);

      const rowEl = document.createElement("div");
      rowEl.className = "confirmRecipientsEditorRow";
      rowEl.tabIndex = 0;

      const idxEl = document.createElement("span");
      idxEl.className = "confirmRecipientsEditorIndex";
      idxEl.textContent = `${idx + 1}.`;

      const phoneEl = document.createElement("span");
      phoneEl.className = "confirmRecipientsEditorPhone";
      phoneEl.textContent = item.phone || "-";

      const nameInput = document.createElement("input");
      nameInput.type = "text";
      nameInput.className = "fieldInput compact";
      nameInput.value = item.name;
      nameInput.placeholder = "Nickname";
      nameInput.addEventListener("input", () => {
        item.name = String(nameInput.value || "").trim() || "Patient";
        updateSampleText(idx);
      });

      const genderSelect = document.createElement("select");
      genderSelect.className = "fieldInput compact";
      const optUnknown = document.createElement("option");
      optUnknown.value = "";
      optUnknown.textContent = "Unknown";
      const optMale = document.createElement("option");
      optMale.value = "male";
      optMale.textContent = "Male";
      const optFemale = document.createElement("option");
      optFemale.value = "female";
      optFemale.textContent = "Female";
      genderSelect.append(optUnknown, optMale, optFemale);
      genderSelect.value = item.gender;
      genderSelect.addEventListener("change", () => {
        item.gender = normalizeGenderKey(genderSelect.value);
        updateSampleText(idx);
      });

      rowEl.addEventListener("click", () => {
        updateSampleText(idx);
      });
      rowEl.addEventListener("focusin", () => {
        updateSampleText(idx);
      });
      rowEl.addEventListener("keydown", (evt) => {
        const targetEl = evt.target;
        const targetTag = String(targetEl?.tagName || "").toLowerCase();
        const isTypingTarget =
          targetTag === "input" ||
          targetTag === "textarea" ||
          targetTag === "select" ||
          targetEl?.isContentEditable === true;
        if (isTypingTarget) return;

        if (evt.key === "Enter" || evt.key === " ") {
          evt.preventDefault();
          updateSampleText(idx);
        }
      });

      rowEl.append(idxEl, phoneEl, nameInput, genderSelect);
      editorRows.appendChild(rowEl);
      editorRowEls.push(rowEl);
    });
  }

  updateSampleText(0);
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

async function sendPreparedItems(items, batchLabel, aiEnabled, options = {}) {
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

  if (!options.skipConfirm) {
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
  }

  resetQueueStats(sendItems.length);
  setBatchSending(true);
  state.currentBatchQueueType = String(options.queueType || "appointment");
  try {
    return await window.api.waSendPreparedBatch({
      batchLabel,
      items: sendItems,
      pacing: { pattern: "random", minSec: state.settings.gapMinSec, maxSec: state.settings.gapMaxSec },
      aiRewrite: aiEnabled
        ? { enabled: true, prompt: AI_VARIATION_PROMPT, fallbackToOriginal: true }
        : { enabled: false },
      safety: { maxRecipients: 500 }
    });
  } finally {
    state.currentBatchQueueType = "";
    setBatchSending(false);
  }
}

function showBatchCompletedAlert(title, result) {
  if (!result || result.stopped) return;
  const sent = Math.max(0, Number(result.sent || 0));
  const failed = Math.max(0, Number(result.failed || 0));
  const skipped = Math.max(0, Number(result.skipped || 0));
  const total = Math.max(sent + failed + skipped, Number(result.total || 0));
  window.alert(`${title} completed.\nTotal: ${total}\nSent: ${sent}\nFailed: ${failed}\nSkipped: ${skipped}`);
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

  const editableRecipients = items.map((item, idx) => ({
    idx,
    phone: normalizePhone(item.phone || ""),
    name: String(item.name || "").trim() || "Patient",
    gender: normalizeGenderKey(item.gender),
    ic_number: normalizeIcNumber(item.ic_number)
  }));

  const renderSampleFromRecipient = (editRow) => {
    const rawIdx = Number(editRow?.idx);
    const idx = Number.isFinite(rawIdx) ? clamp(rawIdx, 0, Math.max(0, items.length - 1), 0) : 0;
    const src = items[idx] || items[0] || {};
    const vars = buildAppointmentTemplateVars({
      baseVars: src.baseVars || {},
      language: src.language || el(langId).value || "bahasa",
      name: String(editRow?.name || src.name || "").trim() || "Patient",
      gender: editRow?.gender || src.gender
    });
    return renderTemplate(src.templateText || src.text || "", vars);
  };

  const confirmRecipients = editableRecipients
    .slice(0, 40)
    .map((x, i) => `${i + 1}. ${x.name || "-"} (${x.phone})`)
    .join("\n");
  const suffix = editableRecipients.length > 40 ? `\n... and ${editableRecipients.length - 40} more` : "";
  const confirmed = await openConfirmModal({
    title: "Confirm Send",
    subtitle: `${editableRecipients.length} messages will be sent one-by-one`,
    recipientsText: `${confirmRecipients}${suffix}`,
    sampleText: renderSampleFromRecipient(editableRecipients[0]),
    recipientsEditable: editableRecipients,
    renderSampleFromRecipient
  });

  if (!confirmed) {
    toast("Canceled", "Send canceled by user");
    return;
  }
  queueSilentPatientProfileUpdates(editableRecipients);

  const sendItems = editableRecipients
    .map((editRow) => {
      const rawIdx = Number(editRow?.idx);
      const idx = Number.isFinite(rawIdx) ? clamp(rawIdx, 0, Math.max(0, items.length - 1), 0) : 0;
      const src = items[idx] || {};
      const phone = normalizePhone(src.phone || editRow.phone || "");
      if (!isValidPhone(phone)) return null;

      const vars = buildAppointmentTemplateVars({
        baseVars: src.baseVars || {},
        language: src.language || el(langId).value || "bahasa",
        name: String(editRow?.name || src.name || "").trim() || "Patient",
        gender: editRow?.gender || src.gender
      });
      const text = renderTemplate(src.templateText || src.text || "", vars);
      if (!String(text || "").trim()) return null;

      return {
        phone,
        name: vars.name,
        text,
        aiVariables: vars,
        templateId: src.templateId || `appointment_${purposeKey}_${el(langId).value || "bahasa"}`
      };
    })
    .filter(Boolean);

  if (sendItems.length === 0) {
    toast("No recipients", "No valid recipients after preview edits.");
    return;
  }

  const res = await sendPreparedItems(sendItems, `appointment_${purposeKey}`, !!el(aiId).checked, { skipConfirm: true });
  if (!res) return;
  if (res.stopped) {
    toast("Appointment send stopped", `Sent: ${res.sent}, Failed: ${res.failed}, Skipped: ${res.skipped}`);
    return;
  }
  showBatchCompletedAlert("Appointment send", res);
  toast("Appointment send done", `Sent: ${res.sent}, Failed: ${res.failed}, Skipped: ${res.skipped}`);
}

function readMarketingTemplateEditorToState() {
  const t = getSelectedMarketingTemplate();
  if (!t) return;
  t.name = el("templateName").value.trim() || "Untitled";
  t.sendPolicy = el("templateSendPolicy").value === "multiple" ? "multiple" : "once";
  const messages = ensureTemplateMessages(t);
  const currentMessage = getSelectedTemplateMessage(t);
  if (currentMessage && messages.length > 0) {
    const nextType = normalizeTemplateMessageType(el("templateMessageType").value || currentMessage.type);
    currentMessage.type = nextType;
    currentMessage.text = el("templateBody").value || "";
    if (nextType === "text") {
      currentMessage.attachment = null;
    } else {
      currentMessage.attachment = normalizeTemplateAttachment(currentMessage.attachment, nextType);
    }
  }
  updateTemplateDerivedFields(t);
}

async function loadAppointments() {
  if (state.appointmentsLoadPromise) return await state.appointmentsLoadPromise;

  const job = (async () => {
    const branch = el("apptBranchSelect").value;
    const dateTs = ymdToKlMidnightTs(el("apptDateInput").value);
    if (!branch) throw new Error("Please select a branch");
    if (!dateTs) throw new Error("Please select a valid date");

    const seq = (state.apptReqSeq = toInt(state.apptReqSeq, 0) + 1);
    const res = await window.api.clinicGetAppointmentList({ branch, date: dateTs });
    if (seq !== state.apptReqSeq) return;
    state.appointments = Array.isArray(res?.appointments) ? res.appointments : [];
    state.selectedAppointmentIds = new Set();
    renderAppointmentTable();
  })();

  state.appointmentsLoadPromise = job;
  try {
    await job;
  } finally {
    if (state.appointmentsLoadPromise === job) {
      state.appointmentsLoadPromise = null;
    }
  }
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

  let res;
  try {
    res = await window.api.clinicGetPastPatients({
      branch,
      start_day: range.startTs,
      end_day: range.endTs
    });
  } catch (e) {
    el("pastPatientRangeText").textContent = `Failed to load ${range.label}: ${String(e?.message || e)}`;
    throw e;
  }
  const rows = Array.isArray(res?.patients) ? res.patients : [];

  const byPhone = new Map();
  for (const row of rows) {
    const phone = normalizePhone(row?.Patient_Phone_No);
    if (!isValidPhone(phone)) continue;

    const existing =
      byPhone.get(phone) || {
        id: `lp_${phone}`,
        phone,
        name: "",
        dentist: "",
        gender: "",
        ic_number: "",
        apptDate: 0,
        apptStartTime: 0,
        selected: false
      };

    const apptDate = toInt(row?.Appt_Date, 0);
    const apptStartTime = toInt(row?.Appt_Start_Time, 0);
    const icNumber = normalizeIcNumber(row?.ic_number || row?.IC_Number);
    const candidateKey = apptStartTime || apptDate;
    const existingKey = toInt(existing.apptStartTime, 0) || toInt(existing.apptDate, 0);

    const nickname = String(row?.nickname || "").trim();
    if (nickname) existing.name = nickname;
    else if (!existing.name && row?.Patient_Name) existing.name = String(row.Patient_Name || "").trim();
    if (!existing.gender && row?.gender) existing.gender = String(row.gender).trim().toLowerCase();
    if (!existing.ic_number && icNumber) existing.ic_number = icNumber;
    if (candidateKey && (!existingKey || candidateKey > existingKey)) {
      if (row?.Dentist_Name) existing.dentist = String(row.Dentist_Name || "").trim();
      if (apptDate) existing.apptDate = apptDate;
      if (apptStartTime) existing.apptStartTime = apptStartTime;
    } else {
      if (!existing.dentist && row?.Dentist_Name) existing.dentist = String(row.Dentist_Name || "").trim();
      if (!existing.apptDate && apptDate) existing.apptDate = apptDate;
      if (!existing.apptStartTime && apptStartTime) existing.apptStartTime = apptStartTime;
    }
    byPhone.set(phone, existing);
  }

  state.marketingLoadedPatients = Array.from(byPhone.values()).sort((a, b) => {
    const bKey = toInt(b.apptStartTime, 0) || toInt(b.apptDate, 0);
    const aKey = toInt(a.apptStartTime, 0) || toInt(a.apptDate, 0);
    return bKey - aKey;
  });
  state.marketingLoadedPage = 1;
  renderMarketingLoadedPatients();
  refreshMarketingLoadedPatientStatuses().catch(() => { });

  el("pastPatientRangeText").textContent = `Loaded ${state.marketingLoadedPatients.length} unique patients from ${range.label
    } (${formatDateForMessage(range.startTs, "english")} - ${formatDateForMessage(range.endTs, "english")})`;
}

async function sendMarketing() {
  if (!state.waConnected) {
    toast("WhatsApp", "Please connect WhatsApp first");
    setActiveTab("connect");
    return;
  }

  const template = getSelectedMarketingTemplate();
  if (!template) throw new Error("Please select a template");
  const normalizedMessages = normalizeTemplateMessages(template.messages, template.body || "");
  template.messages = normalizedMessages;
  updateTemplateDerivedFields(template);
  const missingMediaAt = normalizedMessages.findIndex((msg) => {
    const type = normalizeTemplateMessageType(msg.type);
    if (type === "text") return false;
    return !String(msg?.attachment?.path || msg?.attachment?.url || "").trim();
  });
  if (missingMediaAt >= 0) {
    throw new Error(`Template message ${missingMediaAt + 1} is missing media attachment`);
  }

  const selected = getSelectedMarketingRecipients();
  if (selected.length === 0) throw new Error("Please select recipients");

  const templateId = `marketing_${template.id}`;
  const skipAlreadySent = template.sendPolicy !== "multiple";
  const aiEnabled = !!el("aiMarketing").checked;

  const selectedByPhone = new Map();
  let editableRecipients = [];
  for (const row of selected) {
    const phone = normalizePhone(row.phone);
    if (!isValidPhone(phone)) continue;
    if (selectedByPhone.has(phone)) continue;
    selectedByPhone.set(phone, row);
    editableRecipients.push({
      phone,
      name: String(row.name || "").trim() || "Patient",
      gender: normalizeGenderKey(row.gender),
      ic_number: normalizeIcNumber(row.ic_number)
    });
  }

  if (editableRecipients.length === 0) throw new Error("No valid messages to send");
  if (window.api.marketingCheckStatus) {
    const statusMaps = await getMarketingStatusMapsForPhones(editableRecipients.map((x) => x.phone), { throwOnError: true });
    applyMarketingStatusMapsToRows(selected, statusMaps);
    const allowed = [];
    const blocked = [];
    for (const row of editableRecipients) {
      const src = selectedByPhone.get(row.phone) || {};
      const statusInfo = marketingRecipientStatusInfo(src);
      if (statusInfo.kind !== "already_sent") allowed.push(row);
      else blocked.push(row.phone);
    }
    editableRecipients = allowed;
    renderMarketingRecipients();
    renderMarketingLoadedPatients();
    updateMarketingBlastControls();
    if (blocked.length > 0) {
      toast("Marketing status", `Skipped ${blocked.length} already sent recipient${blocked.length === 1 ? "" : "s"}`);
    }
    if (editableRecipients.length === 0) throw new Error("No eligible recipients after Xano status check");
    const dailyLimit = normalizeMarketingDailyLimit(state.marketingDailyLimit);
    if (dailyLimit.limit_reached || dailyLimit.remaining <= 0) {
      throw new Error("Daily marketing limit reached for this branch/profile.");
    }
    if (editableRecipients.length > dailyLimit.remaining) {
      throw new Error(`Selected ${editableRecipients.length} eligible recipients, but only ${dailyLimit.remaining} remaining in the rolling 24-hour limit. Please reduce the selection.`);
    }
  }
  if (editableRecipients.length > MARKETING_BLAST_LIMIT) {
    throw new Error(`Maximum marketing blast is ${MARKETING_BLAST_LIMIT} contacts only. Please reduce the list.`);
  }

  const sampleTextFromRecipient = (editRow) => {
    const src = selectedByPhone.get(editRow?.phone) || {};
    const merged = {
      ...src,
      phone: normalizePhone(editRow?.phone || src.phone || ""),
      name: String(editRow?.name || src.name || "").trim() || "Patient",
      gender: normalizeGenderKey(editRow?.gender || src.gender)
    };
    const rendered = buildRenderedMarketingMessages(template, buildMarketingTemplateVars(merged), {
      includeMissingMedia: true,
      keepEmptyText: true
    });
    return formatMarketingPreviewFromMessages(rendered);
  };

  const confirmRecipients = editableRecipients
    .slice(0, 40)
    .map((x, i) => `${i + 1}. ${x.name || "-"} (${x.phone})`)
    .join("\n");
  const suffix = editableRecipients.length > 40 ? `\n... and ${editableRecipients.length - 40} more` : "";
  const sampleText = sampleTextFromRecipient(editableRecipients[0]);

  const recentCount = editableRecipients.filter((x) => {
    const recent = marketingRecentWarningInfo(x.phone);
    return recent.isRecent;
  }).length;
  const recentWarning = recentCount > 0
    ? `\n\nWarning: ${recentCount} contact${recentCount === 1 ? "" : "s"} received marketing within the last 30 days.`
    : "";

  const confirmed = await openConfirmModal({
    title: "Confirm Send",
    subtitle: `${editableRecipients.length} messages will be sent one-by-one${skipAlreadySent ? "\nNote: numbers that already received this template will be skipped." : ""
      }${recentWarning}`,
    recipientsText: `${confirmRecipients}${suffix}`,
    sampleText,
    recipientsEditable: editableRecipients,
    renderSampleFromRecipient: sampleTextFromRecipient
  });

  if (!confirmed) {
    toast("Canceled", "Send canceled by user");
    return null;
  }
  queueSilentPatientProfileUpdates(editableRecipients);

  const varsByPhone = {};
  const sendRows = [];
  for (const editRow of editableRecipients) {
    const phone = normalizePhone(editRow.phone);
    if (!isValidPhone(phone)) continue;

    const src = selectedByPhone.get(phone) || {};
    const name = String(editRow.name || src.name || "").trim() || "Patient";
    const gender = normalizeGenderKey(editRow.gender || src.gender);
    const merged = { ...src, phone, name, gender };
    const vars = buildMarketingTemplateVars(merged);
    const renderedMessages = buildRenderedMarketingMessages(template, vars);
    if (renderedMessages.length === 0) continue;

    if (src && typeof src === "object") {
      src.name = name;
      src.gender = gender;
    }

    varsByPhone[phone] = vars;
    sendRows.push({ phone, name });
  }

  if (sendRows.length === 0) throw new Error("No valid messages to send after preview edits");
  renderMarketingRecipients();
  refreshMarketingPreview();

  state.lastMarketingSkippedPhones = [];
  state.lastMarketingSkippedTemplateId = templateId;
  updateMarketingSkippedControls();

  resetQueueStats(sendRows.length);
  setBatchSending(true);
  state.currentBatchQueueType = "marketing";
  let res = null;
  try {
    res = await window.api.waSendBatch({
      templateId,
      templateMessages: template.messages,
      templateBody: template.body,
      recipients: sendRows.map((x) => x.phone),
      varsByPhone,
      pacing: { pattern: "random", minSec: state.settings.gapMinSec, maxSec: state.settings.gapMaxSec },
      templatePacing: {
        pattern: "random",
        minSec: state.settings.templateGapMinSec,
        maxSec: state.settings.templateGapMaxSec
      },
      aiRewrite: aiEnabled ? { enabled: true, prompt: AI_VARIATION_PROMPT, fallbackToOriginal: true } : { enabled: false },
      marketingContext: {
        ...selectedMarketingTemplateApiIds(template),
        branch: currentMarketingBranchForLimit(),
        profile: currentMarketingProfileForLimit()
      },
      safety: { maxRecipients: MARKETING_BLAST_LIMIT },
      skipAlreadySent
    });
  } finally {
    state.currentBatchQueueType = "";
    setBatchSending(false);
  }
  if (!res) return;
  const localSentAt = new Date().toISOString();
  const loggedSentPhones = new Set(
    (Array.isArray(res.marketingLogResults) ? res.marketingLogResults : [])
      .filter((row) => {
        const status = cleanString(row?.status);
        const reason = cleanString(row?.reason);
        return status === "sent" || (status === "skipped" && reason === "already_sent");
      })
      .map((row) => normalizePhone(row?.phone || ""))
      .filter(Boolean)
  );
  const latestDailyLimit = [...(Array.isArray(res.marketingLogResults) ? res.marketingLogResults : [])]
    .reverse()
    .find((row) => row?.daily_limit && typeof row.daily_limit === "object")?.daily_limit;
  if (latestDailyLimit) {
    state.marketingDailyLimit = normalizeMarketingDailyLimit({
      ...latestDailyLimit,
      loaded: true
    });
    updateMarketingBlastControls();
  }
  const sentPhones = Array.isArray(res.sentPhones) && res.sentPhones.length > 0
    ? res.sentPhones.map((phone) => normalizePhone(phone)).filter(Boolean)
    : sendRows.map((row) => normalizePhone(row.phone)).filter(Boolean).filter((phone) => {
      return !Array.isArray(res.skippedAlreadySentPhones) || !res.skippedAlreadySentPhones.map((x) => normalizePhone(x)).includes(phone);
    });
  if (!state.marketingLocalSentAtByPhone || typeof state.marketingLocalSentAtByPhone !== "object") {
    state.marketingLocalSentAtByPhone = {};
  }
  const phonesToMarkSent = loggedSentPhones.size > 0 ? sentPhones.filter((phone) => loggedSentPhones.has(phone)) : sentPhones;
  for (const phone of phonesToMarkSent) {
    state.marketingLocalSentAtByPhone[phone] = localSentAt;
    for (const row of state.marketingRecipients) {
      if (normalizePhone(row?.phone || "") === phone) {
        row.campaignSentAt = localSentAt;
        row.recentMarketingSentAt = localSentAt;
        row.selected = false;
      }
    }
    for (const row of state.marketingLoadedPatients) {
      if (normalizePhone(row?.phone || "") === phone) {
        row.campaignSentAt = localSentAt;
        row.recentMarketingSentAt = localSentAt;
        row.selected = false;
      }
    }
  }
  renderMarketingRecipients();
  renderMarketingLoadedPatients();
  await refreshMarketingBlastGuard();
  await refreshMarketingBlastHistory().catch(() => { });
  addImmediateMarketingBlastHistoryRow(res);
  await refreshMarketingRecipientStatuses().catch(() => { });
  await openMarketingSentChatAfterSend(sendRows, varsByPhone, template, res).catch(() => { });
  state.lastMarketingSkippedPhones = Array.isArray(res.skippedAlreadySentPhones) ? res.skippedAlreadySentPhones : [];
  state.lastMarketingSkippedTemplateId = templateId;
  updateMarketingSkippedControls();
  if (Array.isArray(res.marketingLogErrors) && res.marketingLogErrors.length > 0) {
    const firstError = cleanString(res.marketingLogErrors[0]?.message || "Xano send log failed");
    toast("Marketing log not saved", `${res.marketingLogErrors.length} log failed: ${firstError}`);
  }
  if (res.xanoDailyLimitReached || cleanString(res.xanoStopReason) === "daily_limit_reached") {
    toast("Daily limit reached", "Xano stopped the remaining marketing send because the rolling 24-hour limit was reached.");
  }
  if (state.lastMarketingSkippedPhones.length > 0) {
    toast(
      "Skipped",
      `Skipped ${state.lastMarketingSkippedPhones.length} (already sent). Click Select Skipped to target them with another template.`
    );
  }
  if (res.stopped) {
    toast("Marketing send stopped", `Sent: ${res.sent}, Failed: ${res.failed}, Skipped: ${res.skipped}`);
    return;
  }
  showBatchCompletedAlert("Marketing send", res);
  toast("Marketing send done", `Sent: ${res.sent}, Failed: ${res.failed}, Skipped: ${res.skipped}`);
}

async function saveSettings() {
  const next = {
    gapMinSec: clamp(el("settingGapMin").value, 7, 45, 7),
    gapMaxSec: clamp(el("settingGapMax").value, 7, 45, 45),
    templateGapMinSec: clamp(el("settingTemplateGapMin").value, 1, 30, 2),
    templateGapMaxSec: clamp(el("settingTemplateGapMax").value, 1, 30, 4),
    marketingMonthsAgoDefault: clamp(el("settingMarketingMonths").value, 1, 24, 6),
    marketingPageSizeDefault: clamp(el("settingMarketingPageSize").value, 10, 500, 35)
  };
  if (next.gapMaxSec < next.gapMinSec) next.gapMaxSec = next.gapMinSec;
  if (next.templateGapMaxSec < next.templateGapMinSec) next.templateGapMaxSec = next.templateGapMinSec;

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
  await refreshMarketingBlastGuard().catch(() => { });
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
  if (state.templateDataLoadPromise) return await state.templateDataLoadPromise;

  const job = (async () => {
    const templatesRaw = await window.api.getTemplates();

    state.templates = preferVisibleMarketingTemplates((Array.isArray(templatesRaw) ? templatesRaw : []).map((t, i) => normalizeTemplate(t, i)));
    state.marketingCampaigns = [];
    state.currentCampaignId = "";
    state.currentCampaignEditorId = "";
    state.campaignEditorCreating = false;
    state.resolvedCampaign = null;
    state.marketingTemplateStep = 1;
    state.currentTemplateId = state.currentTemplateId || state.templates[0]?.id || null;
    if (state.currentTemplateId && !state.templates.some((x) => x.id === state.currentTemplateId)) {
      state.currentTemplateId = state.templates[0]?.id || null;
    }

    renderTemplateList();
    loadTemplateEditor();
    renderMarketingTemplateSelect();
    refreshMarketingPreview();
    refreshMarketingRecipientStatuses().catch(() => { });
  })();

  state.templateDataLoadPromise = job;
  try {
    await job;
  } finally {
    if (state.templateDataLoadPromise === job) {
      state.templateDataLoadPromise = null;
    }
  }
}

async function runInitialAppWarmup() {
  if (state.startupWarmupPromise) return await state.startupWarmupPromise;

  const job = (async () => {
    await waitForNextPaint();

    const templateWarmup = reloadTemplateData().catch((e) => {
      console.warn("Background template warmup failed:", e);
    });

    await refreshWaChats({
      refreshMessages: true,
      markRead: true,
      includePhotos: false,
      ensureHistory: false,
      forceHistory: false,
      showLoadingMessages: false
    }).catch((e) => {
      console.warn("Initial WhatsApp chat load failed:", e);
    });

    await waitForNextPaint();

    const whatsappWarmup = state.waConnected
      ? refreshWaChatsWithHistoryWarmup().catch((e) => {
        console.warn("WhatsApp history warmup failed:", e);
      })
      : (() => {
        state.waForceHistoryRefreshOnConnected = true;
        return Promise.resolve();
      })();

    await Promise.allSettled([templateWarmup, whatsappWarmup]);
  })();

  state.startupWarmupPromise = job;
  try {
    await job;
  } finally {
    if (state.startupWarmupPromise === job) {
      state.startupWarmupPromise = null;
    }
  }
}

async function loadInitialDataAfterLogin() {
  if (state.initialDataLoadPromise) return await state.initialDataLoadPromise;

  const job = (async () => {
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

    const range = monthRangeMonthsAgo(el("marketingMonthsAgo").value);
    el("pastPatientRangeText").textContent = `Range: ${range.label} (${formatDateForMessage(
      range.startTs,
      "english"
    )} - ${formatDateForMessage(range.endTs, "english")})`;
    state.marketingLoadedPatients = [];
    state.marketingLoadedPage = 1;
    renderMarketingLoadedPatients();

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
    state.waForceHistoryRefreshOnConnected = !connRes?.connected;
    state.waChatsReqSeq = 0;
    state.waMessagesReqSeq = 0;
    state.waDropDepth = 0;
    state.waEmojiPickerOpen = false;
    state.batchSending = false;
    state.batchStopPending = false;
    state.currentBatchQueueType = "";
    state.queueStats = { total: 0, byIndex: {} };
    state.lastMarketingSkippedPhones = [];
    state.lastMarketingSkippedTemplateId = "";
    state.marketingBlastGuard = normalizeMarketingBlastGuard(null);
    setWaDropActive(false);
    stopWaOutgoingTyping({ sendPaused: false });
    clearAllWaPresenceState({ render: false });
    setWaComposerSending(false);
    setWaPendingAttachments([]);
    closeWaEmojiPicker({ restoreFocus: false });
    el("waChatSearchInput").value = "";
    renderMarketingRecipients();
    renderMarketingLoadedPatients();
    renderActivity();
    renderMarketingBlastHistory();
    setBatchSending(false);

    await waitForNextPaint();

    queueMicrotask(() => {
      refreshMarketingBlastGuard().catch(() => { });
      refreshMarketingRecipientStatuses().catch(() => { });
      refreshMarketingBlastHistory().catch(() => { });
      startMarketingBlastCooldownTicker();
    });

    queueMicrotask(() => {
      runInitialAppWarmup().catch((e) => {
        console.warn("Initial app warmup failed:", e);
      });
    });
  })();

  state.initialDataLoadPromise = job;
  try {
    await job;
  } finally {
    if (state.initialDataLoadPromise === job) {
      state.initialDataLoadPromise = null;
    }
  }
}

function showLoginScreen() {
  state.initialDataLoadPromise = null;
  state.session = { authToken: "", user: {} };
  state.waConnected = false;
  state.waConnecting = false;
  state.waConnToggleBusy = false;
  state.waQrDataUrl = "";
  state.waPairingCode = "";
  state.settingsProfileId = "";
  state.appointments = [];
  state.selectedAppointmentIds = new Set();
  state.apptReqSeq = 0;
  state.marketingRecipients = [];
  state.marketingLoadedPatients = [];
  state.marketingLoadedPage = 1;
  state.marketingCampaigns = [];
  state.currentCampaignId = "";
  state.currentCampaignEditorId = "";
  state.resolvedCampaign = null;
  state.marketingTemplateStep = 1;
  state.marketingRecipientFilter = "not_sent";
  state.marketingSentStatusByPhone = {};
  state.marketingDailyLimit = {
    limit: MARKETING_BLAST_LIMIT,
    sent_last_24h: 0,
    remaining: MARKETING_BLAST_LIMIT,
    limit_reached: false,
    loaded: false
  };
  state.marketingRecentSentAtByPhone = {};
  state.marketingBlastHistoryRows = [];
  state.lastMarketingSkippedPhones = [];
  state.lastMarketingSkippedTemplateId = "";
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
  state.batchSending = false;
  state.batchStopPending = false;
  state.currentBatchQueueType = "";
  state.queueStats = { total: 0, byIndex: {} };
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
  if (state.marketingBlastCooldownTimer) {
    clearInterval(state.marketingBlastCooldownTimer);
    state.marketingBlastCooldownTimer = null;
  }
  closeWaImageLightbox();
  closeWaEmojiPicker({ restoreFocus: false });
  setConnectionBadge(false, "Not connected", false);
  setBatchSending(false);
  clearConnectPreview({ clearPairing: true });
  renderWaChatList();
  renderWaConversationHead();
  renderWaMessages();

  el("appShell").classList.add("hidden");
  el("loginScreen").classList.remove("hidden");
  el("loginEmail").disabled = false;
  el("loginPassword").disabled = false;
  el("btnLogin").disabled = false;
  el("loginPassword").value = "";
  el("loginStatus").textContent = "Use your clinic account to continue.";
  updateMarketingBlastControls();
  setTimeout(() => {
    const emailInput = el("loginEmail");
    if (emailInput && !emailInput.disabled) emailInput.focus();
  }, 0);
}

function showAppShell() {
  el("loginScreen").classList.add("hidden");
  el("appShell").classList.remove("hidden");
}

async function afterLoginLoad() {
  showAppShell();
  updateHeaderGreeting();
  setActiveTab("whatsapp");
  await waitForNextPaint();
  queueMicrotask(() => {
    loadInitialDataAfterLogin().catch((e) => {
      console.warn("Initial data load failed:", e);
      toast("Startup", `Failed to load app data: ${String(e?.message || e)}`);
    });
  });
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
  setBatchSending(false);
  renderQueueStats();
  setQueueActiveView(state.queueActiveView);

  document.querySelectorAll(".tabBtn").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const tabName = String(btn.dataset.tab || "");
      setActiveTab(tabName);

      if (tabName === "templates" || tabName === "marketing") {
        try {
          await reloadTemplateData();
        } catch (e) {
          toast("Template", String(e?.message || e));
        }
      }

      if (tabName === "marketing") {
        await refreshMarketingBlastGuard().catch(() => { });
        await refreshMarketingRecipientStatuses().catch(() => { });
      }

      if (tabName === "queue") {
        setQueueActiveView(state.queueActiveView);
        await refreshMarketingBlastHistory().catch(() => { });
      }

      if (tabName === "whatsapp") {
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

  document.querySelectorAll(".queueModeTab").forEach((btn) => {
    btn.addEventListener("click", async () => {
      setQueueActiveView(btn.dataset.queueView);
      if (normalizeQueueView(btn.dataset.queueView) === "marketing") {
        await refreshMarketingBlastHistory().catch(() => { });
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

  if (el("tab-appointment")) {
    const autoLoadAppointments = async () => {
      try {
        await loadAppointments();
      } catch (e) {
        toast("Appointments", String(e?.message || e));
      }
    };

    el("apptBranchSelect")?.addEventListener("change", autoLoadAppointments);
    el("apptDateInput")?.addEventListener("change", autoLoadAppointments);

    el("btnApptPrevDay")?.addEventListener("click", async () => {
      const cur = String(el("apptDateInput")?.value || "").trim() || getTodayYmdKl();
      el("apptDateInput").value = shiftYmdKl(cur, -1);
      await autoLoadAppointments();
    });

    el("btnApptNextDay")?.addEventListener("click", async () => {
      const cur = String(el("apptDateInput")?.value || "").trim() || getTodayYmdKl();
      el("apptDateInput").value = shiftYmdKl(cur, 1);
      await autoLoadAppointments();
    });

    el("btnApptSelectAll")?.addEventListener("click", () => {
      state.selectedAppointmentIds = new Set(state.appointments.map((a) => String(a.id)));
      renderAppointmentTable();
    });

    el("btnApptClearSelection")?.addEventListener("click", () => {
      state.selectedAppointmentIds = new Set();
      renderAppointmentTable();
    });

    el("btnSelectRemind")?.addEventListener("click", () => selectAppointmentsByRule(appointmentIsRemindEligible));
    el("btnSelectFollow")?.addEventListener("click", () => selectAppointmentsByRule(appointmentIsFollowEligible));
    el("btnSelectReview")?.addEventListener("click", () => selectAppointmentsByRule(appointmentIsFollowEligible));

    el("btnSendRemind")?.addEventListener("click", async () => {
      try {
        await doAppointmentSend("remindAppointment", "langRemind", "aiRemind");
      } catch (e) {
        toast("Send error", String(e?.message || e));
      }
    });

    el("btnSendFollow")?.addEventListener("click", async () => {
      try {
        await doAppointmentSend("followUp", "langFollow", "aiFollow");
      } catch (e) {
        toast("Send error", String(e?.message || e));
      }
    });

    el("btnSendReview")?.addEventListener("click", async () => {
      try {
        await doAppointmentSend("requestReview", "langReview", "aiReview");
      } catch (e) {
        toast("Send error", String(e?.message || e));
      }
    });
  }

  el("btnAddMarketingFromText").addEventListener("click", async () => {
    const inputEl = el("marketingPasteInput");
    const lines = (inputEl?.value || "")
      .split(/\r?\n/)
      .map((x) => x.trim())
      .filter(Boolean);

    if (lines.length === 0) {
      toast("Recipients", "Type at least one phone number first");
      return;
    }

    let added = 0;
    let existed = 0;
    let invalid = 0;
    const invalidSamples = [];
    for (const line of lines) {
      const parsed = parseMarketingManualRecipientLine(line);
      const phoneRaw = String(parsed?.phoneRaw || "").trim();
      const nameRaw = String(parsed?.nameRaw || "").trim();
      if (!isValidPhone(phoneRaw)) {
        invalid++;
        if (invalidSamples.length < 3) invalidSamples.push(line);
        continue;
      }
      const wasAdded = upsertMarketingRecipient({
        phone: phoneRaw,
        name: nameRaw,
        selected: true
      });
      if (wasAdded) added++;
      else existed++;
    }

    renderMarketingRecipients();
    refreshMarketingPreview();
    if (inputEl && added > 0) inputEl.value = "";
    if (added > 0 || existed > 0) {
      refreshMarketingRecipientStatuses().catch(() => { });
    }

    const summary = [];
    if (added > 0) summary.push(`Added ${added}`);
    if (existed > 0) summary.push(`Already in list ${existed}`);
    if (invalid > 0) summary.push(`Invalid ${invalid}`);

    const detail = invalidSamples.length > 0 ? ` Invalid row: ${invalidSamples.join(" | ")}` : "";
    toast(
      "Recipients",
      added === 0 && existed === 0 && invalid > 0
        ? `No valid phone found. Use 0131231231, Ali.${detail}`
        : summary.length > 0
          ? summary.join(", ") + detail
          : "No recipients were added"
    );
  });

  el("btnClearMarketingRecipients").addEventListener("click", () => {
    state.marketingRecipients = [];
    state.marketingSentStatusByPhone = {};
    state.marketingRecentSentAtByPhone = {};
    state.lastMarketingSkippedPhones = [];
    state.lastMarketingSkippedTemplateId = "";
    renderMarketingRecipients();
    refreshMarketingPreview();
  });

  el("btnSelectMarketingSkipped").addEventListener("click", () => {
    const phones = Array.isArray(state.lastMarketingSkippedPhones) ? state.lastMarketingSkippedPhones : [];
    if (phones.length === 0) {
      toast("Recipients", "No skipped recipients from last send");
      return;
    }
    selectMarketingRecipientsByPhones(phones);
  });

  el("marketingSelectAll").addEventListener("change", () => {
    const checked = !!el("marketingSelectAll").checked;
    getFilteredMarketingRecipients().forEach((row) => {
      row.selected = checked && marketingRecipientStatusInfo(row).kind !== "already_sent";
    });
    renderMarketingRecipients();
    refreshMarketingPreview();
  });

  el("marketingRecipientFilter").addEventListener("change", () => {
    state.marketingRecipientFilter = String(el("marketingRecipientFilter").value || "all");
    renderMarketingRecipients();
    refreshMarketingPreview();
  });

  el("marketingCampaignSelect").addEventListener("change", async () => {
    state.currentCampaignId = "";
    state.resolvedCampaign = null;
  });

  el("marketingTemplateSelect").addEventListener("change", async () => {
    state.currentTemplateId = el("marketingTemplateSelect").value;
    state.marketingDailyLimit = {
      limit: MARKETING_BLAST_LIMIT,
      sent_last_24h: 0,
      remaining: MARKETING_BLAST_LIMIT,
      limit_reached: false,
      loaded: false
    };
    renderTemplateList();
    loadTemplateEditor();
    await refreshMarketingRecipientStatuses().catch(() => { });
    await refreshMarketingLoadedPatientStatuses().catch(() => { });
    refreshMarketingPreview();
  });

  el("btnMarketingUseTemplate1").addEventListener("click", async () => {
    toast("Marketing", "Campaign template steps are disabled. Select a template from the dropdown.");
  });

  el("btnMarketingUseTemplate2").addEventListener("click", async () => {
    toast("Marketing", "Campaign template steps are disabled. Select a template from the dropdown.");
  });

  el("btnLoadDueFollowups").addEventListener("click", async () => {
    toast("Due follow-ups", "Follow-up campaign flow is disabled for now.");
  });

  el("btnLoadPastPatients").addEventListener("click", async () => {
    const btn = el("btnLoadPastPatients");
    try {
      if (btn) btn.disabled = true;
      await loadPastPatients();
      refreshMarketingPreview();
    } catch (e) {
      toast("Loaded patients", String(e?.message || e));
    } finally {
      if (btn) btn.disabled = false;
    }
  });

  el("marketingMonthsAgo").addEventListener("input", () => {
    const range = monthRangeMonthsAgo(el("marketingMonthsAgo").value);
    el("pastPatientRangeText").textContent = `Range: ${range.label} (${formatDateForMessage(
      range.startTs,
      "english"
    )} - ${formatDateForMessage(range.endTs, "english")})`;
    state.marketingLoadedPatients = [];
    state.marketingLoadedPage = 1;
    renderMarketingLoadedPatients();
  });

  el("marketingBranchSelect").addEventListener("change", () => {
    const range = monthRangeMonthsAgo(el("marketingMonthsAgo").value);
    el("pastPatientRangeText").textContent = `Range: ${range.label} (${formatDateForMessage(
      range.startTs,
      "english"
    )} - ${formatDateForMessage(range.endTs, "english")})`;
    state.marketingLoadedPatients = [];
    state.marketingLoadedPage = 1;
    state.marketingDailyLimit = {
      limit: MARKETING_BLAST_LIMIT,
      sent_last_24h: 0,
      remaining: MARKETING_BLAST_LIMIT,
      limit_reached: false,
      loaded: false
    };
    renderMarketingLoadedPatients();
    renderMarketingRecipients();
    refreshMarketingPreview();
    refreshMarketingRecipientStatuses().catch(() => { });
  });

  el("marketingPageSize").addEventListener("input", () => {
    const nextSize = clamp(el("marketingPageSize").value, 10, 500, 35);
    state.marketingLoadedPageSize = nextSize;
    el("marketingPageSize").value = String(nextSize);
    state.marketingLoadedPage = 1;
    renderMarketingLoadedPatients();
  });

  el("marketingLoadedSelectAll").addEventListener("change", () => {
    const checked = !!el("marketingLoadedSelectAll").checked;
    for (const row of getMarketingLoadedPageRows()) {
      row.selected = checked && marketingRecipientStatusInfo(row).kind !== "already_sent";
    }
    renderMarketingLoadedPatients();
  });

  el("btnMarketingLoadedPrev").addEventListener("click", () => {
    state.marketingLoadedPage = Math.max(1, toInt(state.marketingLoadedPage, 1) - 1);
    renderMarketingLoadedPatients();
  });

  el("btnMarketingLoadedNext").addEventListener("click", () => {
    state.marketingLoadedPage = Math.min(getMarketingLoadedTotalPages(), toInt(state.marketingLoadedPage, 1) + 1);
    renderMarketingLoadedPatients();
  });

  el("btnMarketingAddLoadedToList").addEventListener("click", () => {
    const selected = getSelectedMarketingLoadedPatients();
    if (selected.length === 0) {
      toast("Loaded patients", "No selected patients to add");
      return;
    }

    let added = 0;
    let existed = 0;
    for (const row of selected) {
      if (marketingRecipientStatusInfo(row).kind === "already_sent") {
        row.selected = false;
        continue;
      }
      const wasAdded = upsertMarketingRecipient({
        phone: row.phone,
        name: row.name,
        dentist: row.dentist,
        gender: row.gender,
        ic_number: row.ic_number,
        apptDate: row.apptDate,
        apptStartTime: row.apptStartTime,
        selected: true
      });
      if (wasAdded) added++;
      else existed++;
      row.selected = false;
    }

    renderMarketingRecipients();
    refreshMarketingPreview();
    renderMarketingLoadedPatients();
    refreshMarketingRecipientStatuses().catch(() => { });
    toast("Send list", `Added ${added}${existed ? ` (already existed: ${existed})` : ""}`);
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
      await refreshMarketingRecipientStatuses().catch(() => { });
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
      await refreshMarketingBlastGuard().catch(() => { });
      toast("Marketing send", String(e?.message || e));
    }
  });

  el("templateSearchInput")?.addEventListener("input", () => {
    state.templateSearch = el("templateSearchInput")?.value || "";
    renderTemplateList();
  });

  el("templateStatusFilter")?.addEventListener("change", () => {
    state.templateStatusFilter = el("templateStatusFilter")?.value || "active";
    renderTemplateList();
  });

  el("templateName").addEventListener("input", () => {
    readMarketingTemplateEditorToState();
    renderTemplateList();
    renderTemplateMessageList();
    const selectedTemplate = getSelectedMarketingTemplate();
    if (selectedTemplate?.active !== false) renderMarketingTemplateSelect();
  });

  el("templateBody").addEventListener("input", () => {
    rememberTemplateCaretPosition();
    readMarketingTemplateEditorToState();
    refreshTemplateVariableSummary();
    renderTemplatePlaceholderPreview(el("templateBody").value || "");
    renderTemplateMessageList();
    renderTemplateList();
    refreshMarketingPreview();
  });
  el("templateBody").addEventListener("click", rememberTemplateCaretPosition);
  el("templateBody").addEventListener("keyup", rememberTemplateCaretPosition);
  el("templateBody").addEventListener("select", rememberTemplateCaretPosition);
  el("templateBody").addEventListener("focus", rememberTemplateCaretPosition);
  el("btnTemplateEmoji").addEventListener("click", () => {
    const btn = el("btnTemplateEmoji");
    if (!btn || btn.disabled) return;
    const nextOpen = !state.templateEmojiPickerOpen;
    if (nextOpen) renderTemplateEmojiPicker();
    setTemplateEmojiPickerOpen(nextOpen);
  });
  el("templateEmojiGrid").addEventListener("click", (evt) => {
    const target = evt.target?.closest?.("button[data-emoji-value]");
    const value = String(target?.dataset?.emojiValue || "");
    if (!value) return;
    insertEmojiIntoTemplate(value);
  });

  el("templateMessageType").addEventListener("change", () => {
    readMarketingTemplateEditorToState();
    renderTemplateMessageList();
    renderTemplateMessageEditor();
    refreshTemplateVariableSummary();
    const selected = getSelectedTemplateMessage();
    renderTemplatePlaceholderPreview(selected?.text || "");
    renderTemplateList();
    refreshMarketingPreview();
  });

  el("btnTemplateAddText").addEventListener("click", () => {
    addTemplateMessageToSelectedTemplate("text");
  });

  el("btnTemplateAddImage").addEventListener("click", async () => {
    try {
      const msg = addTemplateMessageToSelectedTemplate("image");
      if (msg) await attachMediaToSelectedTemplateMessage();
    } catch (e) {
      toast("Template", String(e?.message || e));
    }
  });

  el("btnTemplateAddVideo").addEventListener("click", async () => {
    try {
      const msg = addTemplateMessageToSelectedTemplate("video");
      if (msg) await attachMediaToSelectedTemplateMessage();
    } catch (e) {
      toast("Template", String(e?.message || e));
    }
  });

  el("btnTemplateAddDocument").addEventListener("click", async () => {
    try {
      const msg = addTemplateMessageToSelectedTemplate("document");
      if (msg) await attachMediaToSelectedTemplateMessage();
    } catch (e) {
      toast("Template", String(e?.message || e));
    }
  });

  el("btnTemplateDeleteMessage").addEventListener("click", () => {
    const template = getSelectedMarketingTemplate();
    if (!template) return;
    readMarketingTemplateEditorToState();
    const messages = ensureTemplateMessages(template);
    if (messages.length <= 1) return;
    const selectedId = String(state.currentTemplateMessageId || "");
    const idx = Math.max(
      0,
      messages.findIndex((x) => x.id === selectedId)
    );
    messages.splice(idx, 1);
    const fallback = messages[Math.max(0, idx - 1)] || messages[0] || null;
    state.currentTemplateMessageId = fallback ? fallback.id : null;
    updateTemplateDerivedFields(template);
    renderTemplateMessageList();
    renderTemplateMessageEditor();
    refreshTemplateVariableSummary();
    const nextSelected = getSelectedTemplateMessage(template);
    renderTemplatePlaceholderPreview(nextSelected?.text || "");
    renderTemplateList();
    refreshMarketingPreview();
  });

  el("btnTemplateAttachMedia").addEventListener("click", async () => {
    try {
      await attachMediaToSelectedTemplateMessage();
    } catch (e) {
      toast("Template", String(e?.message || e));
    }
  });

  el("btnTemplateClearMedia").addEventListener("click", () => {
    const template = getSelectedMarketingTemplate();
    if (!template) return;
    const message = getSelectedTemplateMessage(template);
    if (!message) return;
    if (normalizeTemplateMessageType(message.type) === "text") return;
    message.attachment = null;
    updateTemplateDerivedFields(template);
    renderTemplateMessageList();
    renderTemplateMessageEditor();
    renderTemplateList();
    refreshMarketingPreview();
  });

  el("templateSendPolicy").addEventListener("change", () => {
    readMarketingTemplateEditorToState();
  });

  el("campaignEditorSelect").addEventListener("change", () => {
    state.currentCampaignEditorId = cleanString(el("campaignEditorSelect").value);
    state.campaignEditorCreating = !state.currentCampaignEditorId;
    if (state.campaignEditorCreating && !cleanString(state.campaignDraftKey)) enterNewCampaignMode("");
    renderCampaignManager();
  });

  el("btnNewCampaign").addEventListener("click", () => {
    enterNewCampaignMode("");
    renderCampaignManager();
    renderTemplateList();
    loadTemplateEditor();
    const nameInput = document.getElementById("campaignName");
    if (nameInput) nameInput.focus();
  });

  el("campaignName").addEventListener("input", () => {
    if (!state.campaignEditorCreating) return;
    const name = cleanString(document.getElementById("campaignName")?.value);
    state.campaignDraftName = name;
    syncCampaignDraftTemplateNames(name);
  });

  el("btnSaveCampaign").addEventListener("click", async () => {
    toast("Campaign", "Campaign flow is disabled for now. Create or edit marketing templates below.");
  });

  el("btnNewTemplate").addEventListener("click", () => {
    if (!canManageMasterMarketingTemplates()) {
      toast("Templates", "Only Marketing or Developer can create master templates");
      return;
    }
    const id = `t_${Date.now().toString(16)}_${Math.random().toString(16).slice(2, 8)}`;
    const defaultMessage = buildDefaultTemplateMessage("text");
    defaultMessage.text = "Hello {name},";
    state.templates.push(
      normalizeTemplate(
        {
          id,
          name: "New template",
          body: defaultMessage.text,
          messages: [defaultMessage],
          variables: ["name"],
          sendPolicy: "once"
        },
        state.templates.length
      )
    );
    state.currentTemplateId = id;
    const selectedTemplate = getSelectedMarketingTemplate();
    state.currentTemplateMessageId = selectedTemplate?.messages?.[0]?.id || null;
    renderTemplateList();
    loadTemplateEditor();
    renderCampaignManager();
    renderMarketingTemplateSelect();
  });

  el("btnDeleteTemplate").addEventListener("click", () => {
    if (!canManageMasterMarketingTemplates()) {
      toast("Templates", "Only Marketing or Developer can delete master templates");
      return;
    }
    const selected = getSelectedMarketingTemplate();
    if (!selected) return;

    if (!window.confirm(`Delete template \"${selected.name}\"?`)) return;

    state.templates = state.templates.filter((t) => t.id !== selected.id);
    state.currentTemplateId = state.templates[0]?.id || null;
    renderTemplateList();
    loadTemplateEditor();
    renderCampaignManager();
    renderMarketingTemplateSelect();
    refreshMarketingPreview();
  });

  el("btnSaveTemplate").addEventListener("click", async () => {
    try {
      readMarketingTemplateEditorToState();
      const targetTemplates = canManageMasterMarketingTemplates()
        ? state.templates
        : [getSelectedMarketingTemplate()].filter(Boolean);
      if (!canManageMasterMarketingTemplates() && !canEditBranchMarketingTemplates()) {
        throw new Error("You do not have permission to save marketing templates");
      }
      const normalized = targetTemplates.map((t, idx) => normalizeTemplate(t, idx));
      await window.api.saveTemplates(normalized);
      await reloadTemplateData();
      renderTemplateList();
      renderCampaignManager();
      renderMarketingTemplateSelect();
      toast("Templates", canManageMasterMarketingTemplates() ? "Marketing templates saved" : "Branch template saved");
    } catch (e) {
      toast("Templates", String(e?.message || e));
    }
  });

  el("btnSaveAppointmentTemplates")?.addEventListener("click", async () => {
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
      if (!canManageMasterMarketingTemplates()) {
        throw new Error("Only Marketing or Developer can import master templates");
      }
      const res = await window.api.importTemplatesBundle({ mode: "single_marketing_template" });
      if (!res?.ok && res?.canceled) return;
      if (!res?.ok) throw new Error("Import failed");
      await reloadTemplateData();
      const action = res?.replaced ? "replaced" : "added";
      const name = String(res?.templateName || "").trim() || "Template";
      toast("Templates", `Imported ${action}: ${name}`);
    } catch (e) {
      toast("Templates import", String(e?.message || e));
    }
  });

  el("btnExportTemplates").addEventListener("click", async () => {
    try {
      const selected = getSelectedMarketingTemplate();
      if (!selected) throw new Error("Select a marketing template first");
      const res = await window.api.exportTemplatesBundle({
        mode: "single_marketing_template",
        templateId: selected.id
      });
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
      await syncConnectionStateFromBackend().catch(() => { });
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
      await syncConnectionStateFromBackend().catch(() => { });
      toast("Profile", `Deleted ${name}`);
    } catch (e) {
      toast("Profile", String(e?.message || e));
    }
  });

  el("btnClearActivity")?.addEventListener("click", () => {
    state.activityRows = state.activityRows.filter((row) => String(row?.queueType || "appointment") !== "appointment");
    resetQueueStats(0);
    renderActivity();
  });

  el("btnRefreshMarketingHistory").addEventListener("click", async () => {
    await refreshMarketingBlastHistory().catch(() => { });
  });
  el("btnStopSending")?.addEventListener("click", async () => {
    if (!state.batchSending) {
      setBatchSending(false);
      toast("Send", "No active sending process");
      return;
    }

    setBatchStopPending(true);
    try {
      const res = await window.api.waStopBatch();
      if (res?.ok) {
        toast("Send", "Stopping send process...");
        return;
      }
      setBatchSending(false);
      toast("Send", String(res?.message || "No active sending process"));
    } catch (e) {
      setBatchStopPending(false);
      toast("Send", String(e?.message || e));
    }
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
    setTemplateEmojiPickerOpen(false);
  });

  document.addEventListener("mousedown", (evt) => {
    const target = evt.target;
    if (!target) return;

    if (state.waEmojiPickerOpen) {
      const picker = el("waEmojiPicker");
      const emojiBtn = el("btnWaEmoji");
      if (picker && emojiBtn && !picker.contains(target) && !emojiBtn.contains(target)) {
        closeWaEmojiPicker({ restoreFocus: false });
      }
    }

    if (state.templateEmojiPickerOpen) {
      const picker = el("templateEmojiPicker");
      const emojiBtn = el("btnTemplateEmoji");
      if (picker && emojiBtn && !picker.contains(target) && !emojiBtn.contains(target)) {
        setTemplateEmojiPickerOpen(false);
      }
    }
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
    state.waConnected = connected;
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
      if (connecting) {
        stopWaOutgoingTyping({ sendPaused: false });
        clearAllWaPresenceState({ render: true });
      } else {
        resetWaChatUiState({ render: true });
      }
    }

    if (!prevConnected && connected && state.waForceHistoryRefreshOnConnected) {
      state.waForceHistoryRefreshOnConnected = false;
      refreshWaChatsWithHistoryWarmup().catch(() => { });
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

    if (!connected && !connecting) {
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
    if (row.status === "stopped") {
      setBatchSending(false);
    }
    updateQueueStatsFromProgress(row);
    const normalizedStatus = String(row.status || "")
      .trim()
      .toLowerCase();
    pushActivity({
      ts: row.ts || "",
      phone: row.phone || "",
      status: normalizedStatus || "sending",
      error: row.error || "",
      queueType: String(row.queueType || state.currentBatchQueueType || "appointment")
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
