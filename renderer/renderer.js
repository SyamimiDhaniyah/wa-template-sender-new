
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
  activeTab: "whatsapp",
  waChats: [],
  waActiveChatJid: "",
  waMessages: [],
  waChatSearch: "",
  waPendingAttachment: null,
  waSyncTimer: null,
  waLoadingChats: false,
  waLoadingMessages: false,
  waRefreshQueued: false,
  waChatsReqSeq: 0,
  waMessagesReqSeq: 0,
  profiles: [],
  activeProfileId: null,
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

function setConnectionBadge(connected, text) {
  state.waConnected = !!connected;
  const dot = el("connDot");
  if (dot) {
    dot.classList.remove("online", "offline");
    dot.classList.add(connected ? "online" : "offline");
  }
  const msg = text || (connected ? "Connected" : "Not connected");
  el("connText").textContent = msg;
  el("waStatusText").textContent = msg;
}

function setActiveTab(tabName) {
  const tab = String(tabName || "whatsapp");
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

function renderWaAttachmentRow() {
  const row = el("waAttachmentRow");
  const label = el("waAttachmentMeta");
  const att = state.waPendingAttachment;
  if (!att) {
    row.classList.add("hidden");
    label.textContent = "Attachment";
    return;
  }

  const bits = [];
  bits.push(att.fileName || "Attachment");
  if (att.kind) bits.push(att.kind);
  const sizeText = formatWaBytes(att.size);
  if (sizeText) bits.push(sizeText);
  label.textContent = bits.join(" | ");
  row.classList.remove("hidden");
}

function setWaPendingAttachment(attachment) {
  state.waPendingAttachment = attachment || null;
  renderWaAttachmentRow();
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

    const avatar = document.createElement(chat.avatarUrl ? "img" : "div");
    avatar.className = "waChatAvatar";
    if (chat.avatarUrl) {
      avatar.src = chat.avatarUrl;
      avatar.alt = chat.title || "Avatar";
      avatar.loading = "lazy";
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

    if (!msg.fromMe && msg.senderName) {
      const sender = document.createElement("div");
      sender.className = "waSender";
      sender.textContent = msg.senderName;
      bubble.appendChild(sender);
    }

    if (msg.hasMedia && msg.media) {
      if (msg.media.thumbnailDataUrl) {
        const img = document.createElement("img");
        img.className = "waMediaThumb";
        img.src = msg.media.thumbnailDataUrl;
        img.alt = msg.media.fileName || msg.media.kind || "media";
        bubble.appendChild(img);
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
    }

    if (msg.text) {
      const text = document.createElement("div");
      text.className = "waMessageText";
      text.textContent = msg.text;
      bubble.appendChild(text);
    }

    const ts = document.createElement("div");
    ts.className = "waMsgTime";
    ts.textContent = formatWaTimeShort(msg.timestampMs);
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
  if (!state.waActiveChatJid) {
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
      chatJid: state.waActiveChatJid,
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

  if (opts.markRead !== false) {
    try {
      await window.api.waMarkChatRead({ chatJid: state.waActiveChatJid });
      if (requestId !== state.waMessagesReqSeq) return;
      state.waChats = state.waChats.map((chat) =>
        chat.jid === state.waActiveChatJid
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
  state.waActiveChatJid = next;
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
    const res = await window.api.waGetRecentChats({
      search: state.waChatSearch || "",
      limit: 220,
      includePhotos: opts.includePhotos !== false,
      ensureHistory: opts.ensureHistory === true,
      forceHistory: opts.forceHistory === true,
      maxPhotoFetch: Number.isFinite(Number(opts.maxPhotoFetch)) ? Number(opts.maxPhotoFetch) : 35,
      minMinutesBetweenPhotoChecks: Number.isFinite(Number(opts.minMinutesBetweenPhotoChecks))
        ? Number(opts.minMinutesBetweenPhotoChecks)
        : 120
    });
    if (requestId !== state.waChatsReqSeq) return;
    state.waChats = Array.isArray(res?.chats) ? res.chats : [];
    if (!state.waActiveChatJid || !state.waChats.some((x) => x.jid === state.waActiveChatJid)) {
      state.waActiveChatJid = state.waChats[0]?.jid || "";
      if (!state.waActiveChatJid) state.waMessages = [];
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

function scheduleWaSyncRefresh() {
  if (state.waSyncTimer) clearTimeout(state.waSyncTimer);
  state.waSyncTimer = setTimeout(() => {
    state.waSyncTimer = null;
    refreshWaChats({
      refreshMessages: true,
      markRead: false,
      showLoadingMessages: false,
      includePhotos: false
    }).catch(() => {});
  }, 160);
}

async function pickWaAttachment() {
  const res = await window.api.waPickAttachment();
  if (!res?.ok && res?.canceled) return;
  if (!res?.ok) throw new Error("Attachment selection failed");
  setWaPendingAttachment(res.attachment || null);
}

async function sendWaComposerMessage() {
  if (!state.waActiveChatJid) throw new Error("Please select a chat");

  const text = String(el("waComposerInput").value || "");
  const payload = {
    chatJid: state.waActiveChatJid,
    text,
    attachment: state.waPendingAttachment || null
  };
  const hasText = text.trim().length > 0;
  const hasAttachment = !!state.waPendingAttachment;
  if (!hasText && !hasAttachment) return;

  el("btnWaSend").disabled = true;
  try {
    await window.api.waSendChatMessage(payload);
    el("waComposerInput").value = "";
    setWaPendingAttachment(null);
    await refreshWaMessages({ markRead: false, forceBottom: true, showLoading: false });
    await refreshWaChats({ refreshMessages: false, markRead: false });
  } finally {
    el("btnWaSend").disabled = false;
  }
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
    const tdStatus = document.createElement("td");
    tdStatus.innerHTML = appt.Status ? '<span class="badgeYes">Yes</span>' : '<span class="badgeNo">No</span>';
    tr.append(tdCb, tdTime, tdPatient, tdDentist, tdTreatment, tdStatus);
    body.appendChild(tr);
  }

  if (sorted.length === 0) {
    const tr = document.createElement("tr");
    tr.innerHTML = '<td colspan="6" class="smallText">No appointments found for this branch/date.</td>';
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
  const select = el("profileSelect");
  select.innerHTML = "";

  for (const p of state.profiles) {
    const opt = document.createElement("option");
    opt.value = p.id;
    opt.textContent = p.name;
    select.appendChild(opt);
  }
  if (state.activeProfileId) select.value = state.activeProfileId;

  const list = el("profileList");
  list.innerHTML = "";

  for (const p of state.profiles) {
    const item = document.createElement("div");
    item.className = "profileItem";
    item.innerHTML = `
      <div class="profileName">${escapeHtml(p.name)}</div>
      <div class="profileMeta">${escapeHtml(p.id)}${p.id === state.activeProfileId ? " (active)" : ""}</div>
      <div class="profileBtns">
        <button class="btnGhost" data-act="use" data-id="${escapeHtml(p.id)}">Use</button>
        <button class="btnGhost" data-act="rename" data-id="${escapeHtml(p.id)}">Rename</button>
        <button class="btnGhost" data-act="terminate" data-id="${escapeHtml(p.id)}">Terminate</button>
        <button class="btnGhost" data-act="delete" data-id="${escapeHtml(p.id)}">Delete</button>
      </div>
    `;

    item.querySelectorAll("button").forEach((btn) => {
      btn.addEventListener("click", async () => {
        const id = btn.dataset.id;
        const act = btn.dataset.act;
        try {
          if (act === "use") {
            await window.api.setActiveProfile(id);
            await refreshProfiles();
            toast("Profile", "Switched active profile");
          }

          if (act === "rename") {
            const current = state.profiles.find((x) => x.id === id);
            const next = window.prompt("New profile name", current?.name || "");
            if (!next) return;
            await window.api.renameProfile(id, next);
            await refreshProfiles();
            toast("Profile", "Renamed");
          }

          if (act === "terminate") {
            if (!window.confirm("Terminate this WhatsApp session?")) return;
            await window.api.terminateProfileSession(id);
            await refreshProfiles();
            toast("Profile", "Session terminated");
          }

          if (act === "delete") {
            if (!window.confirm("Delete this profile? This removes saved session.")) return;
            await window.api.deleteProfile(id);
            await refreshProfiles();
            toast("Profile", "Deleted");
          }
        } catch (e) {
          toast("Profile error", String(e?.message || e));
        }
      });
    });

    list.appendChild(item);
  }
}

async function refreshProfiles() {
  const res = await window.api.getProfiles();
  state.profiles = Array.isArray(res?.profiles) ? res.profiles : [];
  state.activeProfileId = res?.activeProfileId || null;
  renderProfiles();
  scheduleWaSyncRefresh();
}

function setConnectModeUi() {
  const pairing = el("connectMethod").value === "pairing";
  el("pairingPhoneWrap").classList.toggle("hidden", !pairing);
  el("pairingCodeBox").classList.toggle("hidden", !pairing);
  el("qrImg").classList.toggle("hidden", pairing);
}

async function doHandshake() {
  const method = el("connectMethod").value;
  const phoneNumber = el("pairingPhone").value.trim();
  el("qrImg").src = "";
  el("pairingCodeText").textContent = "-";

  await window.api.waHandshake({ method, phoneNumber });
  toast("Handshake", method === "pairing" ? "Requesting pairing code..." : "Waiting for QR...");
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

  applySettingsToUi();
  renderBranchesToSelect("apptBranchSelect", userBranch);
  renderBranchesToSelect("marketingBranchSelect", userBranch);

  el("apptDateInput").value = getTodayYmdKl();
  const range = monthRangeMonthsAgo(el("marketingMonthsAgo").value);
  el("pastPatientRangeText").textContent = `Range: ${range.label}`;

  renderProfiles();
  setConnectModeUi();
  setConnectionBadge(!!connRes?.connected, connRes?.text || "Not connected");
  state.waChatSearch = "";
  state.waActiveChatJid = "";
  state.waMessages = [];
  state.waChats = [];
  state.waLoadingChats = false;
  state.waLoadingMessages = false;
  state.waRefreshQueued = false;
  state.waChatsReqSeq = 0;
  state.waMessagesReqSeq = 0;
  setWaPendingAttachment(null);
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
  state.appointments = [];
  state.selectedAppointmentIds = new Set();
  state.marketingRecipients = [];
  state.waChats = [];
  state.waActiveChatJid = "";
  state.waMessages = [];
  state.waChatSearch = "";
  state.waLoadingChats = false;
  state.waLoadingMessages = false;
  state.waRefreshQueued = false;
  state.waChatsReqSeq = 0;
  state.waMessagesReqSeq = 0;
  setWaPendingAttachment(null);
  el("waChatSearchInput").value = "";
  el("waComposerInput").value = "";
  if (state.waSyncTimer) {
    clearTimeout(state.waSyncTimer);
    state.waSyncTimer = null;
  }
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

  el("btnWaClearAttachment").addEventListener("click", () => {
    setWaPendingAttachment(null);
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
    try {
      await doHandshake();
    } catch (e) {
      toast("Handshake", String(e?.message || e));
    }
  });

  el("profileSelect").addEventListener("change", async () => {
    try {
      const id = el("profileSelect").value;
      if (!id) return;
      await window.api.setActiveProfile(id);
      await refreshProfiles();
      const status = await window.api.waGetConnectionState();
      setConnectionBadge(!!status?.connected, status?.text || "Not connected");
      await refreshWaChats({
        refreshMessages: true,
        markRead: true,
        ensureHistory: true,
        includePhotos: true
      });
    } catch (e) {
      toast("Profile", String(e?.message || e));
    }
  });

  el("btnCreateProfile").addEventListener("click", async () => {
    const name = el("newProfileName").value.trim();
    if (!name) return toast("Profile", "Please enter profile name");

    try {
      await window.api.createProfile(name);
      el("newProfileName").value = "";
      await refreshProfiles();
      toast("Profile", "Created");
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

  window.api.onQR((dataUrl) => {
    el("qrImg").src = dataUrl || "";
  });

  window.api.onPairingCode((code) => {
    el("pairingCodeText").textContent = String(code || "-");
  });

  window.api.onStatus((status) => {
    if (status?.profileId && state.activeProfileId && status.profileId !== state.activeProfileId) return;
    const prevConnected = state.waConnected;
    setConnectionBadge(!!status?.connected, status?.text || "Not connected");
    if (prevConnected !== !!status?.connected || state.activeTab === "whatsapp") {
      scheduleWaSyncRefresh();
    }
  });

  window.api.onWaChatSync((payload) => {
    if (!payload) return;
    if (payload.profileId && state.activeProfileId && payload.profileId !== state.activeProfileId) return;
    scheduleWaSyncRefresh();
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
