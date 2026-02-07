let templates = [];
let currentTemplateId = null;

let profiles = [];
let activeProfileId = null;
let waConnected = false;

let recipientsData = [];
let activityRows = [];
let batchTotal = 0;
let batchDone = 0;
let aiSaveTimer = null;
let waContactsRows = [];
let waContactsFiltered = [];
let waContactSelection = new Set();
let waContactsLoadToken = 0;

const defaultAiRewriteConfig = {
  enabled: false,
  endpoint: "https://xqoc-ewo0-x3u2.s2.xano.io/api:lY50ALPv/LLM",
  authToken: "",
  prompt: "{message}",
  timeoutMs: 30000,
  fallbackToOriginal: true
};
const aiRewriteFieldIds = ["aiEndpoint", "aiAuthToken", "aiPrompt", "aiTimeoutMs", "aiFallback"];

function el(id) {
  return document.getElementById(id);
}

function escapeHtml(s) {
  return String(s || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function toast(title, body) {
  const wrap = el("toastWrap");
  const t = document.createElement("div");
  t.className = "toast";
  t.innerHTML = `<div class="toastTitle">${escapeHtml(title)}</div><div class="toastBody">${escapeHtml(
    body || ""
  )}</div>`;
  wrap.appendChild(t);

  setTimeout(() => {
    t.style.opacity = "0";
    t.style.transition = "opacity 180ms ease";
    setTimeout(() => t.remove(), 220);
  }, 2400);
}

function setHeader(title, hint) {
  el("pageTitle").textContent = title;
  el("pageHint").textContent = hint;
}

function setConnectionBadge(connected, text) {
  waConnected = !!connected;
  el("connText").textContent = text || (connected ? "Connected" : "Not connected");
  const dot = el("connDot");
  dot.classList.remove("online", "offline");
  dot.classList.add(connected ? "online" : "offline");
  el("btnSend").disabled = !waConnected;
}

async function syncConnectionStateFromBackend() {
  try {
    const s = await window.api.waGetConnectionState();
    if (!s?.ok) return;
    if (s.profileId && activeProfileId && s.profileId !== activeProfileId) return;
    el("waStatus").textContent = s.text || (s.connected ? "Connected" : s.connecting ? "Connecting..." : "Not connected");
    setConnectionBadge(!!s.connected, s.text || (s.connected ? "Connected" : "Not connected"));
  } catch (e) {
    // ignore state sync errors; live status events still update UI
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitForConnectionSync(timeoutMs = 12000, intervalMs = 650) {
  const endAt = Date.now() + Math.max(1000, Number(timeoutMs || 12000));
  while (Date.now() < endAt) {
    const s = await window.api.waGetConnectionState().catch(() => null);
    if (s?.ok) {
      if (!s.profileId || !activeProfileId || s.profileId === activeProfileId) {
        el("waStatus").textContent =
          s.text || (s.connected ? "Connected" : s.connecting ? "Connecting..." : "Not connected");
        setConnectionBadge(!!s.connected, s.text || (s.connected ? "Connected" : "Not connected"));
      }
      if (s.connected || !s.connecting) return;
    }
    await sleep(intervalMs);
  }
}

function switchView(viewName) {
  const views = ["connect", "templates", "send", "activity"];
  for (const v of views) {
    const viewEl = el(`view-${v}`);
    if (v === viewName) viewEl.classList.remove("hidden");
    else viewEl.classList.add("hidden");
  }

  document.querySelectorAll(".navItem").forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.view === viewName);
  });

  if (viewName === "connect") setHeader("Connect", "Connect WhatsApp via QR code and choose a profile");
  if (viewName === "templates") setHeader("Templates", "Manage templates and sent marks");
  if (viewName === "send") setHeader("Send", "Import recipients, preview, and send safely");
  if (viewName === "activity") setHeader("Activity", "Batch results and errors");

  if (viewName === "send") {
    renderRecipientsTable();
    refreshRecipientCount();
    refreshSendPolicyText();
    refreshSendPreview();
  }
}

function uuid() {
  return "t_" + Math.random().toString(16).slice(2) + "_" + Date.now().toString(16);
}

function isValidTemplateVarName(name) {
  return /^[A-Za-z_][A-Za-z0-9_]*$/.test(String(name || ""));
}

function extractVarsFromBody(body) {
  const set = new Set();
  const re = /\{(\w+)\}/g;
  const text = String(body || "");
  let m;
  while ((m = re.exec(text))) {
    const key = String(m[1] || "");
    if (isValidTemplateVarName(key)) set.add(key);
  }
  return Array.from(set);
}

function uniqueValidVars(arr) {
  const set = new Set();
  for (const x of Array.isArray(arr) ? arr : []) {
    const key = String(x || "").trim();
    if (isValidTemplateVarName(key)) set.add(key);
  }
  return Array.from(set);
}

function normalizeTemplate(raw, idx) {
  const t = raw && typeof raw === "object" ? raw : {};
  const variables = uniqueValidVars(t.variables);
  const bodyVars = extractVarsFromBody(t.body || "");
  for (const v of bodyVars) {
    if (!variables.includes(v)) variables.push(v);
  }

  return {
    id: String(t.id || "t_" + String(idx + 1)),
    name: String(t.name || "Untitled"),
    body: String(t.body || ""),
    variables,
    sendPolicy: t.sendPolicy === "multiple" ? "multiple" : "once"
  };
}

function normalizeTemplates(list) {
  return (Array.isArray(list) ? list : []).map((t, idx) => normalizeTemplate(t, idx));
}

function getSelectedTemplate() {
  return templates.find((t) => t.id === currentTemplateId) || null;
}

function syncTemplateVarsFromBody(template) {
  if (!template) return;
  const base = uniqueValidVars(template.variables);
  const fromBody = extractVarsFromBody(template.body || "");
  for (const v of fromBody) {
    if (!base.includes(v)) base.push(v);
  }
  template.variables = base;
}

function getTemplateVariables(template) {
  const t = template || getSelectedTemplate();
  if (!t) return [];
  return uniqueValidVars(t.variables);
}

function templateSnippet(body) {
  const s = String(body || "").replace(/\s+/g, " ").trim();
  return s.length > 84 ? s.slice(0, 84) + "..." : s;
}

function renderTemplate(body, vars) {
  return String(body || "").replace(/\{(\w+)\}/g, (_, key) => {
    const v = vars && Object.prototype.hasOwnProperty.call(vars, key) ? String(vars[key]) : "";
    return v;
  });
}

function normalizeMsisdn(input) {
  let s = String(input || "").trim();
  s = s.replace(/\s+/g, "").replace(/-/g, "");
  if (s.startsWith("+")) s = s.slice(1);
  if (s.startsWith("0")) s = "60" + s.slice(1);
  s = s.replace(/\D/g, "");
  return s;
}

function makeRecipientRow(seed) {
  const src = seed && typeof seed === "object" ? seed : {};
  return {
    phone: String(src.phone || ""),
    vars: src.vars && typeof src.vars === "object" ? { ...src.vars } : {}
  };
}

function ensureRecipientRows(min = 1) {
  while (recipientsData.length < min) recipientsData.push(makeRecipientRow());
}

function getSendPayload() {
  const vars = getTemplateVariables();
  const recipients = [];
  const varsByPhone = {};

  for (const row of recipientsData) {
    const norm = normalizeMsisdn(row.phone || "");
    if (!norm || norm.length < 8) continue;

    recipients.push(norm);

    const rowVars = {};
    for (const key of vars) {
      const val = String(row.vars?.[key] || "").trim();
      if (val) rowVars[key] = val;
    }

    if (Object.keys(rowVars).length > 0) {
      varsByPhone[norm] = rowVars;
    }
  }

  return { recipients, varsByPhone };
}

function readPacing() {
  const pattern = el("pacingPattern").value;
  const minSec = Number(el("minSec").value || 7);
  const maxSec = Number(el("maxSec").value || 10);
  return { pattern, minSec, maxSec };
}

function normalizeAiRewriteConfigClient(input) {
  const cfg = input && typeof input === "object" ? input : {};
  const timeoutRaw = Number(cfg.timeoutMs);
  const timeoutMs = Number.isFinite(timeoutRaw) ? Math.min(120000, Math.max(3000, Math.round(timeoutRaw))) : 30000;
  const endpoint = String(cfg.endpoint || "").trim() || defaultAiRewriteConfig.endpoint;

  return {
    enabled: !!cfg.enabled,
    endpoint,
    authToken: String(cfg.authToken || "").trim(),
    prompt: String(cfg.prompt || defaultAiRewriteConfig.prompt).trim(),
    timeoutMs,
    fallbackToOriginal: cfg.fallbackToOriginal !== false
  };
}

function applyAiRewriteConfigToUi(config) {
  const cfg = normalizeAiRewriteConfigClient({ ...defaultAiRewriteConfig, ...(config || {}) });
  el("aiEnable").checked = !!cfg.enabled;
  el("aiEndpoint").value = cfg.endpoint;
  el("aiAuthToken").value = cfg.authToken;
  el("aiPrompt").value = cfg.prompt;
  el("aiTimeoutMs").value = String(cfg.timeoutMs);
  el("aiFallback").checked = !!cfg.fallbackToOriginal;
  setAiRewriteUiEnabled(cfg.enabled);
}

function setAiRewriteUiEnabled(enabled) {
  const active = !!enabled;
  for (const id of aiRewriteFieldIds) {
    const node = el(id);
    if (node) node.disabled = !active;
  }
  const hint = el("aiRewriteStateText");
  if (hint) {
    hint.textContent = active
      ? "Enabled: AI can rewrite each outgoing message"
      : "Disabled: send original template message";
  }
}

function readAiRewriteConfigFromUi() {
  return normalizeAiRewriteConfigClient({
    enabled: !!el("aiEnable").checked,
    endpoint: el("aiEndpoint").value,
    authToken: el("aiAuthToken").value,
    prompt: el("aiPrompt").value,
    timeoutMs: Number(el("aiTimeoutMs").value || 30000),
    fallbackToOriginal: !!el("aiFallback").checked
  });
}

function scheduleSaveAiRewriteConfig() {
  if (aiSaveTimer) clearTimeout(aiSaveTimer);
  aiSaveTimer = setTimeout(async () => {
    aiSaveTimer = null;
    try {
      await window.api.saveAiRewriteConfig(readAiRewriteConfigFromUi());
    } catch (e) {
      // do not block sending due to config-save errors
    }
  }, 300);
}

function refreshRecipientCount() {
  const count = getSendPayload().recipients.length;
  el("totalRecipients").textContent = String(count);
}

function refreshSendPolicyText() {
  const t = getSelectedTemplate();
  if (!t) {
    el("sendPolicyText").textContent = "-";
    return;
  }
  el("sendPolicyText").textContent = t.sendPolicy === "multiple" ? "Multiple" : "Once";
}

function refreshTemplateSelect() {
  const sel = el("tplSelect");
  sel.innerHTML = "";

  for (const t of templates) {
    const opt = document.createElement("option");
    opt.value = t.id;
    opt.textContent = t.name;
    sel.appendChild(opt);
  }

  if (templates.length > 0) {
    if (!currentTemplateId || !templates.some((x) => x.id === currentTemplateId)) {
      currentTemplateId = templates[0].id;
    }
    sel.value = currentTemplateId;
  }

  refreshSendPolicyText();
}

function renderTemplateList() {
  const list = el("tplList");
  list.innerHTML = "";

  for (const t of templates) {
    const item = document.createElement("div");
    item.className = "tplItem" + (t.id === currentTemplateId ? " active" : "");
    item.innerHTML = `
      <div class="tplName">${escapeHtml(t.name || "Untitled")}</div>
      <div class="tplMeta">${escapeHtml(templateSnippet(t.body))}</div>
    `;

    item.addEventListener("click", () => {
      currentTemplateId = t.id;
      renderTemplateList();
      loadTemplateToEditor();
      refreshTemplateSelect();
      renderRecipientsTable();
      refreshRecipientCount();
      refreshSendPreview();
    });

    list.appendChild(item);
  }
}

function renderTemplateVariableList() {
  const wrap = el("tplVarList");
  wrap.innerHTML = "";

  const t = getSelectedTemplate();
  if (!t) return;

  const vars = getTemplateVariables(t);
  if (vars.length === 0) {
    const empty = document.createElement("span");
    empty.className = "smallMuted";
    empty.textContent = "No variables yet";
    wrap.appendChild(empty);
    return;
  }

  for (const key of vars) {
    const chip = document.createElement("span");
    chip.className = "chipBtn";

    const txt = document.createElement("span");
    txt.textContent = `{${key}}`;

    const del = document.createElement("button");
    del.className = "chipBtnDel";
    del.type = "button";
    del.textContent = "x";
    del.title = "Remove variable";
    del.addEventListener("click", () => {
      t.variables = getTemplateVariables(t).filter((v) => v !== key);
      renderTemplateVariableList();
      renderRecipientsTable();
      refreshSendPreview();
      renderCsvVariableMapping();
    });

    chip.appendChild(txt);
    chip.appendChild(del);
    wrap.appendChild(chip);
  }
}

function loadTemplateToEditor() {
  const t = getSelectedTemplate();
  if (!t) {
    el("tplName").value = "";
    el("tplBody").value = "";
    el("tplSendPolicy").value = "once";
    el("tplPreview").textContent = "Select a template to preview";
    el("tplVarName").value = "";
    renderTemplateVariableList();
    return;
  }

  el("tplName").value = t.name || "";
  el("tplBody").value = t.body || "";
  el("tplSendPolicy").value = t.sendPolicy === "multiple" ? "multiple" : "once";
  el("tplVarName").value = "";
  syncTemplateVarsFromBody(t);
  renderTemplateVariableList();
  refreshTemplatePreview();
}

function refreshTemplatePreview() {
  const t = getSelectedTemplate();
  if (!t) return;

  const demoVars = {
    name: "Ali",
    topic: "quotation",
    date: "10 Feb",
    time: "3:00 PM",
    branch: "Bangi",
    doctor: "Dr. Ashraf"
  };

  for (const key of getTemplateVariables(t)) {
    if (!Object.prototype.hasOwnProperty.call(demoVars, key)) {
      demoVars[key] = `[${key}]`;
    }
  }

  const preview = renderTemplate(el("tplBody").value, demoVars);
  el("tplPreview").textContent = preview || "Preview will appear here";
}

function refreshSendPreview() {
  const t = getSelectedTemplate();
  if (!t) {
    el("sendPreview").textContent = "No template selected";
    return;
  }

  const row = recipientsData.find((r) => normalizeMsisdn(r.phone || "").length >= 8);
  if (!row) {
    el("sendPreview").textContent = "Add at least one recipient to preview";
    return;
  }

  const vars = { name: "Client" };
  for (const key of getTemplateVariables(t)) {
    const val = String(row.vars?.[key] || "").trim();
    if (val) vars[key] = val;
  }

  const preview = renderTemplate(t.body || "", vars);
  el("sendPreview").textContent = preview || "Preview will appear here";
}

function setProgress(done, total) {
  batchDone = done;
  batchTotal = total;
  const pct = total > 0 ? Math.round((done / total) * 100) : 0;
  el("progressPct").textContent = `${pct}%`;
  el("progressFill").style.width = `${pct}%`;

  if (total === 0) {
    el("progressText").textContent = "Idle";
    return;
  }

  if (done >= total) el("progressText").textContent = `Completed ${done}/${total}`;
  else el("progressText").textContent = `Processing ${done}/${total}`;
}

function addActivityRow(row) {
  activityRows.unshift(row);
  if (activityRows.length > 800) activityRows = activityRows.slice(0, 800);
  renderActivityTable();
}

function statusPill(status) {
  if (status === "sent") {
    return `<span class="statusPill pillSent"><span class="pillDot"></span>Sent</span>`;
  }
  if (status === "sending") {
    return `<span class="statusPill pillSending"><span class="pillDot"></span>Sending</span>`;
  }
  if (status === "skipped") {
    return `<span class="statusPill pillSkipped"><span class="pillDot"></span>Skipped</span>`;
  }
  return `<span class="statusPill pillFailed"><span class="pillDot"></span>Failed</span>`;
}

function renderActivityTable() {
  const body = el("activityBody");
  body.innerHTML = "";

  for (const r of activityRows) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${escapeHtml(r.ts || "")}</td>
      <td>${escapeHtml(r.phone || "")}</td>
      <td>${statusPill(r.status)}</td>
      <td>${escapeHtml(r.error || "")}</td>
    `;
    body.appendChild(tr);
  }
}

function renderRecipientsTable() {
  const t = getSelectedTemplate();
  const vars = getTemplateVariables(t);

  const head = el("recipientHead");
  const body = el("recipientBody");

  ensureRecipientRows(1);

  head.innerHTML = "";
  const hr = document.createElement("tr");
  const hNo = document.createElement("th");
  hNo.style.width = "56px";
  hNo.textContent = "#";
  hr.appendChild(hNo);

  const hPhone = document.createElement("th");
  hPhone.style.width = "180px";
  hPhone.textContent = "Phone";
  hr.appendChild(hPhone);

  for (const key of vars) {
    const th = document.createElement("th");
    th.textContent = key;
    hr.appendChild(th);
  }

  const hAct = document.createElement("th");
  hAct.style.width = "90px";
  hAct.textContent = "Action";
  hr.appendChild(hAct);
  head.appendChild(hr);

  body.innerHTML = "";
  recipientsData.forEach((row, rowIndex) => {
    const tr = document.createElement("tr");

    const tdNo = document.createElement("td");
    tdNo.textContent = String(rowIndex + 1);
    tr.appendChild(tdNo);

    const tdPhone = document.createElement("td");
    const phoneInput = document.createElement("input");
    phoneInput.className = "input";
    phoneInput.placeholder = "60123456789";
    phoneInput.value = row.phone || "";
    phoneInput.addEventListener("input", () => {
      row.phone = phoneInput.value;
      refreshRecipientCount();
      refreshSendPreview();
    });
    tdPhone.appendChild(phoneInput);
    tr.appendChild(tdPhone);

    for (const key of vars) {
      const td = document.createElement("td");
      const input = document.createElement("input");
      input.className = "input";
      input.placeholder = key;
      input.value = String(row.vars?.[key] || "");
      input.addEventListener("input", () => {
        if (!row.vars || typeof row.vars !== "object") row.vars = {};
        row.vars[key] = input.value;
        refreshSendPreview();
      });
      td.appendChild(input);
      tr.appendChild(td);
    }

    const tdAct = document.createElement("td");
    const delBtn = document.createElement("button");
    delBtn.className = "btnDanger";
    delBtn.type = "button";
    delBtn.textContent = "Delete";
    delBtn.disabled = recipientsData.length <= 1;
    delBtn.addEventListener("click", () => {
      if (recipientsData.length <= 1) return;
      recipientsData.splice(rowIndex, 1);
      renderRecipientsTable();
      refreshRecipientCount();
      refreshSendPreview();
    });
    tdAct.appendChild(delBtn);
    tr.appendChild(tdAct);

    body.appendChild(tr);
  });
}

/* --------------------------- Profiles UI ---------------------------- */
function renderProfileSelect() {
  const sel = el("profileSelect");
  sel.innerHTML = "";

  for (const p of profiles) {
    const opt = document.createElement("option");
    opt.value = p.id;
    opt.textContent = p.name;
    sel.appendChild(opt);
  }

  if (activeProfileId) sel.value = activeProfileId;
  const active = profiles.find((x) => x.id === activeProfileId);
  el("waProfileText").textContent = active ? `${active.name} (${active.id})` : "-";
}

function renderProfileList() {
  const list = el("profileList");
  list.innerHTML = "";

  for (const p of profiles) {
    const waLine = p.waMsisdn ? ` - ${escapeHtml(p.waMsisdn)}` : "";
    const item = document.createElement("div");
    item.className = "profileItem";
    item.innerHTML = `
      <div class="profileItemLeft">
        <div class="profileName">${escapeHtml(p.name)}</div>
        <div class="profileId">${escapeHtml(p.id)}${p.id === activeProfileId ? " (active)" : ""}${waLine}</div>
      </div>
      <div class="profileBtns">
        <button class="btnGhost" data-act="use" data-id="${escapeHtml(p.id)}">Use</button>
        <button class="btnGhost" data-act="rename" data-id="${escapeHtml(p.id)}">Rename</button>
        <button class="btnGhost" data-act="terminate" data-id="${escapeHtml(p.id)}">Terminate</button>
        <button class="btnGhost" data-act="del" data-id="${escapeHtml(p.id)}">Delete</button>
      </div>
    `;

    item.querySelectorAll("button").forEach((btn) => {
      btn.addEventListener("click", async () => {
        const act = btn.getAttribute("data-act");
        const id = btn.getAttribute("data-id");

        if (act === "use") {
          setConnectionBadge(false, "Connecting...");
          el("waStatus").textContent = "Connecting...";
          const res = await window.api.setActiveProfile(id);
          activeProfileId = res.activeProfileId;
          renderProfileSelect();
          renderProfileList();
          await waitForConnectionSync();
          toast("Profile selected", "Switched profile and attempting reconnect");
        }

        if (act === "rename") {
          const curr = profiles.find((x) => x.id === id);
          const nextName = window.prompt("New profile name", curr?.name || "");
          if (nextName === null) return;
          try {
            await window.api.renameProfile(id, nextName);
            const res2 = await window.api.getProfiles();
            profiles = res2.profiles || [];
            activeProfileId = res2.activeProfileId || activeProfileId;
            renderProfileSelect();
            renderProfileList();
            toast("Profile renamed", "Profile name updated");
          } catch (e) {
            toast("Cannot rename", String(e?.message || e));
          }
        }


        if (act === "terminate") {
          const curr = profiles.find((x) => x.id === id);
          const ok = window.confirm(
            `Terminate WhatsApp session for "${curr?.name || id}"?\nYou will need to handshake again for this profile.`
          );
          if (!ok) return;
          try {
            await window.api.terminateProfileSession(id);
            const res2 = await window.api.getProfiles();
            profiles = res2.profiles || [];
            activeProfileId = res2.activeProfileId || activeProfileId;
            renderProfileSelect();
            renderProfileList();
            if (id === activeProfileId) {
              setConnectionBadge(false, "Not connected");
              el("waStatus").textContent = "Session terminated. Handshake required.";
            }
            toast("Session terminated", "Handshake is required to reconnect this profile");
          } catch (e) {
            toast("Cannot terminate", String(e?.message || e));
          }
        }
        if (act === "del") {
          try {
            await window.api.deleteProfile(id);
            const res2 = await window.api.getProfiles();
            profiles = res2.profiles || [];
            activeProfileId = res2.activeProfileId || null;
            renderProfileSelect();
            renderProfileList();
            toast("Profile deleted", "Profile removed");
          } catch (e) {
            toast("Cannot delete", String(e?.message || e));
          }
        }
      });
    });

    list.appendChild(item);
  }
}

/* --------------------------- CSV ---------------------------- */
function openCsvModal() {
  renderCsvVariableMapping();
  el("csvModal").classList.remove("hidden");
}

function closeCsvModal() {
  el("csvModal").classList.add("hidden");
}

function renderCsvVariableMapping() {
  const wrap = el("csvVarCols");
  wrap.innerHTML = "";

  const vars = getTemplateVariables();
  if (vars.length === 0) {
    const block = document.createElement("div");
    block.innerHTML = `<label class="label">No template variables selected</label>`;
    wrap.appendChild(block);
    return;
  }

  for (const key of vars) {
    const block = document.createElement("div");

    const label = document.createElement("label");
    label.className = "label";
    label.textContent = `${key} col index`;

    const input = document.createElement("input");
    input.className = "input";
    input.type = "number";
    input.placeholder = "Optional";
    input.setAttribute("data-var-col", key);

    block.appendChild(label);
    block.appendChild(input);
    wrap.appendChild(block);
  }
}

function readCsvMappingFromModal() {
  const hasHeader = el("csvHasHeader").value === "true";
  const phoneCol = Number(el("csvPhoneCol").value || 0);

  const varCols = {};
  document.querySelectorAll("[data-var-col]").forEach((node) => {
    const key = node.getAttribute("data-var-col");
    const valueRaw = String(node.value || "").trim();
    if (!key || !valueRaw) return;
    const idx = Number(valueRaw);
    if (!Number.isFinite(idx)) return;
    varCols[key] = idx;
  });

  return { hasHeader, phoneCol, varCols };
}

function mergeImportedRecipients(recipients, varsByPhone) {
  const out = [];
  for (const msisdn of Array.isArray(recipients) ? recipients : []) {
    const key = String(msisdn || "").trim();
    if (!key) continue;
    out.push(
      makeRecipientRow({
        phone: key,
        vars: varsByPhone && typeof varsByPhone === "object" ? varsByPhone[key] || {} : {}
      })
    );
  }

  recipientsData = out.length > 0 ? out : [makeRecipientRow()];
}

function isRecipientRowEmpty(row) {
  if (!row || typeof row !== "object") return true;
  const phone = String(row.phone || "").trim();
  if (phone) return false;
  const vars = row.vars && typeof row.vars === "object" ? row.vars : {};
  return Object.values(vars).every((v) => String(v || "").trim() === "");
}

function mergeWaContactsIntoRecipients(contacts) {
  const list = Array.isArray(contacts) ? contacts : [];
  const templateVars = getTemplateVariables();
  const includeName = templateVars.includes("name");

  // If table is still in initial blank state, replace it instead of appending.
  if (recipientsData.length === 1 && isRecipientRowEmpty(recipientsData[0])) {
    recipientsData = [];
  }

  const seen = new Set();
  for (const row of recipientsData) {
    const n = normalizeMsisdn(row.phone || "");
    if (n.length >= 8) seen.add(n);
  }

  let added = 0;
  for (const c of list) {
    const msisdn = normalizeMsisdn(c?.msisdn || "");
    if (msisdn.length < 8) continue;
    if (seen.has(msisdn)) continue;

    const row = makeRecipientRow({ phone: msisdn, vars: {} });
    if (includeName) {
      const name = String(c?.name || c?.notify || c?.verifiedName || "").trim();
      if (name) row.vars.name = name;
    }

    recipientsData.push(row);
    seen.add(msisdn);
    added++;
  }

  if (recipientsData.length === 0) recipientsData = [makeRecipientRow()];
  return added;
}

function normalizeWaContactRecord(c) {
  const msisdn = normalizeMsisdn(c?.msisdn || c?.phone || "");
  const jid = String(c?.jid || (msisdn ? `${msisdn}@s.whatsapp.net` : "")).trim();
  const name = String(c?.name || "").trim();
  const notify = String(c?.notify || "").trim();
  const verifiedName = String(c?.verifiedName || "").trim();
  const displayName = name || notify || verifiedName || msisdn || "Unknown";
  const imgUrl = String(c?.imgUrl || "").trim();
  const status = String(c?.status || "").trim();
  return { msisdn, jid, name, notify, verifiedName, displayName, imgUrl, status };
}

function openWaContactsModal() {
  el("waContactsModal").classList.remove("hidden");
}

function closeWaContactsModal() {
  el("waContactsModal").classList.add("hidden");
}

function filterWaContactsRows() {
  const q = String(el("waContactsSearch").value || "")
    .trim()
    .toLowerCase();
  if (!q) {
    waContactsFiltered = [...waContactsRows];
    return;
  }

  waContactsFiltered = waContactsRows.filter((c) => {
    const hay = `${c.displayName} ${c.name} ${c.notify} ${c.verifiedName} ${c.msisdn} ${c.jid} ${c.status}`.toLowerCase();
    return hay.includes(q);
  });
}

function renderWaContactsTable() {
  const body = el("waContactsBody");
  body.innerHTML = "";

  filterWaContactsRows();

  for (const c of waContactsFiltered) {
    const tr = document.createElement("tr");

    const tdPick = document.createElement("td");
    const cb = document.createElement("input");
    cb.type = "checkbox";
    cb.checked = waContactSelection.has(c.msisdn);
    cb.addEventListener("change", () => {
      if (cb.checked) waContactSelection.add(c.msisdn);
      else waContactSelection.delete(c.msisdn);
      updateWaContactsSelectionUi();
    });
    tdPick.appendChild(cb);
    tr.appendChild(tdPick);

    const tdContact = document.createElement("td");
    tdContact.innerHTML = `
      <div class="waContactCell">
        <div class="waAvatar">${c.imgUrl ? `<img src="${escapeHtml(c.imgUrl)}" alt="avatar" />` : escapeHtml((c.displayName || "?").slice(0, 1).toUpperCase())}</div>
        <div class="waContactText">
          <div class="waContactName">${escapeHtml(c.displayName)}</div>
          <div class="waContactMeta">${escapeHtml(c.status || c.verifiedName || c.notify || c.name || "")}</div>
        </div>
      </div>
    `;
    tr.appendChild(tdContact);

    const tdName = document.createElement("td");
    tdName.innerHTML = `<span class="waNameText">${escapeHtml(c.name || c.notify || c.verifiedName || "-")}</span>`;
    tr.appendChild(tdName);

    const tdPhone = document.createElement("td");
    tdPhone.textContent = c.msisdn || "-";
    tr.appendChild(tdPhone);

    const tdJid = document.createElement("td");
    tdJid.innerHTML = `<span class="waJidText">${escapeHtml(c.jid || "-")}</span>`;
    tr.appendChild(tdJid);

    body.appendChild(tr);
  }

  if (waContactsFiltered.length === 0) {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td colspan="5" class="smallMuted">No contacts found</td>`;
    body.appendChild(tr);
  }

  updateWaContactsSelectionUi();
}

function updateWaContactsSelectionUi() {
  const visible = waContactsFiltered.length;
  let selectedVisible = 0;
  for (const c of waContactsFiltered) {
    if (waContactSelection.has(c.msisdn)) selectedVisible++;
  }

  const selectAll = el("waContactsSelectAll");
  selectAll.checked = visible > 0 && selectedVisible === visible;
  selectAll.indeterminate = selectedVisible > 0 && selectedVisible < visible;

  el("waContactsCount").textContent = `${waContactSelection.size} selected (${waContactsRows.length} total)`;
}

async function loadWaContactsForPicker(options) {
  const opts = options && typeof options === "object" ? options : {};
  const forcePhotoRefresh = !!opts.forcePhotoRefresh;
  const loadToken = ++waContactsLoadToken;
  const body = el("waContactsBody");
  body.innerHTML = `<tr><td colspan="5" class="smallMuted">Loading contacts...</td></tr>`;
  try {
    // First pass: fast list from local cache/memory, no photo fetch blocking.
    const res = await window.api.waGetContacts({
      includePhotos: false
    });
    if (loadToken !== waContactsLoadToken) return;
    if (!res?.ok) throw new Error(res?.error || "Unable to load contacts");
    if (typeof res.connected === "boolean") {
      setConnectionBadge(res.connected, res.connected ? "Connected" : "Not connected");
    }

    waContactsRows = (res.contacts || []).map(normalizeWaContactRecord).filter((c) => c.msisdn);
    waContactsRows.sort((a, b) => a.displayName.localeCompare(b.displayName));
    waContactSelection = new Set();
    el("waContactsSearch").value = "";
    renderWaContactsTable();

    if (waContactsRows.length === 0) {
      toast("Contacts", "No contacts available yet. Open chats or reconnect to sync contacts.");
    }

    // Second pass: enrich photos in background; table updates when ready.
    if (res.connected) {
      window.api
        .waGetContacts({
          includePhotos: true,
          maxPhotoFetch: 60,
          photoFetchConcurrency: 3,
          minMinutesBetweenPhotoChecks: forcePhotoRefresh ? 1 : 720
        })
        .then((photoRes) => {
          if (loadToken !== waContactsLoadToken) return;
          if (!photoRes?.ok) return;
          waContactsRows = (photoRes.contacts || []).map(normalizeWaContactRecord).filter((c) => c.msisdn);
          waContactsRows.sort((a, b) => a.displayName.localeCompare(b.displayName));
          renderWaContactsTable();
        })
        .catch(() => {});
    }
  } catch (e) {
    if (loadToken !== waContactsLoadToken) return;
    body.innerHTML = `<tr><td colspan="5" class="smallMuted">${escapeHtml(String(e?.message || e))}</td></tr>`;
  }
}

function csvEscape(v) {
  const s = String(v ?? "");
  if (s.includes('"') || s.includes(",") || s.includes("\n")) {
    return `"${s.replaceAll('"', '""')}"`;
  }
  return s;
}

function csvSampleValue(key) {
  const k = String(key || "").toLowerCase();
  if (k === "name") return "Ali";
  if (k === "branch") return "Bangi";
  if (k === "date") return "10 Feb";
  if (k === "time") return "3:00 PM";
  if (k === "topic") return "quotation";
  if (k === "doctor") return "Dr. Ashraf";
  return "";
}

function downloadCsvTemplate() {
  const t = getSelectedTemplate();
  if (!t) {
    toast("No template", "Select a template first");
    return;
  }

  const vars = getTemplateVariables(t);
  const header = ["phone", ...vars];
  const sample = ["60123456789", ...vars.map((k) => csvSampleValue(k))];

  const content = [header.map(csvEscape).join(","), sample.map(csvEscape).join(",")].join("\n");
  const blob = new Blob([content], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);

  const safeName = String(t.name || "template")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "") || "template";

  const link = document.createElement("a");
  link.href = url;
  link.download = `${safeName}_recipients_template.csv`;
  document.body.appendChild(link);
  link.click();
  link.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

/* --------------------------- Init ---------------------------- */
async function init() {
  const profileRes = await window.api.getProfiles();
  profiles = profileRes.profiles || [];
  activeProfileId = profileRes.activeProfileId || null;

  templates = normalizeTemplates(await window.api.getTemplates());
  currentTemplateId = templates[0]?.id || null;
  const aiRes = await window.api.getAiRewriteConfig();
  applyAiRewriteConfigToUi(aiRes?.config || defaultAiRewriteConfig);

  recipientsData = [makeRecipientRow()];

  renderProfileSelect();
  renderProfileList();

  refreshTemplateSelect();
  renderTemplateList();
  loadTemplateToEditor();
  renderRecipientsTable();
  setProgress(0, 0);

  document.querySelectorAll(".navItem").forEach((btn) => {
    btn.addEventListener("click", () => switchView(btn.dataset.view));
  });

  el("profileSelect").addEventListener("change", async () => {
    const id = el("profileSelect").value;
    setConnectionBadge(false, "Connecting...");
    el("waStatus").textContent = "Connecting...";
    const res = await window.api.setActiveProfile(id);
    activeProfileId = res.activeProfileId;
    renderProfileSelect();
    renderProfileList();
    await waitForConnectionSync();
    toast("Profile selected", "Switched profile and attempting reconnect");
  });

  function setConnectUiMode() {
    const method = el("connectMethod")?.value || "qr";
    const isPairing = method === "pairing";
    el("pairingPhoneWrap")?.classList.toggle("hidden", !isPairing);
    el("pairingBox")?.classList.toggle("hidden", !isPairing);
    el("qrImg")?.classList.toggle("hidden", isPairing);
  }

  async function doHandshake() {
    const method = el("connectMethod")?.value || "qr";
    const phoneNumber = (el("pairingPhone")?.value || "").trim();

    el("qrImg").src = "";
    el("pairingCode").textContent = "-";

    try {
      await window.api.waHandshake({ method, phoneNumber });
      toast("Handshake", method === "pairing" ? "Requesting pairing code..." : "Waiting for QR...");
    } catch (e) {
      toast("Handshake failed", String(e?.message || e));
    }
  }

  el("connectMethod").addEventListener("change", () => {
    setConnectUiMode();
  });

  el("btnHandshake").addEventListener("click", doHandshake);
  el("btnHandshakeTop").addEventListener("click", async () => {
    await doHandshake();
    switchView("connect");
  });

  setConnectUiMode();

  el("btnCreateProfile").addEventListener("click", async () => {
    const name = el("newProfileName").value.trim();
    if (!name) {
      toast("Missing name", "Please enter a profile name");
      return;
    }
    await window.api.createProfile(name);
    const res = await window.api.getProfiles();
    profiles = res.profiles || [];
    activeProfileId = res.activeProfileId || activeProfileId;
    el("newProfileName").value = "";
    renderProfileSelect();
    renderProfileList();
    toast("Profile created", "Select it and connect");
  });

  el("btnNewTpl").addEventListener("click", () => {
    const id = uuid();
    const t = { id, name: "New template", body: "Hi {name},", variables: ["name"], sendPolicy: "once" };
    templates.push(normalizeTemplate(t, templates.length));
    currentTemplateId = id;
    renderTemplateList();
    loadTemplateToEditor();
    refreshTemplateSelect();
    renderRecipientsTable();
    refreshSendPolicyText();
    refreshSendPreview();
    toast("Template created", "A new template has been added");
  });

  el("btnSaveTpl").addEventListener("click", async () => {
    const t = getSelectedTemplate();
    if (!t) return;

    t.name = el("tplName").value.trim() || "Untitled";
    t.body = el("tplBody").value || "";
    t.sendPolicy = el("tplSendPolicy").value === "multiple" ? "multiple" : "once";
    syncTemplateVarsFromBody(t);

    templates = normalizeTemplates(templates);
    await window.api.saveTemplates(templates);

    renderTemplateList();
    refreshTemplateSelect();
    loadTemplateToEditor();
    renderRecipientsTable();
    refreshSendPolicyText();
    refreshSendPreview();

    toast("Saved", "Templates saved successfully");
  });

  el("btnDeleteTpl").addEventListener("click", async () => {
    if (!currentTemplateId) return;

    templates = templates.filter((x) => x.id !== currentTemplateId);
    currentTemplateId = templates[0]?.id || null;

    await window.api.saveTemplates(templates);

    refreshTemplateSelect();
    renderTemplateList();
    loadTemplateToEditor();
    renderRecipientsTable();
    refreshSendPolicyText();
    refreshSendPreview();

    toast("Deleted", "Template deleted");
  });

  el("btnClearSentMarks").addEventListener("click", async () => {
    const t = getSelectedTemplate();
    if (!t) {
      toast("No template", "Select a template first");
      return;
    }
    await window.api.clearSentForTemplate(t.id);
    toast("Cleared", "Sent marks cleared for this template");
  });

  el("tplName").addEventListener("input", () => {
    const t = getSelectedTemplate();
    if (!t) return;
    t.name = el("tplName").value;
    renderTemplateList();
    refreshTemplateSelect();
  });

  el("tplBody").addEventListener("input", () => {
    const t = getSelectedTemplate();
    if (!t) return;
    t.body = el("tplBody").value || "";
    syncTemplateVarsFromBody(t);
    renderTemplateVariableList();
    refreshTemplatePreview();
    renderRecipientsTable();
    renderCsvVariableMapping();
    refreshSendPreview();
  });

  el("tplSendPolicy").addEventListener("change", () => {
    const t = getSelectedTemplate();
    if (!t) return;
    t.sendPolicy = el("tplSendPolicy").value === "multiple" ? "multiple" : "once";
    refreshSendPolicyText();
  });

  el("btnAddTplVar").addEventListener("click", () => {
    const t = getSelectedTemplate();
    if (!t) return;

    const raw = String(el("tplVarName").value || "").trim();
    if (!isValidTemplateVarName(raw)) {
      toast("Invalid variable", "Use letters/numbers/underscore, and start with a letter");
      return;
    }

    const vars = getTemplateVariables(t);
    if (!vars.includes(raw)) vars.push(raw);
    t.variables = vars;
    el("tplVarName").value = "";
    renderTemplateVariableList();
    renderRecipientsTable();
    renderCsvVariableMapping();
    refreshSendPreview();
  });

  el("tplSelect").addEventListener("change", () => {
    currentTemplateId = el("tplSelect").value;
    renderTemplateList();
    loadTemplateToEditor();
    renderRecipientsTable();
    refreshSendPolicyText();
    renderCsvVariableMapping();
    refreshSendPreview();
  });

  el("btnAddRecipientRow").addEventListener("click", () => {
    recipientsData.push(makeRecipientRow());
    renderRecipientsTable();
    refreshRecipientCount();
    refreshSendPreview();
  });
  el("btnClearRecipients").addEventListener("click", () => {
    recipientsData = [makeRecipientRow()];
    renderRecipientsTable();
    refreshRecipientCount();
    refreshSendPreview();
    toast("Recipients cleared", "Recipient list reset");
  });

  el("btnImportWaContacts").addEventListener("click", async () => {
    openWaContactsModal();
    await loadWaContactsForPicker();
  });

  el("btnCloseWaContactsModal").addEventListener("click", () => closeWaContactsModal());
  el("waContactsModalBackdrop").addEventListener("click", () => closeWaContactsModal());
  el("btnRefreshWaContacts").addEventListener("click", async () => {
    await loadWaContactsForPicker({ forcePhotoRefresh: true });
  });
  el("waContactsSearch").addEventListener("input", () => {
    renderWaContactsTable();
  });
  el("waContactsSelectAll").addEventListener("change", () => {
    const checked = !!el("waContactsSelectAll").checked;
    for (const c of waContactsFiltered) {
      if (checked) waContactSelection.add(c.msisdn);
      else waContactSelection.delete(c.msisdn);
    }
    renderWaContactsTable();
  });
  el("btnImportSelectedWaContacts").addEventListener("click", () => {
    const selected = waContactsRows.filter((c) => waContactSelection.has(c.msisdn));
    if (selected.length === 0) {
      toast("Contacts", "Please tick at least one contact");
      return;
    }

    const added = mergeWaContactsIntoRecipients(selected);
    renderRecipientsTable();
    refreshRecipientCount();
    refreshSendPreview();
    closeWaContactsModal();
    toast("Contacts loaded", `Added ${added} selected contacts`);
  });

  el("btnDownloadCsvTemplate").addEventListener("click", () => {
    downloadCsvTemplate();
  });

  ["aiEnable", "aiEndpoint", "aiAuthToken", "aiPrompt", "aiTimeoutMs", "aiFallback"].forEach((id) => {
    const node = el(id);
    if (!node) return;
    const evName = node.tagName === "INPUT" && node.type === "checkbox" ? "change" : "input";
    node.addEventListener(evName, () => {
      if (id === "aiEnable") setAiRewriteUiEnabled(!!el("aiEnable").checked);
      scheduleSaveAiRewriteConfig();
    });
  });

  el("pacingPattern").addEventListener("change", () => refreshSendPreview());
  el("minSec").addEventListener("input", () => refreshSendPreview());
  el("maxSec").addEventListener("input", () => refreshSendPreview());

  el("btnClearActivity").addEventListener("click", () => {
    activityRows = [];
    renderActivityTable();
    toast("Cleared", "Activity table cleared");
  });

  el("btnImportCsv").addEventListener("click", () => openCsvModal());
  el("btnCloseCsvModal").addEventListener("click", () => closeCsvModal());
  el("csvModalBackdrop").addEventListener("click", () => closeCsvModal());

  el("btnPickCsvAndImport").addEventListener("click", async () => {
    const mapping = readCsvMappingFromModal();
    try {
      const res = await window.api.importCsv(mapping);
      if (!res.ok && res.canceled) {
        toast("Canceled", "CSV import canceled");
        return;
      }
      if (!res.ok) {
        toast("CSV error", res.error || "Failed to import");
        return;
      }

      mergeImportedRecipients(res.recipients || [], res.varsByPhone || {});
      renderRecipientsTable();
      refreshRecipientCount();
      refreshSendPreview();
      closeCsvModal();
      toast("Imported", `Loaded ${res.recipients?.length || 0} recipients`);
    } catch (e) {
      toast("CSV import failed", String(e?.message || e));
    }
  });

  el("btnSend").addEventListener("click", async () => {
    if (!waConnected) {
      toast("Not connected", "Connect WhatsApp first");
      switchView("connect");
      return;
    }

    const t = getSelectedTemplate();
    if (!t) {
      toast("No template", "Please create or select a template first");
      switchView("templates");
      return;
    }

    const payloadData = getSendPayload();
    if (payloadData.recipients.length === 0) {
      toast("No recipients", "Please add at least one valid phone number");
      return;
    }

    const pacing = readPacing();
    const skipAlreadySent = t.sendPolicy !== "multiple";
    const aiRewrite = readAiRewriteConfigFromUi();
    if (aiRewrite.enabled && !aiRewrite.endpoint) {
      toast("AI rewrite", "Please set backend endpoint URL or disable AI rewrite");
      return;
    }
    if (aiRewrite.enabled && !aiRewrite.prompt) {
      toast("AI rewrite", "Please set rewrite prompt or disable AI rewrite");
      return;
    }

    setProgress(0, payloadData.recipients.length);
    toast("Batch started", `Processing ${payloadData.recipients.length} recipients`);
    switchView("activity");

    try {
      const res = await window.api.waSendBatch({
        templateId: t.id,
        templateBody: t.body,
        recipients: payloadData.recipients,
        varsByPhone: payloadData.varsByPhone,
        aiRewrite,
        pacing,
        safety: { maxRecipients: 200 },
        skipAlreadySent
      });

      toast("Batch finished", `Sent: ${res.sent}, Skipped: ${res.skipped}, Failed: ${res.failed}`);
      setProgress(payloadData.recipients.length, payloadData.recipients.length);
    } catch (e) {
      toast("Batch error", String(e?.message || e));
    }
  });

  window.api.onQR((dataUrl) => {
    el("qrImg").src = dataUrl;
  });

  window.api.onPairingCode((code) => {
    el("pairingCode").textContent = String(code || "-");
  });

  window.api.onStatus((s) => {
    if (s?.profileId && activeProfileId && s.profileId !== activeProfileId) return;
    el("waStatus").textContent = s.text || "Status updated";
    const connected = !!s.connected;
    setConnectionBadge(connected, s.text || (connected ? "Connected" : "Not connected"));
    if (connected) {
      window.api
        .getProfiles()
        .then((res) => {
          profiles = res.profiles || [];
          activeProfileId = res.activeProfileId || activeProfileId;
          renderProfileSelect();
          renderProfileList();
        })
        .catch(() => {});
    }

    const active = profiles.find((x) => x.id === activeProfileId);
    el("waProfileText").textContent = active ? `${active.name} (${active.id})` : "-";

    toast("WhatsApp status", s.text || "Updated");
  });

  window.api.onBatchProgress((p) => {
    if (!p) return;

    addActivityRow({
      ts: p.ts || "",
      phone: p.phone || "",
      status: p.status || "failed",
      error: p.error || ""
    });

    if (p.total && p.index) {
      const done =
        p.status === "sent" || p.status === "failed" || p.status === "skipped"
          ? p.index
          : Math.max(0, p.index - 1);
      setProgress(done, p.total);
    }
  });

  setConnectionBadge(false, "Connecting...");
  switchView("connect");
  refreshRecipientCount();
  refreshTemplatePreview();
  refreshSendPolicyText();
  refreshSendPreview();

  await syncConnectionStateFromBackend();
  await waitForConnectionSync();
}

init().catch((e) => {
  console.error(e);
  toast("Init error", String(e?.message || e));
});

