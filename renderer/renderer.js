let templates = [];
let currentTemplateId = null;

let profiles = [];
let activeProfileId = null;
let waConnected = false;

let activityRows = [];
let batchTotal = 0;
let batchDone = 0;

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
    refreshSendPreview();
    refreshRecipientCount();
  }
}

function uuid() {
  return "t_" + Math.random().toString(16).slice(2) + "_" + Date.now().toString(16);
}

function getSelectedTemplate() {
  return templates.find((t) => t.id === currentTemplateId) || null;
}

function templateSnippet(body) {
  const s = String(body || "").replace(/\s+/g, " ").trim();
  return s.length > 70 ? s.slice(0, 70) + "..." : s;
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

function parsePhonesRaw() {
  const raw = el("phones").value || "";
  const arr = raw
    .split("\n")
    .map((s) => s.trim())
    .filter(Boolean);
  return arr;
}

function parsePhonesNormalized() {
  const raw = parsePhonesRaw();
  const out = [];
  for (const p of raw) {
    const n = normalizeMsisdn(p);
    if (n && n.length >= 8) out.push(n);
  }
  return out;
}

function parseVarsJson() {
  const raw = el("varsJson").value.trim();
  if (!raw) return {};
  try {
    const obj = JSON.parse(raw);
    if (obj && typeof obj === "object") return obj;
    return {};
  } catch (e) {
    throw new Error("Invalid JSON in Variables field");
  }
}

function readPacing() {
  const pattern = el("pacingPattern").value;
  const minSec = Number(el("minSec").value || 7);
  const maxSec = Number(el("maxSec").value || 10);
  return { pattern, minSec, maxSec };
}

function refreshRecipientCount() {
  const count = parsePhonesNormalized().length;
  el("totalRecipients").textContent = String(count);
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
    if (!currentTemplateId) currentTemplateId = templates[0].id;
    sel.value = currentTemplateId;
  }
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
      refreshSendPreview();
    });

    list.appendChild(item);
  }
}

function loadTemplateToEditor() {
  const t = getSelectedTemplate();
  if (!t) {
    el("tplName").value = "";
    el("tplBody").value = "";
    el("tplPreview").textContent = "Select a template to preview";
    return;
  }

  el("tplName").value = t.name || "";
  el("tplBody").value = t.body || "";
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

  const preview = renderTemplate(el("tplBody").value, demoVars);
  el("tplPreview").textContent = preview || "Preview will appear here";
}

function refreshSendPreview() {
  const t = getSelectedTemplate();
  if (!t) {
    el("sendPreview").textContent = "No template selected";
    return;
  }

  const recipients = parsePhonesRaw();
  if (recipients.length === 0) {
    el("sendPreview").textContent = "Enter recipients to preview";
    return;
  }

  let varsByPhone = {};
  try {
    varsByPhone = parseVarsJson();
  } catch (e) {
    el("sendPreview").textContent = e.message;
    return;
  }

  const firstRaw = recipients[0];
  const normalized = normalizeMsisdn(firstRaw);
  const vars = varsByPhone[firstRaw] || varsByPhone[normalized] || { name: "Client" };
  const body = t.body || "";
  const preview = renderTemplate(body, vars);
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
    const item = document.createElement("div");
    item.className = "profileItem";
    item.innerHTML = `
      <div class="profileItemLeft">
        <div class="profileName">${escapeHtml(p.name)}</div>
        <div class="profileId">${escapeHtml(p.id)}${p.id === activeProfileId ? " (active)" : ""}</div>
      </div>
      <div class="profileBtns">
        <button class="btnGhost" data-act="use" data-id="${escapeHtml(p.id)}">Use</button>
        <button class="btnGhost" data-act="del" data-id="${escapeHtml(p.id)}">Delete</button>
      </div>
    `;

    item.querySelectorAll("button").forEach((btn) => {
      btn.addEventListener("click", async () => {
        const act = btn.getAttribute("data-act");
        const id = btn.getAttribute("data-id");

        if (act === "use") {
          const res = await window.api.setActiveProfile(id);
          activeProfileId = res.activeProfileId;
          renderProfileSelect();
          renderProfileList();
          setConnectionBadge(false, "Not connected");
          toast("Profile selected", "Click Connect and scan QR if needed");
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

/* --------------------------- CSV Modal ---------------------------- */
function openCsvModal() {
  el("csvModal").classList.remove("hidden");
}

function closeCsvModal() {
  el("csvModal").classList.add("hidden");
}

function readCsvMappingFromModal() {
  const hasHeader = el("csvHasHeader").value === "true";
  const phoneCol = Number(el("csvPhoneCol").value || 0);

  const readOpt = (id) => {
    const v = el(id).value;
    if (v === "" || v === null || v === undefined) return null;
    const n = Number(v);
    if (!Number.isFinite(n)) return null;
    return n;
  };

  const varCols = {};
  const nameCol = readOpt("csvNameCol");
  const topicCol = readOpt("csvTopicCol");
  const dateCol = readOpt("csvDateCol");
  const timeCol = readOpt("csvTimeCol");
  const branchCol = readOpt("csvBranchCol");
  const doctorCol = readOpt("csvDoctorCol");

  if (nameCol !== null) varCols.name = nameCol;
  if (topicCol !== null) varCols.topic = topicCol;
  if (dateCol !== null) varCols.date = dateCol;
  if (timeCol !== null) varCols.time = timeCol;
  if (branchCol !== null) varCols.branch = branchCol;
  if (doctorCol !== null) varCols.doctor = doctorCol;

  return { hasHeader, phoneCol, varCols };
}

function mergeVarsJson(existingJsonText, varsByPhoneNew) {
  let base = {};
  try {
    base = existingJsonText.trim() ? JSON.parse(existingJsonText) : {};
  } catch (e) {
    base = {};
  }

  const merged = { ...base };
  for (const k of Object.keys(varsByPhoneNew || {})) {
    merged[k] = { ...(merged[k] || {}), ...(varsByPhoneNew[k] || {}) };
  }
  return JSON.stringify(merged, null, 2);
}

/* --------------------------- Init ---------------------------- */
async function init() {
  const profileRes = await window.api.getProfiles();
  profiles = profileRes.profiles || [];
  activeProfileId = profileRes.activeProfileId || null;

  templates = await window.api.getTemplates();
  if (!Array.isArray(templates)) templates = [];
  currentTemplateId = templates[0]?.id || null;

  renderProfileSelect();
  renderProfileList();

  refreshTemplateSelect();
  renderTemplateList();
  loadTemplateToEditor();
  setProgress(0, 0);

  document.querySelectorAll(".navItem").forEach((btn) => {
    btn.addEventListener("click", () => switchView(btn.dataset.view));
  });

  el("profileSelect").addEventListener("change", async () => {
    const id = el("profileSelect").value;
    const res = await window.api.setActiveProfile(id);
    activeProfileId = res.activeProfileId;
    renderProfileSelect();
    renderProfileList();
    setConnectionBadge(false, "Not connected");
    toast("Profile selected", "Click Connect and scan QR if needed");
  });

  function setConnectUiMode() {
    const method = el("connectMethod")?.value || "qr";
    const isPairing = method === "pairing";
    el("pairingPhoneWrap")?.classList.toggle("hidden", !isPairing);
    // QR image vs pairing code overlay
    el("pairingBox")?.classList.toggle("hidden", !isPairing);
    el("qrImg")?.classList.toggle("hidden", isPairing);
  }

  async function doHandshake() {
    const method = el("connectMethod")?.value || "qr";
    const phoneNumber = (el("pairingPhone")?.value || "").trim();

    // reset UI placeholders
    el("qrImg").src = "";
    el("pairingCode").textContent = "-";

    try {
      await window.api.waHandshake({ method, phoneNumber });
      toast("Handshake", method === "pairing" ? "Requesting pairing code..." : "Waiting for QR..."
      );
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
    toast("Profile created", "Select it and click Connect");
  });

  el("btnNewTpl").addEventListener("click", () => {
    const id = uuid();
    const t = { id, name: "New template", body: "Hi {name}," };
    templates.push(t);
    currentTemplateId = id;
    renderTemplateList();
    loadTemplateToEditor();
    refreshTemplateSelect();
    toast("Template created", "A new template has been added");
  });

  el("btnSaveTpl").addEventListener("click", async () => {
    const t = getSelectedTemplate();
    if (!t) return;

    t.name = el("tplName").value.trim() || "Untitled";
    t.body = el("tplBody").value || "";

    await window.api.saveTemplates(templates);

    renderTemplateList();
    refreshTemplateSelect();
    refreshTemplatePreview();
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
    refreshTemplatePreview();
    refreshSendPreview();
  });

  el("tplSelect").addEventListener("change", () => {
    currentTemplateId = el("tplSelect").value;
    renderTemplateList();
    loadTemplateToEditor();
    refreshSendPreview();
  });

  el("phones").addEventListener("input", () => {
    refreshRecipientCount();
    refreshSendPreview();
  });

  el("varsJson").addEventListener("input", () => {
    refreshSendPreview();
  });

  el("pacingPattern").addEventListener("change", () => refreshSendPreview());
  el("minSec").addEventListener("input", () => refreshSendPreview());
  el("maxSec").addEventListener("input", () => refreshSendPreview());

  el("btnClearActivity").addEventListener("click", () => {
    activityRows = [];
    renderActivityTable();
    toast("Cleared", "Activity table cleared");
  });

  // CSV modal controls
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

      // Fill recipients (normalized from backend)
      const recipients = res.recipients || [];
      el("phones").value = recipients.join("\n");

      // Merge vars
      const varsNew = res.varsByPhone || {};
      el("varsJson").value = mergeVarsJson(el("varsJson").value || "", varsNew);

      refreshRecipientCount();
      refreshSendPreview();
      closeCsvModal();
      toast("Imported", `Loaded ${recipients.length} recipients`);
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

    const recipients = parsePhonesNormalized();
    if (recipients.length === 0) {
      toast("No recipients", "Please enter at least one phone number");
      return;
    }

    let varsByPhone = {};
    try {
      varsByPhone = parseVarsJson();
    } catch (e) {
      toast("Invalid JSON", e.message);
      return;
    }

    const pacing = readPacing();
    const skipAlreadySent = el("skipSent").checked;

    setProgress(0, recipients.length);
    toast("Batch started", `Processing ${recipients.length} recipients`);
    switchView("activity");

    try {
      const res = await window.api.waSendBatch({
        templateId: t.id,
        templateBody: t.body,
        recipients,
        varsByPhone,
        pacing,
        safety: { maxRecipients: 200 },
        skipAlreadySent
      });

      toast("Batch finished", `Sent: ${res.sent}, Skipped: ${res.skipped}, Failed: ${res.failed}`);
      setProgress(recipients.length, recipients.length);
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
    el("waStatus").textContent = s.text || "Status updated";
    const connected = !!s.connected;
    setConnectionBadge(connected, s.text || (connected ? "Connected" : "Not connected"));

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

  setConnectionBadge(false, "Not connected");
  switchView("connect");
  refreshRecipientCount();
  refreshTemplatePreview();
  refreshSendPreview();
}

init().catch((e) => {
  console.error(e);
  toast("Init error", String(e?.message || e));
});
