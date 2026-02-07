const { contextBridge, ipcRenderer } = require("electron");

contextBridge.exposeInMainWorld("api", {
  getTemplates: () => ipcRenderer.invoke("app:getTemplates"),
  saveTemplates: (templates) => ipcRenderer.invoke("app:saveTemplates", templates),
  getAiRewriteConfig: () => ipcRenderer.invoke("app:getAiRewriteConfig"),
  saveAiRewriteConfig: (config) => ipcRenderer.invoke("app:saveAiRewriteConfig", config),

  getProfiles: () => ipcRenderer.invoke("app:getProfiles"),
  createProfile: (name) => ipcRenderer.invoke("app:createProfile", name),
  deleteProfile: (profileId) => ipcRenderer.invoke("app:deleteProfile", profileId),
  setActiveProfile: (profileId) => ipcRenderer.invoke("app:setActiveProfile", profileId),

  // Start a WhatsApp handshake (QR or pairing code)
  waHandshake: (payload) => ipcRenderer.invoke("wa:handshake", payload),
  waAutoReconnect: () => ipcRenderer.invoke("wa:autoReconnect"),
  waGetContacts: () => ipcRenderer.invoke("wa:getContacts"),
  waSendBatch: (payload) => ipcRenderer.invoke("wa:sendBatch", payload),

  clearSentForTemplate: (templateId) => ipcRenderer.invoke("wa:clearSentForTemplate", templateId),
  importCsv: (mapping) => ipcRenderer.invoke("app:openCsvDialogAndParse", mapping),

  onQR: (cb) => ipcRenderer.on("wa:qr", (_e, dataUrl) => cb(dataUrl)),
  onPairingCode: (cb) => ipcRenderer.on("wa:pairingCode", (_e, code) => cb(code)),
  onStatus: (cb) => ipcRenderer.on("wa:status", (_e, s) => cb(s)),
  onBatchProgress: (cb) => ipcRenderer.on("batch:progress", (_e, p) => cb(p))
});
