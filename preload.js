const { contextBridge, ipcRenderer, webUtils } = require("electron");

contextBridge.exposeInMainWorld("api", {
  getTemplates: () => ipcRenderer.invoke("app:getTemplates"),
  saveTemplates: (templates) => ipcRenderer.invoke("app:saveTemplates", templates),
  getAppointmentTemplates: () => ipcRenderer.invoke("app:getAppointmentTemplates"),
  saveAppointmentTemplates: (templates) => ipcRenderer.invoke("app:saveAppointmentTemplates", templates),
  getClinicSettings: () => ipcRenderer.invoke("app:getClinicSettings"),
  saveClinicSettings: (settings) => ipcRenderer.invoke("app:saveClinicSettings", settings),
  exportTemplatesBundle: () => ipcRenderer.invoke("app:exportTemplatesBundle"),
  importTemplatesBundle: () => ipcRenderer.invoke("app:importTemplatesBundle"),
  getAiRewriteConfig: () => ipcRenderer.invoke("app:getAiRewriteConfig"),
  saveAiRewriteConfig: (config) => ipcRenderer.invoke("app:saveAiRewriteConfig", config),

  clinicGetSession: () => ipcRenderer.invoke("clinic:getSession"),
  clinicLogin: (payload) => ipcRenderer.invoke("clinic:login", payload),
  clinicLogout: () => ipcRenderer.invoke("clinic:logout"),
  clinicRefreshMe: () => ipcRenderer.invoke("clinic:refreshMe"),
  clinicGetBranchList: () => ipcRenderer.invoke("clinic:getBranchList"),
  clinicGetAppointmentList: (payload) => ipcRenderer.invoke("clinic:getAppointmentList", payload),
  clinicGetPatient: (payload) => ipcRenderer.invoke("clinic:getPatient", payload),
  clinicGetPastPatients: (payload) => ipcRenderer.invoke("clinic:getPastPatients", payload),

  getProfiles: () => ipcRenderer.invoke("app:getProfiles"),
  createProfile: (name) => ipcRenderer.invoke("app:createProfile", name),
  renameProfile: (profileId, name) => ipcRenderer.invoke("app:renameProfile", profileId, name),
  terminateProfileSession: (profileId) => ipcRenderer.invoke("app:terminateProfileSession", profileId),
  deleteProfile: (profileId) => ipcRenderer.invoke("app:deleteProfile", profileId),
  setActiveProfile: (profileId) => ipcRenderer.invoke("app:setActiveProfile", profileId),

  // Start a WhatsApp handshake (QR or pairing code)
  waHandshake: (payload) => ipcRenderer.invoke("wa:handshake", payload),
  waAutoReconnect: () => ipcRenderer.invoke("wa:autoReconnect"),
  waGetConnectionState: () => ipcRenderer.invoke("wa:getConnectionState"),
  waGetContacts: (options) => ipcRenderer.invoke("wa:getContacts", options),
  waGetRecentChats: (options) => ipcRenderer.invoke("wa:getRecentChats", options),
  waGetChatMessages: (payload) => ipcRenderer.invoke("wa:getChatMessages", payload),
  waMarkChatRead: (payload) => ipcRenderer.invoke("wa:markChatRead", payload),
  waSendPresence: (payload) => ipcRenderer.invoke("wa:sendPresence", payload),
  waSendChatMessage: (payload) => ipcRenderer.invoke("wa:sendChatMessage", payload),
  waSetTyping: (payload) => ipcRenderer.invoke("wa:setTyping", payload),
  waPickAttachment: () => ipcRenderer.invoke("wa:pickAttachment"),
  waGetPathForDroppedFile: (file) => {
    try {
      if (!webUtils || typeof webUtils.getPathForFile !== "function") return "";
      return String(webUtils.getPathForFile(file) || "");
    } catch {
      return "";
    }
  },
  waDownloadMedia: (payload) => ipcRenderer.invoke("wa:downloadMedia", payload),
  waResolveImagePreview: (payload) => ipcRenderer.invoke("wa:resolveImagePreview", payload),
  waSendBatch: (payload) => ipcRenderer.invoke("wa:sendBatch", payload),
  waSendPreparedBatch: (payload) => ipcRenderer.invoke("wa:sendPreparedBatch", payload),

  clearSentForTemplate: (templateId) => ipcRenderer.invoke("wa:clearSentForTemplate", templateId),
  importCsv: (mapping) => ipcRenderer.invoke("app:openCsvDialogAndParse", mapping),

  onQR: (cb) => ipcRenderer.on("wa:qr", (_e, dataUrl) => cb(dataUrl)),
  onPairingCode: (cb) => ipcRenderer.on("wa:pairingCode", (_e, code) => cb(code)),
  onStatus: (cb) => ipcRenderer.on("wa:status", (_e, s) => cb(s)),
  onWaChatSync: (cb) => ipcRenderer.on("wa:chatSync", (_e, payload) => cb(payload)),
  onWaPresence: (cb) => ipcRenderer.on("wa:presence", (_e, payload) => cb(payload)),
  onBatchProgress: (cb) => ipcRenderer.on("batch:progress", (_e, p) => cb(p))
});
