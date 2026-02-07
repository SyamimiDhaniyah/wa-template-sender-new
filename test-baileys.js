const makeWASocket = require("@whiskeysockets/baileys").default;
const { useMultiFileAuthState, DisconnectReason } = require("@whiskeysockets/baileys");
const pino = require("pino");

(async () => {
  const { state, saveCreds } = await useMultiFileAuthState("./_test_auth");

  const sock = makeWASocket({
    auth: state,
    logger: pino({ level: "info" }),
    printQRInTerminal: true
  });

  sock.ev.on("creds.update", saveCreds);

  sock.ev.on("connection.update", (u) => {
    console.log("connection.update:", {
      connection: u.connection,
      lastDisconnect: u.lastDisconnect?.error?.output?.statusCode,
      hasQr: !!u.qr
    });

    if (u.connection === "close") {
      const code = u.lastDisconnect?.error?.output?.statusCode;
      if (code === DisconnectReason.loggedOut) console.log("Logged out");
    }
  });
})();
