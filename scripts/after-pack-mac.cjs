"use strict";

const fs = require("fs");
const path = require("path");

function ensureExecutable(filePath) {
  try {
    fs.chmodSync(filePath, 0o755);
  } catch {
    // Ignore chmod failures during packaging.
  }
}

exports.default = async function afterPack(context) {
  if (context.electronPlatformName !== "darwin") {
    return;
  }

  const backendDir = path.join(
    context.appOutDir,
    "Contents",
    "Resources",
    "app.asar.unpacked",
    "go-backend"
  );

  if (!fs.existsSync(backendDir)) {
    return;
  }

  const packagedBackend = path.join(backendDir, "go-backend");
  if (!fs.existsSync(packagedBackend)) {
    throw new Error(`Expected packaged backend missing for mac build: ${packagedBackend}`);
  }

  ensureExecutable(packagedBackend);

  const unexpectedFiles = [
    "go-backend.exe",
    "go-backend-mac-arm64",
    "go-backend-mac-amd64",
    "go.mod",
    "go.sum",
    "main.go",
    "WhatsConect Launcher.cmd"
  ];

  for (const fileName of unexpectedFiles) {
    const filePath = path.join(backendDir, fileName);
    if (fs.existsSync(filePath)) {
      fs.rmSync(filePath, { force: true, recursive: true });
    }
  }
};
