"use strict";

const fs = require("fs");
const path = require("path");
const { Arch } = require("builder-util");

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

  const arm64Backend = path.join(backendDir, "go-backend-mac-arm64");
  const amd64Backend = path.join(backendDir, "go-backend-mac-amd64");
  const packagedBackend = path.join(backendDir, "go-backend");

  const selectedBackend = context.arch === Arch.arm64 ? arm64Backend : amd64Backend;
  const otherBackend = context.arch === Arch.arm64 ? amd64Backend : arm64Backend;

  if (!fs.existsSync(selectedBackend)) {
    throw new Error(`Expected backend binary missing for arch ${context.arch}: ${selectedBackend}`);
  }

  if (fs.existsSync(packagedBackend)) {
    fs.rmSync(packagedBackend, { force: true });
  }

  fs.renameSync(selectedBackend, packagedBackend);
  ensureExecutable(packagedBackend);

  if (fs.existsSync(otherBackend)) {
    fs.rmSync(otherBackend, { force: true });
  }
};
