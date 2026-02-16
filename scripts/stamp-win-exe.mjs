import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { rcedit } from "rcedit";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const rootDir = path.resolve(__dirname, "..");

const pkgPath = path.join(rootDir, "package.json");
const pkg = JSON.parse(fs.readFileSync(pkgPath, "utf-8"));

const exeName = `${pkg.build?.productName || "WhatsConect"}.exe`;
const exePath = path.join(rootDir, "dist", "win-unpacked", exeName);
const iconPath = path.join(rootDir, "assets", "icon.ico");

if (!fs.existsSync(exePath)) {
  throw new Error(`Executable not found: ${exePath}`);
}
if (!fs.existsSync(iconPath)) {
  throw new Error(`Icon not found: ${iconPath}`);
}

const productName = String(pkg.build?.productName || "WhatsConect");
const appVersion = String(pkg.version || "1.0.0");
const company = String(pkg.author || "").trim();
const description = String(pkg.description || productName);

await rcedit(exePath, {
  icon: iconPath,
  "file-version": appVersion,
  "product-version": appVersion,
  "requested-execution-level": "asInvoker",
  "version-string": {
    ProductName: productName,
    FileDescription: description,
    CompanyName: company,
    InternalName: productName,
    OriginalFilename: exeName
  }
});

const rootExePath = path.join(rootDir, "dist", exeName);
fs.copyFileSync(exePath, rootExePath);

console.log(`Stamped icon/version for: ${exePath}`);
console.log(`Copied stamped exe to: ${rootExePath}`);
