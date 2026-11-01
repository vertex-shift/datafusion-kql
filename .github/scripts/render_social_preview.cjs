const assert = require("node:assert/strict");
const { execFileSync } = require("node:child_process");
const fs = require("node:fs");
const path = require("node:path");
const { chromium } = require("playwright");

const root = path.resolve(__dirname, "../..");
const base = process.argv[2] || "http://127.0.0.1:4173/datafusion-kql/";
const source = path.join(root, "site/assets/social-preview.svg");
const target = path.join(root, "site/assets/social-preview.png");
const fonts = [
  "barlow-regular.woff2", "barlow-semibold.woff2", "barlow-condensed-semibold.woff2"
];

async function main() {
  const browser = await chromium.launch({ headless: true });
  try {
    const context = await browser.newContext({
      viewport: { width: 1200, height: 630 }, deviceScaleFactor: 1, colorScheme: "light"
    });
    const page = await context.newPage();
    const checks = [];
    const seen = new Set();
    page.on("response", response => {
      const filename = path.posix.basename(new URL(response.url()).pathname);
      if (fonts.includes(filename)) {
        seen.add(filename);
        checks.push(response.body().then(bytes => {
          assert.deepEqual(bytes, fs.readFileSync(path.join(root, "site/assets/fonts", filename)),
            `The preview server returned stale font bytes: ${filename}`);
        }));
      }
    });
    const response = await page.goto(new URL("assets/social-preview.svg", base).href, {
      waitUntil: "networkidle"
    });
    assert.equal(response.status(), 200);
    assert.deepEqual(await response.body(), fs.readFileSync(source), "The preview server returned stale SVG bytes");
    await page.evaluate(() => document.fonts.ready);
    await Promise.all(checks);
    assert.deepEqual([...seen].sort(), [...fonts].sort());
    const faces = await page.evaluate(() => [...document.fonts].map(font => ({
      family: font.family, weight: font.weight, status: font.status
    })));
    assert.equal(faces.length, 3);
    assert.ok(faces.every(font => font.status === "loaded"), "The licensed local fonts did not load");
    await page.screenshot({ path: target });
    const provenance = `Original social-preview.svg; local SIL OFL Barlow fonts; Chromium ${browser.version()} at 1x`;
    execFileSync("python3", ["-c", `
from pathlib import Path
import struct, sys, zlib
from check_site import SITE, social_fingerprint
path = SITE / "assets/social-preview.png"
image = path.read_bytes()
assert image[-12:] == b"\\x00\\x00\\x00\\x00IEND\\xaeB\\x60\\x82"
assert struct.unpack(">II", image[16:24]) == (1200, 630)
chunks = []
for key, value in (("SourceFingerprint", social_fingerprint(SITE)), ("Source", sys.argv[1])):
    data = key.encode("ascii") + b"\\0" + value.encode("ascii")
    chunk = b"tEXt" + data
    chunks.append(struct.pack(">I", len(data)) + chunk + struct.pack(">I", zlib.crc32(chunk)))
path.write_bytes(image[:-12] + b"".join(chunks) + image[-12:])
print(f"Rendered {path.name}: {path.stat().st_size:,} bytes, 1200x630, with source provenance.")
`, provenance], {
      cwd: __dirname, env: { ...process.env, PYTHONDONTWRITEBYTECODE: "1" }, stdio: "inherit"
    });
    await context.close();
  } finally {
    await browser.close();
  }
}

main().catch(error => { console.error(error); process.exitCode = 1; });
