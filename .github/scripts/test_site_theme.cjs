const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const vm = require("node:vm");

const key = "datafusion-kql-theme";
const source = fs.readFileSync(path.resolve(__dirname, "../../site/assets/theme.js"), "utf8");

function boot({ saved = null, readError = null, writeError = null } = {}) {
  let stored = saved;
  let ready = false;
  const documentEvents = new Map();
  const windowEvents = new Map();
  const selectEvents = new Map();
  const warnings = [];
  const label = { hidden: true };
  const select = {
    value: "system",
    closest: () => label,
    addEventListener: (name, callback) => selectEvents.set(name, callback)
  };
  const status = { hidden: true, textContent: "" };
  const metas = ["light", "dark"].map(scheme => ({
    dataset: { colorScheme: scheme },
    media: `(prefers-color-scheme: ${scheme})`
  }));
  const document = {
    documentElement: { dataset: {} },
    getElementById: id => ready ? { "theme-select": select, "theme-status": status }[id] : null,
    querySelectorAll: () => metas,
    addEventListener: (name, callback) => documentEvents.set(name, callback)
  };
  const storage = {
    getItem: name => {
      assert.equal(name, key);
      if (readError) throw readError;
      return stored;
    },
    setItem: (name, value) => {
      assert.equal(name, key);
      if (writeError) throw writeError;
      stored = value;
    },
    removeItem: name => {
      assert.equal(name, key);
      if (writeError) throw writeError;
      stored = null;
    }
  };
  const window = { addEventListener: (name, callback) => windowEvents.set(name, callback) };
  vm.runInNewContext(source, {
    document, window, localStorage: storage, DOMException,
    console: { warn: (...args) => warnings.push(args) }
  });
  const beforeReady = document.documentElement.dataset.theme;
  ready = true;
  documentEvents.get("DOMContentLoaded")();
  return {
    document, label, select, status, metas, warnings, beforeReady,
    saved: () => stored,
    choose: value => {
      select.value = value;
      selectEvents.get("change")();
    },
    storageEvent: (value, name = key, area = storage) => {
      windowEvents.get("storage")({ newValue: value, key: name, storageArea: area });
    }
  };
}

test("System is the default before DOM readiness without saving an override", () => {
  const page = boot();
  assert.equal(page.beforeReady, "system");
  assert.equal(page.select.value, "system");
  assert.equal(page.label.hidden, false);
  assert.equal(page.saved(), null);
  assert.equal(page.status.hidden, true);
  assert.equal(page.warnings.length, 0);
});

for (const preference of ["light", "dark"]) {
  test(`saved ${preference} applies before DOM readiness and updates browser chrome`, () => {
    const page = boot({ saved: preference });
    assert.equal(page.beforeReady, preference);
    assert.equal(page.select.value, preference);
    for (const meta of page.metas) {
      assert.equal(meta.media, meta.dataset.colorScheme === preference ? "all" : "not all");
    }
  });
}

test("manual overrides persist and System removes them", () => {
  const page = boot();
  page.choose("dark");
  assert.equal(page.document.documentElement.dataset.theme, "dark");
  assert.equal(page.saved(), "dark");
  page.choose("light");
  assert.equal(page.saved(), "light");
  page.choose("system");
  assert.equal(page.document.documentElement.dataset.theme, "system");
  assert.equal(page.saved(), null);
  for (const meta of page.metas) {
    assert.equal(meta.media, `(prefers-color-scheme: ${meta.dataset.colorScheme})`);
  }
});

test("other tabs update selection and clearing storage restores System", () => {
  const page = boot({ saved: "dark" });
  page.storageEvent("light");
  assert.equal(page.select.value, "light");
  assert.equal(page.document.documentElement.dataset.theme, "light");
  page.storageEvent(null, null);
  assert.equal(page.select.value, "system");
});

test("unrelated keys and session storage do not alter the theme", () => {
  const page = boot({ saved: "dark" });
  page.storageEvent("light", "unrelated");
  page.storageEvent("light", key, {});
  assert.equal(page.select.value, "dark");
});

test("invalid stored preferences report the problem and follow System", () => {
  const page = boot({ saved: "sepia" });
  assert.equal(page.beforeReady, "system");
  assert.equal(page.select.value, "system");
  assert.equal(page.status.hidden, false);
  assert.match(page.status.textContent, /invalid/);
  assert.equal(page.warnings.length, 1);
});

test("blocked reads keep system defaults and explain page-only changes", () => {
  const page = boot({ readError: new DOMException("Blocked", "SecurityError") });
  assert.equal(page.beforeReady, "system");
  assert.equal(page.select.value, "system");
  assert.equal(page.status.hidden, false);
  assert.match(page.status.textContent, /Changes apply to this page only/);
});

for (const name of ["SecurityError", "QuotaExceededError"]) {
  test(`${name} on save preserves the current-page override with a visible warning`, () => {
    const page = boot({ saved: "light", writeError: new DOMException("Unavailable", name) });
    page.choose("dark");
    assert.equal(page.document.documentElement.dataset.theme, "dark");
    assert.equal(page.select.value, "dark");
    assert.equal(page.saved(), "light");
    assert.equal(page.status.hidden, false);
    assert.match(page.status.textContent, /cannot be saved/);
    assert.equal(page.warnings.length, 1);
  });
}

test("unexpected storage errors are not swallowed", () => {
  assert.throws(() => boot({ readError: new Error("Unexpected read failure") }), /Unexpected read failure/);
  const page = boot({ writeError: new Error("Unexpected save failure") });
  assert.throws(() => page.choose("dark"), /Unexpected save failure/);
});
