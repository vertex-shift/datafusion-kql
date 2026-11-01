"use strict";

(() => {
  const key = "datafusion-kql-theme";
  const choices = ["system", "light", "dark"];
  let preference = "system";
  let notice = "";
  document.documentElement.dataset.theme = preference;

  function render() {
    const select = document.getElementById("theme-select");
    const status = document.getElementById("theme-status");
    if (select) select.value = preference;
    if (status) {
      status.textContent = notice;
      status.hidden = !notice;
    }
  }

  function apply(value) {
    preference = value ?? "system";
    notice = "";
    if (!choices.includes(preference)) {
      preference = "system";
      notice = "Saved theme preference is invalid; following your system setting.";
      console.warn(notice);
    }
    document.documentElement.dataset.theme = preference;
    for (const meta of document.querySelectorAll("meta[data-color-scheme]")) {
      meta.media = preference === "system"
        ? `(prefers-color-scheme: ${meta.dataset.colorScheme})`
        : meta.dataset.colorScheme === preference ? "all" : "not all";
    }
    render();
  }

  function storageError(error) {
    if (!(error instanceof DOMException) ||
        !["SecurityError", "QuotaExceededError"].includes(error.name)) {
      throw error;
    }
    notice = "Theme preference cannot be saved. Changes apply to this page only.";
    console.warn(notice, error);
    render();
  }

  try {
    apply(localStorage.getItem(key));
  } catch (error) {
    storageError(error);
  }

  document.addEventListener("DOMContentLoaded", () => {
    const select = document.getElementById("theme-select");
    if (!select || !select.closest(".theme-control")) {
      throw new Error("The color theme selector is missing.");
    }
    select.closest(".theme-control").hidden = false;
    render();
    select.addEventListener("change", () => {
      apply(select.value);
      try {
        if (preference === "system") localStorage.removeItem(key);
        else localStorage.setItem(key, preference);
      } catch (error) {
        storageError(error);
      }
    });
  }, { once: true });

  window.addEventListener("storage", event => {
    if (event.key !== key && event.key !== null) return;
    try {
      if (event.storageArea === localStorage) apply(event.newValue);
    } catch (error) {
      storageError(error);
    }
  });
})();
