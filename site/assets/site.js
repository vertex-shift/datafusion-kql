"use strict";

(() => {
  const examples = {
    telemetry: {
      query: "datatable(Service:string, Duration:long)\n  ['api',120,'api',260,'worker',90]\n| where Duration > 100\n| summarize Requests=count(),\n    P95=percentile(Duration,95) by Service",
      columns: ["Service", "Requests", "P95"],
      rows: [["api", "2", "260"]],
      caption: "Synthetic telemetry query result",
      note: "Small groups retain exact nearest ranks; large percentile states use bounded t-digest sketches.",
      link: "https://github.com/vertex-shift/datafusion-kql/blob/main/docs/compatibility.md#bounded-percentiles-and-distribution-sketches",
      label: "Read the percentile contract"
    },
    dynamic: {
      query: "print Samples=dynamic([1,5,3])\n| mv-apply Value=Samples to typeof(long)\n    on (top 2 by Value\n        | summarize Total=sum(Value))\n| project Total",
      columns: ["Total"],
      rows: [["8"]],
      caption: "Synthetic dynamic array query result",
      note: "Per-row array transformations preserve KQL types. Common typed top-k reductions use batch Arrow kernels.",
      link: "https://github.com/vertex-shift/datafusion-kql/blob/main/docs/compatibility.md#grouped-subqueries",
      label: "Read the grouped-query contract"
    },
    series: {
      query: "print Values=dynamic([1,3,5,7,20,19,18,17])\n| extend (RSquare, Split)=\n    series_fit_2lines(Values)\n| project RSquare, Split",
      columns: ["RSquare", "Split"],
      rows: [["1.0", "4"]],
      caption: "Synthetic two-segment trend query result",
      note: "One native struct-valued computation supplies correlated outputs. Segment edge profiles are documented, not fully service-certified.",
      link: "https://github.com/vertex-shift/datafusion-kql/blob/main/docs/compatibility.md#outlier-scores-and-two-segment-trend-fitting",
      label: "Read the trend-fitting contract"
    }
  };

  const query = document.getElementById("example-query");
  const table = document.getElementById("example-result");
  const note = document.getElementById("example-note");
  const reference = document.getElementById("example-reference");
  const view = document.getElementById("example-view");
  const picker = document.querySelector(".example-picker");
  const buttons = [...document.querySelectorAll("[data-example]")];

  if (query && table && note && reference && view && picker && buttons.length) {
    picker.setAttribute("role", "tablist");
    view.setAttribute("role", "tabpanel");
    view.setAttribute("aria-labelledby", buttons[0].id);
    for (const button of buttons) {
      const selected = button.getAttribute("aria-pressed") === "true";
      button.disabled = false;
      button.removeAttribute("aria-pressed");
      button.setAttribute("role", "tab");
      button.setAttribute("aria-controls", view.id);
      button.setAttribute("aria-selected", String(selected));
      button.tabIndex = selected ? 0 : -1;
    }

    function select(button) {
      const example = examples[button.dataset.example];
      if (!example) return;
      query.textContent = example.query;
      note.textContent = example.note;
      reference.href = example.link;
      reference.textContent = example.label;
      table.caption.textContent = example.caption;
      const header = document.createElement("tr");
      for (const column of example.columns) {
        const cell = document.createElement("th");
        cell.scope = "col";
        cell.textContent = column;
        header.append(cell);
      }
      table.tHead.replaceChildren(header);
      const body = document.createDocumentFragment();
      for (const row of example.rows) {
        const line = document.createElement("tr");
        for (const value of row) {
          const cell = document.createElement("td");
          cell.textContent = value;
          line.append(cell);
        }
        body.append(line);
      }
      table.tBodies[0].replaceChildren(body);
      view.setAttribute("aria-labelledby", button.id);
      for (const tab of buttons) {
        const selected = tab === button;
        tab.setAttribute("aria-selected", String(selected));
        tab.tabIndex = selected ? 0 : -1;
      }
      view.classList.remove("changed");
      requestAnimationFrame(() => view.classList.add("changed"));
    }

    buttons.forEach((button, index) => {
      button.addEventListener("click", () => select(button));
      button.addEventListener("keydown", event => {
        let next;
        if (event.key === "ArrowRight") next = (index + 1) % buttons.length;
        if (event.key === "ArrowLeft") next = (index + buttons.length - 1) % buttons.length;
        if (event.key === "Home") next = 0;
        if (event.key === "End") next = buttons.length - 1;
        if (next === undefined) return;
        event.preventDefault();
        select(buttons[next]);
        buttons[next].focus();
      });
    });
  }

  const status = document.getElementById("copy-status");
  for (const button of document.querySelectorAll("[data-copy]")) {
    button.hidden = false;
    const label = button.textContent;
    let timer;
    button.addEventListener("click", async () => {
      const source = document.getElementById(button.dataset.copy);
      if (!source || !status) return;
      button.disabled = true;
      try {
        if (!navigator.clipboard || !window.isSecureContext) {
          throw new Error("Clipboard access is not available in this browser.");
        }
        await navigator.clipboard.writeText(source.textContent);
        button.textContent = "Copied";
        status.textContent = `${label} succeeded.`;
      } catch {
        button.textContent = "Select & copy";
        status.textContent = "Clipboard access is unavailable. The code has been selected; copy it manually.";
        const selection = window.getSelection();
        if (selection) {
          const range = document.createRange();
          range.selectNodeContents(source);
          selection.removeAllRanges();
          selection.addRange(range);
        }
      } finally {
        button.disabled = false;
        clearTimeout(timer);
        timer = setTimeout(() => { button.textContent = label; }, 2500);
      }
    });
  }
})();
