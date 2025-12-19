const el = (id) => document.getElementById(id);

const state = {
  tab: "run",
  filter: ""
};

function fmtUtc(d) {
  // ISO but short-ish
  const s = new Date(d).toISOString();
  return s.replace("T", " ").replace("Z", "Z").slice(0, 19) + "Z";
}

function safeText(x) {
  return (x ?? "").toString();
}

function setHealth(ok) {
  const chip = el("healthChip");
  if (ok) {
    chip.className = "chip chip-good";
    chip.textContent = "API healthy";
  } else {
    chip.className = "chip chip-bad";
    chip.textContent = "API down";
  }
}

function applyTabs() {
  document.querySelectorAll(".tab").forEach(b => {
    b.classList.toggle("active", b.dataset.tab === state.tab);
  });
  document.querySelectorAll(".tabbody").forEach(p => p.classList.remove("active"));
  el(`tab-${state.tab}`).classList.add("active");

  // Re-apply filter whenever tab changes
  applyFilter();
}

function applyFilter() {
  const q = state.filter.trim().toLowerCase();
  const tableId = state.tab === "run" ? "runTable" :
                  state.tab === "samples" ? "samplesTable" : "alertsTable";
  const tbody = el(tableId).querySelector("tbody");

  Array.from(tbody.querySelectorAll("tr")).forEach(tr => {
    const txt = tr.textContent.toLowerCase();
    tr.style.display = (q === "" || txt.includes(q)) ? "" : "none";
  });
}

function rowRun(r) {
  return `
<tr>
  <td>${fmtUtc(r.tsUtc)}</td>
  <td>${safeText(r.name)}</td>
  <td>${safeText(r.feedId)}</td>
  <td class="right">${r.listeners}</td>
  <td><a class="link" href="${r.url}" target="_blank" rel="noreferrer">open</a></td>
</tr>`;
}

function rowSample(r) {
  return `
<tr>
  <td>${fmtUtc(r.tsUtc)}</td>
  <td>${safeText(r.name)}</td>
  <td>${safeText(r.feedId)}</td>
  <td class="right">${r.listeners}</td>
  <td><a class="link" href="${r.url}" target="_blank" rel="noreferrer">open</a></td>
</tr>`;
}

function rowAlert(r) {
  const badgeClass = r.alertType === "spike" ? "badge badge-spike"
                   : r.alertType === "feed_seen" ? "badge badge-new"
                   : "badge";
  return `
<tr>
  <td>${fmtUtc(r.tsUtc)}</td>
  <td>${safeText(r.name)}</td>
  <td>${safeText(r.feedId)}</td>
  <td><span class="${badgeClass}">${safeText(r.alertType)}</span></td>
  <td>${safeText(r.message)}</td>
  <td><a class="link" href="${r.url}" target="_blank" rel="noreferrer">open</a></td>
</tr>`;
}

async function refreshAll() {
  // health
  try {
    const h = await fetch("/api/health");
    setHealth(h.ok);
  } catch {
    setHealth(false);
  }

  // latest run
  try {
    const res = await fetch("/api/latest-run?limit=500");
    const data = await res.json();
    const run = data.run;
    const records = data.records ?? [];

    el("latestRunMetric").textContent = run ? `${records.length} rows` : "—";
    el("latestRunHint").textContent = run
      ? `Run ${run.runId} • started ${fmtUtc(run.startedAtUtc)}`
      : "No ingest run recorded (yet).";

    const tbody = el("runTable").querySelector("tbody");
    tbody.innerHTML = records.map(rowRun).join("");

    el("runEmpty").hidden = !!run;
  } catch (e) {
    el("latestRunMetric").textContent = "—";
    el("latestRunHint").textContent = "Failed to load latest run.";
  }

  // samples
  try {
    const res = await fetch("/api/samples?limit=750");
    const data = await res.json();
    const rows = data.rows ?? [];

    el("samplesMetric").textContent = `${rows.length} rows`;
    const tbody = el("samplesTable").querySelector("tbody");
    tbody.innerHTML = rows.map(rowSample).join("");
  } catch {
    el("samplesMetric").textContent = "—";
  }

  // alerts
  try {
    const res = await fetch("/api/alerts?limit=750");
    const data = await res.json();
    const rows = data.rows ?? [];

    el("alertsMetric").textContent = `${rows.length} rows`;
    const tbody = el("alertsTable").querySelector("tbody");
    tbody.innerHTML = rows.map(rowAlert).join("");
  } catch {
    el("alertsMetric").textContent = "—";
  }

  el("lastRefresh").textContent = `Refreshed ${fmtUtc(new Date().toISOString())}`;

  applyFilter();
}

document.querySelectorAll(".tab").forEach(b => {
  b.addEventListener("click", () => {
    state.tab = b.dataset.tab;
    applyTabs();
  });
});

el("search").addEventListener("input", (e) => {
  state.filter = e.target.value ?? "";
  applyFilter();
});

el("refresh").addEventListener("click", refreshAll);

applyTabs();
refreshAll();
setInterval(refreshAll, 5000);
