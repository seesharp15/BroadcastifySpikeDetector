const el = (id) => document.getElementById(id);

const state = {
    tab: "run",
    filter: "",
    selected: null, // { feedId, name, tsUtc, listeners }
    lastSelectedTr: null,
    suppressInspectorRefresh: false,

    // UX: avoid ripping the UI out from under the user while they scroll/click/type
    lastUserActionMs: 0
};

// ------------------ Utilities ------------------

function nowMs() { return Date.now(); }

function fmtUtc(d) {
    const s = new Date(d).toISOString();
    return s.replace("T", " ").replace("Z", "Z").slice(0, 19) + "Z";
}

function safeText(x) {
    return (x ?? "").toString();
}

function setText(id, text) {
    const n = el(id);
    if (!n) return;
    n.textContent = text;
}

function setHidden(id, hidden) {
    const n = el(id);
    if (!n) return;
    n.hidden = hidden;
}

function hasInspector() {
    return !!el("inspectorSubtitle") &&
        !!el("inspHistoryTable") &&
        !!el("inspFeedName") &&
        !!el("inspFeedId");
}

function markUserAction() {
    state.lastUserActionMs = nowMs();
}

function isUserActiveRecently(ms) {
    return (nowMs() - state.lastUserActionMs) < ms;
}

// Capture/restore scroll for any scrollable element
function captureScroll(node) {
    if (!node) return null;
    return { top: node.scrollTop, left: node.scrollLeft };
}

function restoreScroll(node, snap) {
    if (!node || !snap) return;
    node.scrollTop = snap.top;
    node.scrollLeft = snap.left;
}

// Best-effort: find the nearest scroll container for a table.
// Prefer an explicit wrapper if you have it; otherwise fall back to the table parent.
function getScrollContainerForTable(tableId) {
    // If you have wrappers in HTML, use them (recommended):
    // <div id="runScroll" class="table-scroll"><table id="runTable">...
    const explicit =
        tableId === "runTable" ? el("runScroll") :
            tableId === "samplesTable" ? el("samplesScroll") :
                tableId === "alertsTable" ? el("alertsScroll") :
                    null;

    if (explicit) return explicit;

    const table = el(tableId);
    if (!table) return null;

    // Heuristic: if parent scrolls, use it; else use parent anyway.
    return table.parentElement;
}

// Preserve a set of scroll containers while we perform DOM updates.
async function withPreservedScroll(containers, fn) {
    const snaps = containers.map(c => captureScroll(c));
    try {
        // If fn is async, wait for it. If it is sync, await will wrap it.
        await fn();
    } finally {
        // Restore after DOM updates/layout has settled.
        // Two RAFs gives the browser a chance to paint and apply layout after any innerHTML changes.
        await new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve)));
        for (let i = 0; i < containers.length; i++) {
            restoreScroll(containers[i], snaps[i]);
        }
    }
}

// Re-apply selected row highlight after tables get re-rendered
function reapplySelectionHighlight() {
    if (!state.selected?.feedId) return;

    // selection exists in whichever tab table contains that feed (run/samples/alerts)
    const tables = ["runTable", "samplesTable", "alertsTable"];
    let found = null;

    for (const tid of tables) {
        const tbody = el(tid)?.querySelector("tbody");
        if (!tbody) continue;

        // Prefer exact match by feed + ts if available (more stable than just feedId)
        const ts = state.selected.tsUtc ? new Date(state.selected.tsUtc).toISOString() : null;

        let tr = null;
        if (ts) {
            tr = tbody.querySelector(`tr[data-feed-id="${CSS.escape(state.selected.feedId)}"][data-ts="${CSS.escape(ts)}"]`);
        }
        if (!tr) {
            tr = tbody.querySelector(`tr[data-feed-id="${CSS.escape(state.selected.feedId)}"]`);
        }

        if (tr) {
            found = tr;
            break;
        }
    }

    // Clear old
    if (state.lastSelectedTr) state.lastSelectedTr.classList.remove("selected");

    // Apply new
    if (found) {
        found.classList.add("selected");
        state.lastSelectedTr = found;
    } else {
        state.lastSelectedTr = null;
    }
}

// ------------------ Health ------------------

function setHealth(ok) {
    const chip = el("healthChip");
    if (!chip) return;

    if (ok) {
        chip.className = "chip chip-good";
        chip.textContent = "API healthy";
    } else {
        chip.className = "chip chip-bad";
        chip.textContent = "API down";
    }
}

// ------------------ Tabs & Filter ------------------

function applyTabs() {
    document.querySelectorAll(".tab").forEach(b => {
        b.classList.toggle("active", b.dataset.tab === state.tab);
    });
    document.querySelectorAll(".tabbody").forEach(p => p.classList.remove("active"));
    const body = el(`tab-${state.tab}`);
    if (body) body.classList.add("active");
    applyFilter();
}

function applyFilter() {
    const q = state.filter.trim().toLowerCase();
    const tableId =
        state.tab === "run" ? "runTable" :
            state.tab === "samples" ? "samplesTable" :
                "alertsTable";

    const tbody = el(tableId)?.querySelector("tbody");
    if (!tbody) return;

    Array.from(tbody.querySelectorAll("tr")).forEach(tr => {
        const txt = tr.textContent.toLowerCase();
        tr.style.display = (q === "" || txt.includes(q)) ? "" : "none";
    });
}

// ------------------ Row Renderers ------------------

function rowRun(r) {
    return `
<tr data-feed-id="${safeText(r.feedId)}"
    data-feed-name="${safeText(r.name)}"
    data-ts="${safeText(r.tsUtc)}"
    data-listeners="${safeText(r.listeners)}">
  <td>${fmtUtc(r.tsUtc)}</td>
  <td>${safeText(r.name)}</td>
  <td>${safeText(r.feedId)}</td>
  <td>${r.listeners}</td>
  <td>${r.rank ?? ""}</td>
  <td><a class="link" href="${r.url}" target="_blank" rel="noreferrer">open</a></td>
</tr>`;
}

function rowSample(r) {
    return `
<tr data-feed-id="${safeText(r.feedId)}"
    data-feed-name="${safeText(r.name)}"
    data-ts="${safeText(r.tsUtc)}"
    data-listeners="${safeText(r.listeners)}">
  <td>${fmtUtc(r.tsUtc)}</td>
  <td>${safeText(r.name)}</td>
  <td>${safeText(r.feedId)}</td>
  <td>${r.listeners}</td>
  <td>${r.rank ?? ""}</td>
  <td><a class="link" href="${r.url}" target="_blank" rel="noreferrer">open</a></td>
</tr>`;
}

function rowAlert(r) {
    const badgeClass =
        r.alertType === "spike" ? "badge badge-spike" :
            r.alertType === "feed_seen" ? "badge badge-new" :
                "badge";

    return `
<tr data-feed-id="${safeText(r.feedId)}"
    data-feed-name="${safeText(r.name)}"
    data-ts="${safeText(r.tsUtc)}"
    data-listeners="">
  <td>${fmtUtc(r.tsUtc)}</td>
  <td>${safeText(r.name)}</td>
  <td>${safeText(r.feedId)}</td>
  <td><span class="${badgeClass}">${safeText(r.alertType)}</span></td>
  <td>${safeText(r.message)}</td>
  <td><a class="link" href="${r.url}" target="_blank" rel="noreferrer">open</a></td>
</tr>`;
}

function rowHistory(r) {
    return `
<tr>
  <td>${fmtUtc(r.tsUtc)}</td>
  <td>${safeText(r.listeners)}</td>
  <td>${safeText(r.rank ?? "")}</td>
  <td><a class="link" href="${r.url}" target="_blank" rel="noreferrer">open</a></td>
</tr>`;
}

// ------------------ Inspector UI ------------------

function clearInspector() {
    if (!hasInspector()) return;

    if (state.lastSelectedTr) state.lastSelectedTr.classList.remove("selected");
    state.selected = null;
    state.lastSelectedTr = null;

    setText("inspectorSubtitle", "Click any row to inspect that feed.");

    setText("inspFeedName", "—");
    setText("inspFeedId", "—");
    setText("inspSelectedTime", "—");
    setText("inspSelectedListeners", "—");

    setText("statFeedMean", "—");
    setText("statFeedStd", "—");
    setText("statFeedP95", "—");
    setText("statThreshold", "—");
    setText("statGlobalMean", "—");
    setText("statGlobalP95", "—");
    setText("statHint", "—");

    const tbody = el("inspHistoryTable")?.querySelector("tbody");
    if (tbody) tbody.innerHTML = "";

    setHidden("inspEmpty", false);
    setHidden("inspError", true);
    setText("inspError", "");
}

function setInspectorLoading(sel) {
    if (!hasInspector()) return;

    setText("inspFeedName", safeText(sel.name));
    setText("inspFeedId", safeText(sel.feedId));
    setText("inspSelectedTime", sel.tsUtc ? fmtUtc(sel.tsUtc) : "—");
    setText("inspSelectedListeners", sel.listeners == null ? "—" : `${sel.listeners}`);

    setText("inspectorSubtitle", "Loading detector inspection…");

    setText("statFeedMean", "…");
    setText("statFeedStd", "…");
    setText("statFeedP95", "…");
    setText("statThreshold", "…");
    setText("statGlobalMean", "…");
    setText("statGlobalP95", "…");
    setText("statHint", "—");

    const tbody = el("inspHistoryTable")?.querySelector("tbody");
    if (tbody) tbody.innerHTML = "";

    setHidden("inspEmpty", true);
    setHidden("inspError", true);
    setText("inspError", "");
}

function setInspectorError(msg) {
    if (!hasInspector()) return;
    setText("inspectorSubtitle", "Failed to load inspector data.");
    setHidden("inspError", false);
    setText("inspError", msg);
    setHidden("inspEmpty", true);
}

// ------------------ API Calls ------------------

async function fetchStreamHistory(feedId) {
    const res = await fetch(`/api/stream-history?feedId=${encodeURIComponent(feedId)}&limit=5000`);
    if (!res.ok) throw new Error(`stream-history failed: ${res.status}`);
    return await res.json(); // { feedId, rows: [...] }
}

async function fetchInspection(feedId) {
    const res = await fetch(`/api/inspect-feed?feedId=${encodeURIComponent(feedId)}`);
    if (!res.ok) throw new Error(`inspect-feed failed: ${res.status}`);
    return await res.json();
}

// ------------------ Inspector Render ------------------

function renderInspection(payload, historyRows) {
    const b = payload?.baseline ?? {};
    const d = payload?.decision ?? {};
    const t = payload?.thresholds ?? {};
    const c = payload?.current ?? {};

    const baselineUsed = b.used ?? "—";

    const median = (typeof b.median === "number") ? b.median : null;
    const mad = (typeof b.mad === "number") ? b.mad : null;
    const currentZ = (typeof b.currentRobustZ === "number") ? b.currentRobustZ : null;

    setText("statFeedMean", median == null ? "—" : median.toFixed(1));
    setText("statFeedStd", mad == null ? "—" : mad.toFixed(1));

    let feedP95Text = "—";
    if (Array.isArray(b.recentRobustZ) && b.recentRobustZ.length > 0) {
        const zs = b.recentRobustZ.filter(x => typeof x === "number");
        if (zs.length > 0) {
            const maxZ = Math.max(...zs);
            feedP95Text = maxZ.toFixed(2);
        }
    }
    setText("statFeedP95", feedP95Text);

    const spikeTxt = d.isSpikeNow ? "SPIKE" : "no spike";
    setText("statThreshold", `${baselineUsed} • ${spikeTxt}`);

    setText("statGlobalMean", "—");
    setText("statGlobalP95", "—");

    const parts = [];
    if (c.listeners != null) parts.push(`current listeners=${c.listeners}`);
    parts.push(`rank=${c.rank ?? "null"}`);
    if (c.sampleAgeSeconds != null) parts.push(`age=${c.sampleAgeSeconds}s`);

    if (baselineUsed === "per_feed") {
        const pf = t.perFeed ?? {};
        parts.push(`per-feed: persist=${pf.persistSamples} z>=${pf.robustZ} recov<=${pf.recoveryZ}`);

        if (Array.isArray(b.recentRobustZ) && b.recentRobustZ.length > 0) {
            const zs = b.recentRobustZ.filter(x => typeof x === "number");
            if (zs.length > 0) {
                const minZ = Math.min(...zs);
                const maxZ = Math.max(...zs);
                parts.push(`recentZ min=${minZ.toFixed(2)} max=${maxZ.toFixed(2)}`);
            }
        }

        if (currentZ != null) parts.push(`currentZ=${currentZ.toFixed(2)}`);
    }
    else if (baselineUsed === "global_bucket") {
        const g = t.global ?? {};
        parts.push(`global: bucket=${b.bucketUsed} inferred=${b.inferredBucket}`);
        parts.push(`z>=${g.robustZ} & listeners>=${g.newFeedMinListeners}`);

        if (b.bucketStatsUsed) {
            const bs = b.bucketStatsUsed;
            parts.push(`bucketStats n=${bs.sampleCount} median=${Number(bs.median).toFixed(1)} mad=${Number(bs.mad).toFixed(1)}`);
        }

        if (currentZ != null) parts.push(`currentZ=${currentZ.toFixed(2)}`);
    }
    else if (baselineUsed === "hard_threshold") {
        const g = t.global ?? {};
        parts.push(`hard threshold listeners>=${g.newFeedMinListeners}`);
    }

    parts.push(`recovered=${d.isRecovered}`);
    if (Array.isArray(historyRows)) parts.push(`historyRows=${historyRows.length}`);

    setText("statHint", parts.join(" • "));
}

// ------------------ Select Feed ------------------

async function selectFeed(sel, tr) {
    if (!hasInspector()) return;

    state.selected = sel;

    if (state.lastSelectedTr) state.lastSelectedTr.classList.remove("selected");
    if (tr) tr.classList.add("selected");
    state.lastSelectedTr = tr ?? null;

    setInspectorLoading(sel);

    // Preserve inspector history scroll if it is inside a scroll container
    const inspScroll = el("inspHistoryScroll") ?? el("inspHistoryTable")?.parentElement ?? null;
    const inspSnap = captureScroll(inspScroll);

    try {
        const [hist, inspect] = await Promise.all([
            fetchStreamHistory(sel.feedId),
            fetchInspection(sel.feedId)
        ]);

        const rows = hist?.rows ?? [];
        const tbody = el("inspHistoryTable")?.querySelector("tbody");
        if (tbody) tbody.innerHTML = rows.map(rowHistory).join("");

        setHidden("inspEmpty", rows.length > 0);

        if (sel.listeners == null || sel.listeners === "") {
            const curListeners = inspect?.current?.listeners;
            if (typeof curListeners === "number") setText("inspSelectedListeners", `${curListeners}`);
        }

        const name = safeText(sel.name);
        setText("inspectorSubtitle", `Inspecting ${name} (${safeText(sel.feedId)})`);

        renderInspection(inspect, rows);
    } catch (e) {
        setInspectorError(e?.message ?? "Unknown error");
    } finally {
        // Restore inspector history scroll after DOM update
        requestAnimationFrame(() => restoreScroll(inspScroll, inspSnap));
    }
}

// ------------------ Table Click Wiring ------------------

function installTableClickHandlers() {
    const attach = (tableId) => {
        const table = el(tableId);
        if (!table) return;

        table.addEventListener("click", (evt) => {
            markUserAction();

            if (evt.target.closest("a")) return;

            const tr = evt.target.closest("tr");
            if (!tr) return;

            const feedId = tr.dataset.feedId;
            const name = tr.dataset.feedName;
            const tsRaw = tr.dataset.ts;
            const listenersRaw = tr.dataset.listeners;

            if (!feedId) return;

            const ts = tsRaw ? new Date(tsRaw) : null;

            const listeners =
                listenersRaw == null || listenersRaw === ""
                    ? null
                    : Number.isFinite(Number(listenersRaw)) ? Number(listenersRaw) : null;

            selectFeed({ feedId, name, tsUtc: ts, listeners }, tr);
        }, { passive: true });
    };

    attach("runTable");
    attach("samplesTable");
    attach("alertsTable");
}

// ------------------ Refresh ------------------

async function refreshAll() {
    // If user is actively scrolling/clicking/typing, don't rip the DOM apart.
    // This is not a hack; it's basic UX for polling dashboards.
    if (isUserActiveRecently(1200)) return;

    // Preserve scroll per-tab table containers during DOM updates.
    const runScroll = getScrollContainerForTable("runTable");
    const samplesScroll = getScrollContainerForTable("samplesTable");
    const alertsScroll = getScrollContainerForTable("alertsTable");

    const containers = [runScroll, samplesScroll, alertsScroll].filter(Boolean);

    await withPreservedScroll(containers, async () => {
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

            setText("latestRunMetric", run ? `${records.length} rows` : "—");
            setText(
                "latestRunHint",
                run ? `Run ${run.runId} • started ${fmtUtc(run.startedAtUtc)}` : "No ingest run recorded (yet)."
            );

            const tbody = el("runTable")?.querySelector("tbody");
            if (tbody) tbody.innerHTML = records.map(rowRun).join("");

            setHidden("runEmpty", !!run);
        } catch {
            setText("latestRunMetric", "—");
            setText("latestRunHint", "Failed to load latest run.");
        }

        // samples
        try {
            const res = await fetch("/api/samples?limit=10000");
            const data = await res.json();
            const rows = data.rows ?? [];

            setText("samplesMetric", `${rows.length} rows`);
            const tbody = el("samplesTable")?.querySelector("tbody");
            if (tbody) tbody.innerHTML = rows.map(rowSample).join("");
        } catch {
            setText("samplesMetric", "—");
        }

        // alerts
        try {
            const res = await fetch("/api/alerts?limit=750");
            const data = await res.json();
            const rows = data.rows ?? [];

            setText("alertsMetric", `${rows.length} rows`);
            const tbody = el("alertsTable")?.querySelector("tbody");
            if (tbody) tbody.innerHTML = rows.map(rowAlert).join("");
        } catch {
            setText("alertsMetric", "—");
        }

        setText("lastRefresh", `Refreshed ${fmtUtc(new Date())}`);

        applyFilter();

        // Reapply highlight after rebuilding table HTML
        reapplySelectionHighlight();

        // Optional: keep inspector updated on refresh
        if (state.selected?.feedId && !state.suppressInspectorRefresh) {
            // Do NOT pass old TR (it's stale). We will re-highlight via reapplySelectionHighlight().
            await selectFeed(state.selected, state.lastSelectedTr);
            // After inspector refresh, re-highlight again (selectFeed may have toggled state.lastSelectedTr)
            reapplySelectionHighlight();
        }
    });
}

// ------------------ Init ------------------

document.addEventListener("scroll", markUserAction, { passive: true });
document.addEventListener("mousemove", markUserAction, { passive: true });
document.addEventListener("keydown", markUserAction);

document.querySelectorAll(".tab").forEach(b => {
    b.addEventListener("click", () => {
        markUserAction();
        state.tab = b.dataset.tab;
        applyTabs();
    });
});

const search = el("search");
if (search) {
    search.addEventListener("input", (e) => {
        markUserAction();
        state.filter = e.target.value ?? "";
        applyFilter();
    });
}

const refreshBtn = el("refresh");
if (refreshBtn) refreshBtn.addEventListener("click", () => {
    markUserAction();
    refreshAll();
});

const clearBtn = el("inspectorClear");
if (clearBtn) clearBtn.addEventListener("click", () => {
    markUserAction();
    clearInspector();
});

installTableClickHandlers();
applyTabs();
clearInspector();
refreshAll();

// Poll, but don’t be obnoxious
setInterval(refreshAll, 15000);
