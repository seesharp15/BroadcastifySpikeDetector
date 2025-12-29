const el = (id) => document.getElementById(id);

const state = {
    tab: "run",
    filter: "",
    selected: null, // { feedId, name, tsUtc, listeners }
    lastSelectedTr: null,
    suppressInspectorRefresh: false
};

// ------------------ Utilities ------------------

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
  <td>${r.rank}</td>
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
  <td>${r.rank}</td>
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
  <td>${safeText(r.rank)}</td>
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

// ------------------ Inspector Render (matches your detector payload) ------------------

function renderInspection(payload, historyRows) {
    // Payload shape from /api/inspect-feed (the one we wrote):
    // {
    //   current: { listeners, rank, sampleAgeSeconds, tsUtc },
    //   baseline: { used, median, mad, currentRobustZ, recentRobustZ, bucketUsed, inferredBucket, bucketStatsUsed },
    //   thresholds: { perFeed: {...}, global: {...} },
    //   decision: { isSpikeNow, isRecovered },
    //   ...
    // }

    const b = payload?.baseline ?? {};
    const d = payload?.decision ?? {};
    const t = payload?.thresholds ?? {};
    const c = payload?.current ?? {};

    const baselineUsed = b.used ?? "—";

    // Your inspector labels say Mean/StdDev/P95, but your detector uses Median/MAD.
    // So we map:
    // - Feed Mean -> Median
    // - Feed Std Dev -> MAD
    // - Feed P95 -> (approx) max recent Z OR leave as — if not available
    // You can rename labels later; this makes it useful right now.

    const median = (typeof b.median === "number") ? b.median : null;
    const mad = (typeof b.mad === "number") ? b.mad : null;
    const currentZ = (typeof b.currentRobustZ === "number") ? b.currentRobustZ : null;

    setText("statFeedMean", median == null ? "—" : median.toFixed(1));
    setText("statFeedStd", mad == null ? "—" : mad.toFixed(1));

    // "Feed P95" isn't directly computed by detector. We'll show:
    // - If per_feed and we have recentZ, show max recentZ as a “recent peak”
    // - Else show —
    let feedP95Text = "—";
    if (Array.isArray(b.recentRobustZ) && b.recentRobustZ.length > 0) {
        const zs = b.recentRobustZ.filter(x => typeof x === "number");
        if (zs.length > 0) {
            const maxZ = Math.max(...zs);
            feedP95Text = maxZ.toFixed(2);
        }
    }
    setText("statFeedP95", feedP95Text);

    // Threshold slot: show decision summary in a compact way
    const spikeTxt = d.isSpikeNow ? "SPIKE" : "no spike";
    setText("statThreshold", `${baselineUsed} • ${spikeTxt}`);

    // Global mean/p95: not in payload. If you want those, we can add them to the API later.
    setText("statGlobalMean", "—");
    setText("statGlobalP95", "—");

    // Hint explains what you’re looking at (and why a spike didn’t fire)
    const parts = [];

    if (c.listeners != null) {
        parts.push(`current listeners=${c.listeners}`);
    }
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

    if (Array.isArray(historyRows)) {
        parts.push(`historyRows=${historyRows.length}`);
    }

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

    try {
        const [hist, inspect] = await Promise.all([
            fetchStreamHistory(sel.feedId),
            fetchInspection(sel.feedId)
        ]);

        const rows = hist?.rows ?? [];
        const tbody = el("inspHistoryTable")?.querySelector("tbody");
        if (tbody) tbody.innerHTML = rows.map(rowHistory).join("");

        setHidden("inspEmpty", rows.length > 0);

        // If the clicked row didn't have listeners (alerts tab), fill from inspection current
        if (sel.listeners == null || sel.listeners === "") {
            const curListeners = inspect?.current?.listeners;
            if (typeof curListeners === "number") setText("inspSelectedListeners", `${curListeners}`);
        }

        // Subtitle
        const name = safeText(sel.name);
        setText("inspectorSubtitle", `Inspecting ${name} (${safeText(sel.feedId)})`);

        renderInspection(inspect, rows);
    } catch (e) {
        setInspectorError(e?.message ?? "Unknown error");
    }
}

// ------------------ Table Click Wiring ------------------

function installTableClickHandlers() {
    const attach = (tableId) => {
        const table = el(tableId);
        if (!table) return;

        table.addEventListener("click", (evt) => {
            // Don't intercept link clicks
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
        });
    };

    attach("runTable");
    attach("samplesTable");
    attach("alertsTable");
}

// ------------------ Refresh ------------------

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

    // Optional: keep inspector updated on refresh (without re-highlighting a row)
    // This can be chatty; if you don't want it, comment this block out.
    if (state.selected?.feedId && !state.suppressInspectorRefresh) {
        // Don't pass the old tr because the table DOM may have been rebuilt; keep selection highlight as-is.
        await selectFeed(state.selected, state.lastSelectedTr);
    }
}

// ------------------ Init ------------------

document.querySelectorAll(".tab").forEach(b => {
    b.addEventListener("click", () => {
        state.tab = b.dataset.tab;
        applyTabs();
    });
});

const search = el("search");
if (search) {
    search.addEventListener("input", (e) => {
        state.filter = e.target.value ?? "";
        applyFilter();
    });
}

const refreshBtn = el("refresh");
if (refreshBtn) refreshBtn.addEventListener("click", refreshAll);

const clearBtn = el("inspectorClear");
if (clearBtn) clearBtn.addEventListener("click", clearInspector);

installTableClickHandlers();
applyTabs();
clearInspector();
refreshAll();
setInterval(refreshAll, 5000);
