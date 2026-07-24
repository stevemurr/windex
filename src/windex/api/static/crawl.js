// windex crawl console — its own page (/crawl), not a dashboard tab.
//
// Three panels: build a recipe (with a no-commit Preview), watch the live run
// over SSE, and re-run anything from history. Same no-build contract as the main
// console: vendored Preact + htm, shared helpers from lib.js.
//
// WRITE TOKEN. Starting/cancelling a crawl and previewing are write-token gated
// (they make the server fetch a caller-chosen host — the same capability either
// way). The dashboard console never needed a token because none of ITS actions
// are gated, so there is no existing pattern to reuse: the token is entered here
// and kept in localStorage. It is sent as `Authorization: Bearer`, never placed
// in a URL, so it stays out of server logs and browser history.
import { html, getJSON, num } from "./lib.js";
import { useState, useEffect, useCallback, useRef } from "preact/hooks";

const TOKEN_KEY = "windex.writeToken";
const getToken = () => localStorage.getItem(TOKEN_KEY) || "";

const postAuth = (path, body) =>
  fetch(path, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...(getToken() ? { Authorization: `Bearer ${getToken()}` } : {}),
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  });

// A 401 here means the token is missing/wrong; anything else carries the API's
// own message (422 recipe validation is the common one) which is far more useful
// to show verbatim than a generic "request failed".
async function callJSON(path, body) {
  const r = await postAuth(path, body);
  let data = null;
  try { data = await r.json(); } catch { /* empty body */ }
  if (!r.ok) {
    const detail = data && data.detail ? data.detail : `HTTP ${r.status}`;
    throw new Error(r.status === 401 ? "write token missing or invalid" : detail);
  }
  return data;
}

const DEFAULTS = {
  source: "", seed: "", max_depth: 2, max_pages: 500, host_interval: 2,
  path_prefix: "", whole_host: false,
  exclude: "\\.(js|css|woff2?|png|svg|ico|gif|jpe?g|pdf)$",
  include: "", quality_filters: false, prune: false,
};

// Form state → the recipe document the API validates.
//
// A blank path_prefix is OMITTED rather than sent as "": the server reads a
// missing prefix as "default to the seed's own directory" and an explicit "" as
// "the whole host", which are opposite meanings. That is why `whole_host` is a
// separate checkbox and not just an empty box — without it the form has no way
// to say "follow any link on this host", the shape you want when the seed is an
// index page whose articles live somewhere else entirely.
function toRecipe(f) {
  const lines = (s) => s.split("\n").map((x) => x.trim()).filter(Boolean);
  const scope = {};
  if (f.whole_host) scope.path_prefix = "";           // explicit: whole host
  else if (f.path_prefix.trim()) scope.path_prefix = f.path_prefix.trim();
  if (f.exclude.trim()) scope.exclude = lines(f.exclude);
  if (f.include.trim()) scope.include = lines(f.include);
  return {
    seed: f.seed.trim(),
    scope,
    limits: {
      max_depth: Number(f.max_depth),
      max_pages: Number(f.max_pages),
      host_interval: Number(f.host_interval),
    },
    extract: { quality_filters: !!f.quality_filters },
    dedup: { prune: !!f.prune },
  };
}

function Field({ label, children }) {
  return html`<div class="cw-field"><label>${label}</label>${children}</div>`;
}

function NewCrawl({ onStarted }) {
  const [f, setF] = useState(DEFAULTS);
  const [busy, setBusy] = useState("");
  const [msg, setMsg] = useState(null);
  const set = (k) => (e) => setF({ ...f, [k]: e.target.type === "checkbox" ? e.target.checked : e.target.value });

  const preview = async () => {
    setBusy("preview"); setMsg(null);
    try {
      const r = await callJSON("/v1/crawl/preview", toRecipe(f));
      const rej = Object.entries(r.rejected || {}).map(([k, v]) => `${k}:${v}`).join(", ");
      setMsg({
        ok: true,
        text: `${r.in_scope} page(s) would be crawled from ${r.seeds.length} seed(s).` +
              (rej ? ` Rejected — ${rej}.` : "") +
              (r.sample ? ` Sample: "${r.sample.title}" (${num(r.sample.chars)} chars).` : ""),
        urls: r.urls || [],
        suggest: r.suggest,
      });
    } catch (e) { setMsg({ ok: false, text: e.message }); }
    setBusy("");
  };

  const start = async () => {
    if (!f.source.trim()) { setMsg({ ok: false, text: "a source name is required" }); return; }
    setBusy("start"); setMsg(null);
    try {
      const r = await callJSON("/v1/crawl", { source: f.source.trim(), ...toRecipe(f) });
      setMsg({ ok: true, text: `queued run #${r.run_id} → ${r.source}` });
      onStarted(r.run_id);
    } catch (e) { setMsg({ ok: false, text: e.message }); }
    setBusy("");
  };

  return html`
    <div class="cw-card">
      <h2>New crawl</h2>
      <p class="cw-hint">
        Give a seed link and windex crawls that cluster into a searchable source.
        <strong>Preview</strong> fetches only the seed and reports what would be
        indexed — nothing is written until you Start.
      </p>
      <${Field} label="Source name (lowercase, a-z0-9_)">
        <input type="text" value=${f.source} onInput=${set("source")} placeholder="claude_cookbook" />
      <//>
      <${Field} label="Seed URL">
        <input type="text" value=${f.seed} onInput=${set("seed")}
               placeholder="https://platform.claude.com/cookbook/" />
      <//>
      <div class="cw-row">
        <${Field} label="Depth"><input type="number" min="0" max="8" value=${f.max_depth} onInput=${set("max_depth")} /><//>
        <${Field} label="Max pages"><input type="number" min="1" value=${f.max_pages} onInput=${set("max_pages")} /><//>
        <${Field} label="Interval (s)"><input type="number" min="1" step="0.5" value=${f.host_interval} onInput=${set("host_interval")} /><//>
      </div>
      <label class="cw-check" style="margin-bottom:10px">
        <input type="checkbox" checked=${f.whole_host} onChange=${set("whole_host")} />
        Whole-host link following (follow any link on this host, ignoring the path)
      </label>
      <${Field} label=${f.whole_host ? "Path prefix (disabled — whole host)"
                                     : "Path prefix (blank = the seed's own directory)"}>
        <input type="text" value=${f.whole_host ? "" : f.path_prefix}
               disabled=${f.whole_host}
               onInput=${set("path_prefix")} placeholder="/cookbook/" />
      <//>
      <${Field} label="Exclude patterns (regex, one per line)">
        <textarea value=${f.exclude} onInput=${set("exclude")}></textarea>
      <//>
      <${Field} label="Include patterns (regex, one per line — blank = allow all in prefix)">
        <textarea value=${f.include} onInput=${set("include")}></textarea>
      <//>
      <label class="cw-check">
        <input type="checkbox" checked=${f.quality_filters} onChange=${set("quality_filters")} />
        Apply quality filters (off suits curated doc sites — they over-reject short code-heavy pages)
      </label>
      <label class="cw-check" style="margin-top:8px">
        <input type="checkbox" checked=${f.prune} onChange=${set("prune")} />
        Self-cleaning (remove indexed pages this crawl no longer finds)
      </label>
      ${f.prune && html`<div class="cw-note" style="margin-top:8px">
        Pages already indexed under this source that this run does not reach will be
        removed. Skipped automatically if the run is cancelled, hits its page budget,
        or any page fails — an incomplete crawl is never treated as proof a page is gone.
      </div>`}
      <div class="cw-btns">
        <button class="cw-btn" disabled=${!!busy || !f.seed.trim()} onClick=${preview}>
          ${busy === "preview" ? "Previewing…" : "Preview"}
        </button>
        <button class="cw-btn cw-btn-primary" disabled=${!!busy || !f.seed.trim()} onClick=${start}>
          ${busy === "start" ? "Queuing…" : "Start crawl"}
        </button>
      </div>
      ${msg && html`
        <div class="cw-note ${msg.ok ? "" : "is-err"}" style="margin-top:12px">
          ${msg.text}
          ${msg.suggest && html`
            <div style="margin-top:8px">
              The seed's links mostly live under <code>${msg.suggest.path_prefix}</code>,
              outside the current scope. Use that prefix to reach
              ${" "}${msg.suggest.would_add} more page(s):
              <button class="cw-btn" style="margin-left:6px"
                      onClick=${() => setF({ ...f, whole_host: false,
                                             path_prefix: msg.suggest.path_prefix })}>
                Use ${msg.suggest.path_prefix}
              </button>
              <button class="cw-btn" style="margin-left:4px"
                      onClick=${() => setF({ ...f, whole_host: true })}>
                Whole host
              </button>
            </div>`}
          ${msg.urls && msg.urls.length > 0 && html`
            <div style="margin-top:8px;max-height:150px;overflow-y:auto">
              ${msg.urls.slice(0, 60).map((u) => html`<div><code>${u}</code></div>`)}
            </div>`}
        </div>`}
    </div>`;
}

const PCT = (r) => {
  const s = r.stats || {};
  const total = (s.found || 0) || 1;
  return Math.min(100, Math.round(((s.fetched || 0) / total) * 100));
};

function LiveRun({ runId, onCancelled }) {
  const [run, setRun] = useState(null);
  const [lines, setLines] = useState([]);
  const feedRef = useRef(null);

  useEffect(() => {
    if (!runId) return undefined;
    setRun(null); setLines([]);
    const es = new EventSource(`/v1/crawl/runs/${runId}/events`);
    es.addEventListener("run", (e) => setRun(JSON.parse(e.data)));
    es.addEventListener("urls", (e) => {
      const rows = JSON.parse(e.data);
      // Newest first, and bounded: a 20k-page crawl must not grow the DOM without
      // limit just because the tab stayed open.
      setLines((prev) => [...rows.reverse(), ...prev].slice(0, 500));
    });
    es.addEventListener("end", () => es.close());
    es.onerror = () => es.close();
    return () => es.close();
  }, [runId]);

  const cancel = async () => {
    try { await callJSON(`/v1/crawl/runs/${runId}/cancel`); onCancelled(); }
    catch (e) { alert(e.message); }
  };

  if (!runId) return html`<div class="cw-card"><h2>Live run</h2>
    <div class="cw-empty">Start a crawl, or pick a run from history, to watch it here.</div></div>`;
  const s = (run && run.stats) || {};
  const active = run && (run.status === "running" || run.status === "pending");
  return html`
    <div class="cw-card">
      <h2>Run #${runId} ${run && html`<span class="cw-tag ${run.status}">${run.status}</span>`}</h2>
      <p class="cw-hint">
        ${run ? `${run.source} — ${run.recipe && run.recipe.seeds ? run.recipe.seeds[0] : ""}` : "connecting…"}
      </p>
      <div class="cw-bar"><i style=${`width:${run ? PCT(run) : 0}%`}></i></div>
      <div class="cw-stats">
        <div class="cw-stat"><div class="cw-n">${num(s.found)}</div><div class="cw-l">found</div></div>
        <div class="cw-stat"><div class="cw-n">${num(s.fetched)}</div><div class="cw-l">fetched</div></div>
        <div class="cw-stat"><div class="cw-n">${num(s.staged)}</div><div class="cw-l">indexed</div></div>
        <div class="cw-stat"><div class="cw-n">${num(s.skipped)}</div><div class="cw-l">skipped</div></div>
        <div class="cw-stat ${s.failed ? "is-bad" : ""}"><div class="cw-n">${num(s.failed)}</div><div class="cw-l">failed</div></div>
        ${s.pruned !== undefined && html`
          <div class="cw-stat"><div class="cw-n">${num(s.pruned)}</div><div class="cw-l">removed</div></div>`}
      </div>
      ${s.prune_skipped && html`<div class="cw-note" style="margin-bottom:12px">
        Self-cleaning did not run: <code>${s.prune_skipped}</code>. The crawl was
        incomplete, so removing pages it did not reach would have deleted content
        that is still there.
      </div>`}
      ${s.truncated && html`<div class="cw-note is-err" style="margin-bottom:12px">
        Stopped at the <code>max_pages</code> budget with URLs still queued — this
        crawl is <strong>incomplete</strong>. Raise the budget and re-run to finish it.
      </div>`}
      ${run && run.error && html`<div class="cw-note is-err" style="margin-bottom:12px">${run.error}</div>`}
      ${active && html`<div class="cw-btns" style="margin-top:0;margin-bottom:12px">
        <button class="cw-btn cw-btn-danger" onClick=${cancel}>Stop crawl</button>
      </div>`}
      <div class="cw-feed" ref=${feedRef}>
        ${lines.length === 0
          ? html`<div class="cw-empty">no page results yet</div>`
          : lines.map((l) => html`
              <div class="cw-line">
                <span class="cw-tag ${l.status}">${l.status}</span>
                <span class="cw-u"><a href=${l.url} target="_blank" rel="noopener noreferrer">${l.url}</a></span>
                ${l.reason && html`<span class="cw-l" style="color:var(--muted)">${l.reason}</span>`}
              </div>`)}
      </div>
    </div>`;
}

function History({ selected, onSelect, refreshKey }) {
  const [runs, setRuns] = useState([]);
  const load = useCallback(() => getJSON("/v1/crawl/runs?limit=25")
    .then((d) => setRuns(d.runs || [])).catch(() => {}), []);
  useEffect(() => { load(); const id = setInterval(load, 5000); return () => clearInterval(id); },
            [load, refreshKey]);

  const rerun = async (r) => {
    try {
      // Re-runs the run's OWN frozen recipe, not the source's current one — what
      // you see in this row is what executes.
      const out = await callJSON("/v1/crawl", { source: r.source, ...r.recipe });
      onSelect(out.run_id);
    } catch (e) { alert(e.message); }
  };

  return html`
    <div class="cw-card">
      <h2>History</h2>
      <p class="cw-hint">Every run keeps the exact recipe it executed, so re-running reproduces it.</p>
      ${runs.length === 0
        ? html`<div class="cw-empty">no crawls yet</div>`
        : html`<table class="cw-runs">
            <thead><tr><th>#</th><th>Source</th><th>Status</th><th>Indexed</th><th>When</th><th></th></tr></thead>
            <tbody>
              ${runs.map((r) => html`
                <tr class=${r.id === selected ? "is-sel" : ""}>
                  <td class="cw-mono">${r.id}</td>
                  <td>${r.source}</td>
                  <td><span class="cw-tag ${r.status}">${r.status}</span></td>
                  <td class="cw-mono">${num((r.stats || {}).staged)}</td>
                  <td class="cw-mono">${(r.requested_at || "").replace("T", " ").slice(0, 16)}</td>
                  <td>
                    <button class="cw-btn" onClick=${() => onSelect(r.id)}>Watch</button>
                    <button class="cw-btn" onClick=${() => rerun(r)}>Re-run</button>
                  </td>
                </tr>`)}
            </tbody>
          </table>`}
    </div>`;
}

// Exported as a TAB rather than self-rendering a page: the shell (header, token
// field, tab strip) lives in manage.js so the Sources and Crawl tabs share one
// token entry instead of each keeping its own.
export function CrawlTab() {
  const [runId, setRunId] = useState(null);
  const [refreshKey, setRefreshKey] = useState(0);
  const select = (id) => { setRunId(id); setRefreshKey((k) => k + 1); };
  return html`
    <div class="cw-grid">
      <div><${NewCrawl} onStarted=${select} /></div>
      <div>
        <${LiveRun} runId=${runId} onCancelled=${() => setRefreshKey((k) => k + 1)} />
        <${History} selected=${runId} onSelect=${select} refreshKey=${refreshKey} />
      </div>
    </div>`;
}
