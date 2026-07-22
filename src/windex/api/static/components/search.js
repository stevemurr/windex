// Search tab — free-text query over the /v1/search index with source + mode
// filters. Submit-only, no polling: holds q / source / mode plus the last
// results and timings. Mirrors the inline console markup 1:1 — same CSS class
// names, same /v1/search query params (q, source, mode, limit).
import { html, num } from "../lib.js";
import { useState, useCallback } from "preact/hooks";

// [value, label] — value is the /v1/search `source` param, label is the pill.
const SOURCES = [
  ["all", "Everything"],
  ["news", "News"],
  ["github", "GitHub"],
  ["wiki", "Wiki"],
  ["arxiv", "arXiv"],
  ["smallweb", "Small Web"],
  ["docs", "Docs"],
  ["hn", "HN"],
  ["hf", "HF"],
];

export function SearchTab() {
  const [q, setQ] = useState("");
  const [source, setSource] = useState("all");
  const [mode, setMode] = useState("hybrid");
  const [took, setTook] = useState("");
  const [view, setView] = useState({ kind: "idle" });

  const run = useCallback(async (e) => {
    e.preventDefault();
    const query = q.trim();
    if (!query) return;
    setView({ kind: "searching" });
    setTook("");
    const params = new URLSearchParams({ q: query, source, mode, limit: 10 });
    try {
      const r = await fetch(`/v1/search?${params}`);
      const data = await r.json();
      if (!r.ok) throw new Error(JSON.stringify(data.detail || data));
      const tm = data.timings || {};
      const parts = [`${data.results.length} result${data.results.length === 1 ? "" : "s"}`,
                     `${num(tm.total_ms ?? data.took_ms)} ms total`];
      if (tm.embed_query_ms) parts.push(`embed ${num(tm.embed_query_ms)} ms`);
      if (tm.search_ms != null) parts.push(`index ${num(tm.search_ms)} ms`);
      if ((data.mode || "").includes("degraded")) parts.push("⚠ degraded to keyword");
      setTook(parts.join(" · "));
      setView({ kind: "results", items: data.results });
    } catch (err) {
      setView({ kind: "error", message: String(err.message || err) });
    }
  }, [q, source, mode]);

  return html`
    <form id="f" onSubmit=${run}>
      <input id="q" type="search"
             placeholder="Search news, GitHub projects, Wikipedia, arXiv, the Small Web, programming docs, and Hacker News…"
             autocomplete="off" autofocus value=${q} onInput=${(e) => setQ(e.target.value)} />
      <button type="submit">Search</button>
    </form>
    <div class="controls">
      <div class="seg" role="radiogroup" aria-label="Source">
        ${SOURCES.map(([value, label]) => html`
          <label key=${value}>
            <input type="radio" name="source" value=${value}
                   checked=${source === value} onChange=${() => setSource(value)} />${label}
          </label>`)}
      </div>
      <span class="mode">
        <select id="mode" class="pillselect" aria-label="Retrieval mode"
                value=${mode} onChange=${(e) => setMode(e.target.value)}>
          <option value="hybrid">Hybrid</option>
          <option value="dense">Semantic</option>
          <option value="lexical">Keyword</option>
        </select>
      </span>
    </div>

    <div id="took" aria-live="polite">${took}</div>
    <div id="results">${renderResults(view)}</div>`;
}

// Results block mirrors the four inline states: pre-search (empty), in-flight,
// error, and the result cards (or the "nothing matched" note when zero).
function renderResults(view) {
  if (view.kind === "idle") return "";
  if (view.kind === "searching") return html`<div class="empty">searching…</div>`;
  if (view.kind === "error") return html`<div class="empty">Search failed: ${view.message}</div>`;
  if (!view.items.length)
    return html`<div class="empty">Nothing matched. Try fewer or different words.</div>`;
  return view.items.map((res) => html`
    <article class="card" key=${res.url}>
      <h3><a href=${res.url} target="_blank" rel="noopener">${res.title || res.url}</a></h3>
      <div class="meta">
        <span class=${"badge src-" + res.source}>${res.source}</span>
        ${res.stars != null ? html`<span class="badge">★ ${num(res.stars)}</span>` : ""}
        ${res.points != null ? html`<span class="badge">▲ ${num(res.points)}</span>` : ""}
        ${res.language ? html`<span class="badge">${res.language}</span>` : ""}
        ${res.primary_category ? html`<span class="badge">${res.primary_category}</span>` : ""}
        ${res.authors ? html`<span class="badge">${res.authors}</span>` : ""}
        ${res.published_at ? html`<span class="badge">${res.published_at.slice(0, 10)}</span>` : ""}
        <span class="url">${res.url}</span>
      </div>
      ${res.snippet ? html`<p class="snippet">${res.snippet}</p>` : ""}
    </article>`);
}
