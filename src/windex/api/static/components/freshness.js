// Freshness table — per-source index/pending counts, last-embed + last-update
// times, sorted freshest-first. Clicking a row expands a stats panel for that
// source (doc totals, per-status breakdown, content date span) fetched from
// /v1/datasets/{source}/stats. The manual "Check for data update" action now
// lives in the Control panel, so there is no per-row button here.
import { html, getJSON, usePoll, agoTs, num } from "../lib.js";
import { useState, useCallback } from "preact/hooks";

// most-recent activity for sorting; sources with no activity sort to the bottom
const activityTs = (r) => Math.max(r.last_embed_ts || 0, r.last_update_ts || 0);

// ISO string → short calendar date; null/undefined → null (caller omits the span)
const fmtDate = (iso) => (iso ? new Date(iso).toLocaleDateString() : null);

// The expanding stats panel for the selected source.
function StatsPanel({ stats }) {
  if (stats === null) return html`<div class="fstats fstats-loading">loading…</div>`;
  if (stats.error) return html`<div class="fstats fstats-loading">stats unavailable</div>`;
  const byStatus = stats.by_status || {};
  const from = fmtDate(stats.content_from);
  const to = fmtDate(stats.content_to);
  return html`
    <div class="fstats">
      <div class="fstat-total"><b>${num(stats.total)}</b> documents</div>
      <div class="fstat-list">
        ${Object.keys(byStatus).map((k) => html`
          <div class="fstat-kv" key=${k}><span class="fk">${k}</span><span class="fv">${num(byStatus[k])}</span></div>`)}
      </div>
      ${from && to
        ? html`<div class="fstat-span">content: ${from} → ${to}</div>`
        : ""}
    </div>`;
}

export function FreshnessTable() {
  const [rows, setRows] = useState(null);
  const [selected, setSelected] = useState(null); // source string or null
  const [stats, setStats] = useState(null);        // stats for `selected`, null while loading
  const load = useCallback(() => getJSON("/v1/freshness").then(setRows).catch(() => {}), []);
  usePoll(load, 15000);

  // Toggle the stats panel. Fetching sits in its own state, so the 15s
  // freshness poll re-renders the table without disturbing an open panel.
  const pick = (source) => {
    if (selected === source) { setSelected(null); setStats(null); return; }
    setSelected(source);
    setStats(null);
    getJSON(`/v1/datasets/${encodeURIComponent(source)}/stats`)
      .then(setStats)
      .catch(() => setStats({ error: true }));
  };

  const sorted = rows === null ? null : [...rows].sort((a, b) => activityTs(b) - activityTs(a));

  const body = sorted === null
    ? html`<tr><td colspan="5" class="fempty">loading…</td></tr>`
    : sorted.length === 0
      ? html`<tr><td colspan="5" class="fempty">no sources</td></tr>`
      : sorted.map((r) => html`
          <${FreshRow} key=${r.source} r=${r}
                       open=${selected === r.source}
                       stats=${selected === r.source ? stats : undefined}
                       onPick=${pick} />`);

  return html`
    <table class="ftable">
      <thead><tr>
        <th>source</th><th>indexed</th><th>pending</th><th>last embed</th><th>last update</th>
      </tr></thead>
      <tbody>${body}</tbody>
    </table>`;
}

// A data row plus, when selected, the expanding stats row beneath it. Kept as a
// component so htm can return the two <tr>s as a fragment.
function FreshRow({ r, open, stats, onPick }) {
  return html`
    <tr class=${"frow" + (open ? " open" : "")} onClick=${() => onPick(r.source)}>
      <td class="fname">${r.source}</td>
      <td>${num(r.indexed)}</td>
      <td>${num(r.pending)}</td>
      <td>${r.last_embed_ts ? agoTs(r.last_embed_ts) : "—"}</td>
      <td>${r.last_update_ts ? agoTs(r.last_update_ts) : "—"}</td>
    </tr>
    ${open
      ? html`<tr class="fstatsrow"><td colspan="5"><${StatsPanel} stats=${stats} /></td></tr>`
      : ""}`;
}
