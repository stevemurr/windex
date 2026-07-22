// Recent progress feed — a rolling log of the newest documents by a timestamp.
// Two instances live in the Console pane: "recently indexed" (created_at, freshly
// harvested/staged) and "recently embedded" (indexed_at, landed in Qdrant). Both
// hit /v1/recent/{indexed,embedded} and get the same shape:
// {id, source, url, title, ts} where ts is unix epoch seconds (→ agoTs).
import { html, getJSON, usePoll, agoTs } from "../lib.js";
import { useState, useCallback } from "preact/hooks";

export function RecentFeed({ endpoint, empty }) {
  const [rows, setRows] = useState(null);
  const load = useCallback(() => getJSON(endpoint).then(setRows).catch(() => {}), [endpoint]);
  usePoll(load, 5000);

  if (rows === null) return html`<div class="feed-empty">loading…</div>`;
  if (rows.length === 0) return html`<div class="feed-empty">${empty}</div>`;
  return html`
    <div class="feed">
      ${rows.map((r) => html`
        <div class="feedrow" key=${r.id + ":" + r.ts}>
          <span class="badge src-${r.source}">${r.source}</span>
          <span class="t">
            <a href=${r.url} target="_blank" rel="noopener">${r.title || r.url}</a>
          </span>
          <span class="when">${agoTs(r.ts)}</span>
        </div>`)}
    </div>`;
}
