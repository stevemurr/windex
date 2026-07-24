// windex manage — one console for source settings and crawling.
//
// Two tabs rather than two pages: editing a source's settings and re-crawling it
// are the same job, and separate pages would duplicate the write-token entry and
// the source list. The token lives HERE, in the shell, so both tabs share it.
//
// WRITE TOKEN. Everything that mutates (settings PATCH/DELETE, crawl start,
// cancel, preview) is write-token gated. It is entered once, kept in
// localStorage, and always sent as an `Authorization: Bearer` header — never in
// a URL, so it stays out of server logs and browser history. It is deliberately
// NOT injected server-side: `GET /manage` is unauthenticated on the LAN, so
// embedding the token in the page would hand it to anyone who loads it, and that
// same token also guards /v1/memory/* and /v1/sources.
import { render } from "preact";
import { html } from "./lib.js";
import { useState, useEffect } from "preact/hooks";
import { CrawlTab } from "./crawl.js";
import { SourcesTab } from "./components/sources.js";

const TOKEN_KEY = "windex.writeToken";
const TABS = [["sources", "Sources"], ["crawl", "Crawl"]];

function App() {
  const initial = (location.hash || "").replace("#", "");
  const [tab, setTab] = useState(TABS.some(([t]) => t === initial) ? initial : "sources");
  const [token, setToken] = useState(localStorage.getItem(TOKEN_KEY) || "");

  useEffect(() => { localStorage.setItem(TOKEN_KEY, token); }, [token]);
  useEffect(() => { location.hash = tab; }, [tab]);

  return html`
    <div class="cw-wrap">
      <div class="cw-head">
        <h1>windex manage</h1>
        <span class="cw-sub">source settings and web-cluster crawling</span>
        <span class="cw-spacer"></span>
        <a href="/">← console</a>
      </div>

      <div class="cw-tabs">
        ${TABS.map(([id, label]) => html`
          <button class="cw-tab ${tab === id ? "is-on" : ""}" onClick=${() => setTab(id)}>
            ${label}
          </button>`)}
        <span class="cw-spacer"></span>
        <input class="cw-token" type="password" value=${token}
               onInput=${(e) => setToken(e.target.value)}
               placeholder="write token" title="WINDEX_WRITE_TOKEN — required for any change" />
      </div>

      ${tab === "sources"
        ? html`<${SourcesTab} token=${token} />`
        : html`<${CrawlTab} />`}
    </div>`;
}

render(html`<${App} />`, document.getElementById("root"));
