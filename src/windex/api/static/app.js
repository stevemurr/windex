// windex console — Preact + htm, no build, vendored (see vendor/VENDOR.md).
// App shell: owns the header chrome (wordmark, tagline, Grafana link,
// pause/resume) and the Search / Console pill tabs, then composes the section
// components that live under ./components/*. Same CSS class names and same
// /v1/* endpoints as the inline dashboard console — nothing new invented here.
import { render } from "preact";
import { html, getJSON, post, usePoll } from "./lib.js";
import { useState, useEffect, useRef, useLayoutEffect, useCallback } from "preact/hooks";
import { ControlPanel } from "./components/control.js";
import { SearchTab } from "./components/search.js";
import { FreshnessTable } from "./components/freshness.js";
import { ScheduledJobs } from "./components/schedule.js";
import { ActivityDock } from "./components/dock.js";

// [id, label] — id is the hash fragment + pane suffix, label is the pill text.
const TABS = [["search", "Search"], ["console", "Console"]];
const tabIndex = (id) => TABS.findIndex(([t]) => t === id);

// Grafana link — fetched once. The server advertises the URL in /v1/stats
// links; we only render the anchor when it's actually configured.
function GrafanaLink() {
  const [href, setHref] = useState("");
  useEffect(() => {
    getJSON("/v1/stats")
      .then((d) => setHref((d.links && d.links.grafana) || ""))
      .catch(() => {});
  }, []);
  if (!href) return "";
  return html`<a id="grafanaLink" href=${href} target="_blank" rel="noopener">Metrics ↗</a>`;
}

// Pause/resume indexing — polls /v1/loops for indexing_paused and flips it.
// Hidden until the first poll lands (like the inline #ctl's initial display:none).
function PauseButton() {
  const [paused, setPaused] = useState(null);
  const load = useCallback(
    () => getJSON("/v1/loops").then((d) => setPaused(!!d.indexing_paused)).catch(() => {}), []);
  usePoll(load, 4000);
  if (paused === null) return "";
  const toggle = async () => { await post(`/v1/control/${paused ? "start" : "pause"}`); load(); };
  return html`<button id="ctl" onClick=${toggle}>${paused ? "Resume indexing" : "Pause indexing"}</button>`;
}

// Search / Console pill tabs. The pill tracks the active button's box (measured
// after layout); both panes stay mounted and CSS .active toggles visibility, so
// each tab keeps its own state across switches.
function Tabs({ active, onSwitch }) {
  const navRef = useRef(null);
  const pillRef = useRef(null);
  const movePill = useCallback(() => {
    const nav = navRef.current, pill = pillRef.current;
    if (!nav || !pill) return;
    const btn = nav.querySelector("button.active");
    if (btn) { pill.style.left = `${btn.offsetLeft}px`; pill.style.width = `${btn.offsetWidth}px`; }
  }, []);
  useLayoutEffect(movePill, [active]);
  useEffect(() => {
    window.addEventListener("resize", movePill);
    return () => window.removeEventListener("resize", movePill);
  }, [movePill]);
  return html`
    <nav class="tabs" role="tablist" ref=${navRef}>
      <span class="pill" aria-hidden="true" ref=${pillRef}></span>
      ${TABS.map(([id, label]) => html`
        <button key=${id} id=${"tabbtn-" + id} role="tab"
                class=${active === id ? "active" : ""}
                onClick=${() => onSwitch(id)}>${label}</button>`)}
    </nav>`;
}

function App() {
  const [active, setActive] = useState(location.hash === "#console" ? "console" : "search");
  const [dir, setDir] = useState("");   // pane slide direction on the last switch
  const switchTab = (name) => {
    if (name === active) return;
    setDir(tabIndex(name) > tabIndex(active) ? "slide-left" : "slide-right");
    setActive(name);
    history.replaceState(null, "", `#${name}`);
  };
  const paneClass = (id) => "tabpane" + (active === id ? " active " + dir : "");

  return html`
    <main>
      <header>
        <span class="wordmark">windex</span>
        <span class="tagline">self-hosted web index</span>
        <span style="margin-left:auto"></span>
        <${GrafanaLink} />
        <${PauseButton} />
      </header>

      <${Tabs} active=${active} onSwitch=${switchTab} />

      <section id="tab-search" class=${paneClass("search")}>
        <${SearchTab} />
      </section>

      <section id="tab-console" class=${paneClass("console")}>
        <section id="status">
          <h2>Control</h2>
          <${ControlPanel} />

          <h2 style="margin-top:1.6rem">Freshness</h2>
          <${FreshnessTable} />

          <h2 style="margin-top:1.6rem">Scheduled jobs</h2>
          <${ScheduledJobs} />
        </section>
      </section>
    </main>`;
}

// The activity dock is position:fixed and lives outside the main flow, so it
// mounts once into its own node; the App proper renders into #root.
render(html`<${ActivityDock} />`, document.body.appendChild(document.createElement("div")));
render(html`<${App} />`, document.getElementById("root"));
