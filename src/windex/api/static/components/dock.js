// Bottom activity dock (gmail-chat style). Polls /v1/activity every ~3s and
// shows a fixed summary bar with running/error chips; expanding it lists the
// activities grouped Actions/Loops/Services, and each row tails its log via
// /v1/logs/{name}. Migrated verbatim (classes + endpoints) from the inline
// dashboard console; listens for lib.js's windex:opendock to pop straight to a
// log when Freshness/Scheduled fire openDockLog.
import { html, getJSON, usePoll, agoTs } from "../lib.js";
import { useState, useCallback, useEffect, useLayoutEffect, useRef } from "preact/hooks";

const GROUPS = { action: "Actions", loop: "Loops", service: "Services" };
const dstatusColor = (a) => (a.error ? "var(--accent)" : a.running ? "var(--ok)" : "var(--muted)");
const isErr = (ln) => /error|traceback|fail|exception/i.test(ln);

// The grouped activity list — direct dgroup/dockrow children of #dockPanel.
function DockList({ acts, onOpen }) {
  const items = [];
  for (const g of Object.keys(GROUPS)) {
    const rows = acts.filter((a) => a.group === g);
    if (!rows.length) continue;
    items.push(html`<div class="dgroup" key=${"g-" + g}>${GROUPS[g]}</div>`);
    for (const a of rows) {
      const anim = a.running ? ";animation:pulse 1.6s ease-in-out infinite" : "";
      const meta = a.running ? "running" : a.error ? "error" : a.last_ts ? agoTs(a.last_ts) : "—";
      items.push(html`
        <div class="dockrow" key=${a.name} onClick=${() => onOpen({ name: a.name, label: a.label })}>
          <span class="dstatus" style=${"background:" + dstatusColor(a) + anim}></span>
          <span style="flex:1">${a.label}</span>
          <span style="color:var(--muted);font-size:.72rem">${meta}</span>
        </div>`);
    }
  }
  return items.length ? items : html`<div class="dgroup">nothing to show</div>`;
}

// #dockLogBody contents: loading → unavailable → lines (error lines flagged).
function logBody(log) {
  if (log === null) return "loading…";
  if (!log.available) return "log unavailable (not started yet, or drive detached)";
  if (!log.lines || !log.lines.length) return "(empty)";
  return log.lines.map((ln, i) => html`<div key=${i} class=${isErr(ln) ? "errline" : ""}>${ln}</div>`);
}

export function ActivityDock() {
  const [acts, setActs] = useState([]);
  const [open, setOpen] = useState(false);
  const [current, setCurrent] = useState(null); // { name, label } or null (list view)
  const [log, setLog] = useState(null);          // { available, lines } or null (loading)
  const bodyRef = useRef(null);
  const wasAtBottom = useRef(true);

  const load = useCallback(() => getJSON("/v1/activity").then(setActs).catch(() => {}), []);
  usePoll(load, 3000);

  // Any section can pop the dock open to a specific log (lib.js openDockLog).
  useEffect(() => {
    const onOpen = (e) => { setOpen(true); setCurrent({ name: e.detail.name, label: e.detail.label }); };
    window.addEventListener("windex:opendock", onOpen);
    return () => window.removeEventListener("windex:opendock", onOpen);
  }, []);

  // Tail the selected log every ~1.5s while it's open.
  useEffect(() => {
    if (!current) return;
    setLog(null);
    let alive = true;
    const tail = async () => {
      let d;
      try { d = await getJSON(`/v1/logs/${encodeURIComponent(current.name)}?lines=400`); } catch (e) { return; }
      if (!alive) return;
      const body = bodyRef.current;
      wasAtBottom.current = body ? body.scrollHeight - body.scrollTop - body.clientHeight < 40 : true;
      setLog(d);
    };
    tail();
    const id = setInterval(tail, 1500);
    return () => { alive = false; clearInterval(id); };
  }, [current]);

  // Keep the tail pinned to the bottom while the user is already there.
  useLayoutEffect(() => {
    const body = bodyRef.current;
    if (body && wasAtBottom.current) body.scrollTop = body.scrollHeight;
  }, [log]);

  const toggle = () => { setCurrent(null); setOpen((o) => !o); };

  const running = acts.filter((a) => a.running).length;
  const errs = acts.filter((a) => a.error);
  const chips = acts.filter((a) => a.error || (a.running && a.group === "action"));
  const summary = `Activity — ${running} running${errs.length ? ` · ${errs.length} error` : ""}`;

  return html`
    <div id="activityDock">
      <div id="dockBar" onClick=${toggle}>
        <span id="dockSummary">${summary}</span>
        <span id="dockChips">${chips.map((a) => html`
          <span class=${"dchip " + (a.error ? "err" : "run")} key=${a.name}><span class="dot"></span>${a.label}</span>`)}
        </span>
        <span id="dockCaret">${open ? "▾" : "▴"}</span>
      </div>
      ${open && html`
        <div id="dockPanel">
          ${current
            ? html`
              <div id="dockLogHead">
                <button onClick=${() => setCurrent(null)}>‹ back</button>
                <b>${current.label}</b>
              </div>
              <pre id="dockLogBody" ref=${bodyRef}>${logBody(log)}</pre>`
            : html`<${DockList} acts=${acts} onOpen=${setCurrent} />`}
        </div>`}
    </div>`;
}
