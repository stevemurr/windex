// Control panel — per-dataset loop controls + system actions.
// The reference component the other sections follow: a function component with
// hooks, a poll, action handlers hitting the /v1/* endpoints, and markup via
// the shared `html` tag reusing the existing CSS classes.
//
// Each dataset row carries FOUR self-explanatory controls: an embed-loop
// toggle (embed desired-state), an ingest-loop toggle (auto-fetch
// desired-state), a "Check for data update" button (kicks a refresh sweep and
// pops the dock to its log), and a "Logs" button (tails that source's embed
// log). The stageline + sysbtns stay as they were.
import { html, getJSON, post, usePoll, openDockLog } from "../lib.js";
import { useState, useCallback } from "preact/hooks";

// state → a plain status word (not the cryptic "UP"). "up" = the loop process
// is alive, "down" = enabled but not running, "disabled" = turned off.
const STATE_WORD = { up: "running", down: "stopped", disabled: "disabled" };

export function ControlPanel() {
  const [data, setData] = useState({ loops: [], watchdog_running: false, indexing_paused: false });
  const load = useCallback(() => getJSON("/v1/loops").then(setData).catch(() => {}), []);
  usePoll(load, 4000);

  const toggleEmbed = async (source, enabled) => { await post(`/v1/loops/${source}`, { enabled }); load(); };
  const toggleIngest = async (source, enabled) => { await post(`/v1/ingest/${source}`, { enabled }); load(); };
  const bulk = async (enabled) => {
    if (!enabled && !confirm("Stop ALL embed loops? They stay off until turned back on.")) return;
    await post("/v1/system/loops", { enabled }); load();
  };
  const sys = async (action) => {
    const r = await post(`/v1/system/${action}`);
    if (!r.ok) { alert("action failed"); return; }
    setTimeout(load, 1200);
  };
  const checkUpdate = async (source) => {
    await post("/v1/system/refresh", { sources: [source] });
    openDockLog("refresh", "Refresh sweep");
  };

  const loops = data.loops || [];
  const on = loops.filter((l) => l.enabled).length;
  return html`
    <div class="stageline">
      supervisor <b>${data.watchdog_running ? "running" : "not running"}</b>
      · <b>${on}/${loops.length}</b> loops on
    </div>
    <div class="sysbtns">
      <button onClick=${() => sys("up")} title="Start any enabled loop or serve that is down">Reconcile</button>
      <button onClick=${() => sys("restart")} title="Stop every loop, then start the enabled ones">Restart loops</button>
      <button onClick=${() => bulk(true)}>Start all</button>
      <button class="stop" onClick=${() => bulk(false)}>Stop all</button>
    </div>
    <div class="loopgrid">
      ${loops.map((l) => html`
        <div class="looprow" key=${l.source}>
          <div class="lhead">
            <span class="lname">${l.source}</span>
            <span class=${"lstate " + l.state}>${STATE_WORD[l.state] || l.state}</span>
          </div>
          <div class="lctls">
            <button class=${"ltog " + (l.enabled ? "on" : "off")}
                    title="Embed loop desired-state — turns embedding of new ${l.source} content on or off"
                    onClick=${() => toggleEmbed(l.source, !l.enabled)}>
              Embed loop: ${l.enabled ? "on" : "off"}</button>
            <button class=${"ltog " + (l.ingest_enabled ? "on" : "off")}
                    title="Ingest loop desired-state — turns auto-fetching of new ${l.source} data on or off"
                    onClick=${() => toggleIngest(l.source, !l.ingest_enabled)}>
              Ingest loop: ${l.ingest_enabled ? "on" : "off"}</button>
            <button class="fbtn" title=${"Fetch any new content for " + l.source + " now"}
                    onClick=${() => checkUpdate(l.source)}>Check for data update</button>
            <button class="fbtn" title=${"Tail the " + l.source + " embed log"}
                    onClick=${() => openDockLog(l.log, l.source + " embed")}>Logs</button>
          </div>
        </div>`)}
    </div>`;
}
