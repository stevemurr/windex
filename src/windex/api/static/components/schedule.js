// Scheduled jobs — an editor over the DB-backed schedule (full CRUD).
// Polls /v1/schedule; each row edits hour/minute/weekday/enabled inline
// (Save = PUT), can Run now (POST, tails the log via the dock), or Delete
// (DELETE, with a confirm). An "Add schedule" form appends new ingest/command
// entries. Invalid edits come back 422 with a message, which we surface.
import { html, getJSON, post, usePoll, agoTs, openDockLog } from "../lib.js";
import { useState, useCallback } from "preact/hooks";

// PUT/DELETE aren't in lib.js (post is POST-only); thin wrappers here.
const putJSON = (path, body) =>
  fetch(path, { method: "PUT", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
const del = (path) => fetch(path, { method: "DELETE" });

// weekday convention: 0=Sun … 6=Sat, null = every day. "" is the null option.
const WEEKDAYS = [["", "Every day"], ["0", "Sun"], ["1", "Mon"], ["2", "Tue"],
                  ["3", "Wed"], ["4", "Thu"], ["5", "Fri"], ["6", "Sat"]];
const wdToOpt = (w) => (w === null || w === undefined ? "" : String(w));
const optToWd = (v) => (v === "" ? null : Number(v));

const INGEST_TARGETS = ["ccnews", "gh", "wiki", "arxiv", "smallweb", "docs", "hn", "hf"];
const COMMAND_TARGETS = ["daily", "maintain"];

// One editable schedule row. Editable fields live in local state seeded from
// props (keyed by name, so the 15s poll re-render never clobbers an in-progress
// edit); read-only context (cadence, last run, running) comes straight from props.
function ScheduleRow({ row, onReload }) {
  const [hour, setHour] = useState(row.hour);
  const [minute, setMinute] = useState(row.minute);
  const [weekday, setWeekday] = useState(row.weekday);
  const [enabled, setEnabled] = useState(!!row.enabled);

  const save = async () => {
    const r = await putJSON(`/v1/schedule/${encodeURIComponent(row.name)}`,
      { hour: Number(hour), minute: Number(minute), weekday, enabled });
    if (r.status === 422) { alert(await r.text()); return; }
    if (!r.ok) { alert("save failed"); return; }
    onReload();
  };
  const run = async () => {
    const r = await post(`/v1/schedule/${encodeURIComponent(row.name)}/run`);
    if (!r.ok) { alert("failed to start"); return; }
    openDockLog(row.name, row.label);
    setTimeout(onReload, 800);
  };
  const remove = async () => {
    if (!confirm(`Delete schedule "${row.label}"?`)) return;
    const r = await del(`/v1/schedule/${encodeURIComponent(row.name)}`);
    if (r.status === 404) { alert("already gone"); onReload(); return; }
    if (!r.ok) { alert("delete failed"); return; }
    onReload();
  };

  return html`
    <div class="schedrow" key=${row.name}>
      <div class="shead">
        <span class="sname">${row.label}</span>
        <span class="swhen">${row.cadence}${row.last_run_ts ? " · ran " + agoTs(row.last_run_ts) : ""}</span>
      </div>
      <div class="sedit">
        <label>hour<input type="number" min="0" max="23" value=${hour}
                          onInput=${(e) => setHour(e.currentTarget.value)} /></label>
        <label>min<input type="number" min="0" max="59" value=${minute}
                         onInput=${(e) => setMinute(e.currentTarget.value)} /></label>
        <label>day<select value=${wdToOpt(weekday)} onChange=${(e) => setWeekday(optToWd(e.currentTarget.value))}>
          ${WEEKDAYS.map(([v, t]) => html`<option key=${v} value=${v}>${t}</option>`)}
        </select></label>
        <label class="schk"><input type="checkbox" checked=${enabled}
                                   onChange=${(e) => setEnabled(e.currentTarget.checked)} /> enabled</label>
      </div>
      <div class="sbtns">
        <button class="fbtn" onClick=${save}>Save</button>
        <button class="fbtn" disabled=${row.running} onClick=${run}>${row.running ? "running…" : "Run"}</button>
        <button class="fbtn sdel" onClick=${remove}>Delete</button>
      </div>
    </div>`;
}

// The "Add schedule" form. Target options depend on kind; the name field
// auto-fills a sensible default until the user types their own.
function AddSchedule({ onAdded }) {
  const [kind, setKind] = useState("ingest");
  const [target, setTarget] = useState(INGEST_TARGETS[0]);
  const [hour, setHour] = useState(3);
  const [minute, setMinute] = useState(0);
  const [weekday, setWeekday] = useState(null);
  const [name, setName] = useState("ingest-" + INGEST_TARGETS[0]);
  const [nameEdited, setNameEdited] = useState(false);

  const defaultName = (k, t) => (k === "ingest" ? `ingest-${t}` : `${t}`);
  const changeKind = (k) => {
    const t = (k === "ingest" ? INGEST_TARGETS : COMMAND_TARGETS)[0];
    setKind(k); setTarget(t);
    if (!nameEdited) setName(defaultName(k, t));
  };
  const changeTarget = (t) => {
    setTarget(t);
    if (!nameEdited) setName(defaultName(kind, t));
  };

  const submit = async (e) => {
    e.preventDefault();
    if (!name.trim()) { alert("name is required"); return; }
    const r = await putJSON(`/v1/schedule/${encodeURIComponent(name.trim())}`,
      { kind, target, hour: Number(hour), minute: Number(minute), weekday, enabled: true });
    if (r.status === 422) { alert(await r.text()); return; }
    if (!r.ok) { alert("add failed"); return; }
    setNameEdited(false);
    onAdded();
  };

  const targets = kind === "ingest" ? INGEST_TARGETS : COMMAND_TARGETS;
  return html`
    <form class="schedadd" onSubmit=${submit}>
      <div class="satitle">Add schedule</div>
      <div class="sarow">
        <label>kind<select value=${kind} onChange=${(e) => changeKind(e.currentTarget.value)}>
          <option value="ingest">ingest</option>
          <option value="command">command</option>
        </select></label>
        <label>target<select value=${target} onChange=${(e) => changeTarget(e.currentTarget.value)}>
          ${targets.map((t) => html`<option key=${t} value=${t}>${t}</option>`)}
        </select></label>
        <label>hour<input type="number" min="0" max="23" value=${hour}
                          onInput=${(e) => setHour(e.currentTarget.value)} /></label>
        <label>min<input type="number" min="0" max="59" value=${minute}
                         onInput=${(e) => setMinute(e.currentTarget.value)} /></label>
        <label>day<select value=${wdToOpt(weekday)} onChange=${(e) => setWeekday(optToWd(e.currentTarget.value))}>
          ${WEEKDAYS.map(([v, t]) => html`<option key=${v} value=${v}>${t}</option>`)}
        </select></label>
      </div>
      <div class="sarow">
        <label class="saname">name<input type="text" value=${name}
              onInput=${(e) => { setName(e.currentTarget.value); setNameEdited(true); }} /></label>
        <button class="fbtn" type="submit">Add schedule</button>
      </div>
    </form>`;
}

export function ScheduledJobs() {
  const [rows, setRows] = useState([]);
  const load = useCallback(() => getJSON("/v1/schedule").then(setRows).catch(() => {}), []);
  usePoll(load, 15000);

  return html`
    <div id="schedList">
      ${rows.map((r) => html`<${ScheduleRow} key=${r.name} row=${r} onReload=${load} />`)}
    </div>
    <${AddSchedule} onAdded=${load} />`;
}
