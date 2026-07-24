// Sources tab — runtime-editable settings for every source.
//
// The form is GENERATED from the server's schema (`GET /v1/settings`), never
// hardcoded here: the allowlist in settings_schema.py is the security boundary,
// and a hand-written form would drift from it — showing a field that is no
// longer editable, or (worse) implying one is when the server will refuse it.
//
// Every value carries an origin badge (default | env | edited). That is not
// decoration: once a key is overridden, its .env value is ignored, and without
// the badge a later "why is this ignoring my .env?" has no visible answer.
import { html, getJSON, num } from "../lib.js";
import { useState, useEffect, useCallback } from "preact/hooks";

const ORIGIN_LABEL = { db: "edited", env: ".env", default: "default" };

// windex carries two vocabularies for the same two sources: the LOOP/CLI name
// (used by /v1/loops, the control flags and the settings scopes) and the CORPUS
// name (used by documents.source and /v1/stats). Only these two differ; without
// the map their document counts silently render blank.
const CORPUS_NAME = { ccnews: "news", gh: "github" };

// /v1/stats.documents is {source: {status: count}}. "Indexed" means live docs —
// anything not tombstoned.
const liveDocs = (documents, scope) => {
  const byStatus = (documents || {})[CORPUS_NAME[scope] || scope];
  if (!byStatus) return undefined;
  return Object.entries(byStatus)
    .filter(([status]) => status !== "deleted")
    .reduce((sum, [, n]) => sum + n, 0);
};

function Field({ scope, field, token, onChanged }) {
  const [draft, setDraft] = useState(field.value ?? "");
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState("");
  useEffect(() => { setDraft(field.value ?? ""); }, [field.value]);

  const dirty = String(draft) !== String(field.value ?? "");

  const send = async (method, path, body) => {
    setBusy(true); setErr("");
    try {
      const r = await fetch(path, {
        method,
        headers: {
          "Content-Type": "application/json",
          ...(token ? { Authorization: `Bearer ${token}` } : {}),
        },
        body: body === undefined ? undefined : JSON.stringify(body),
      });
      const data = await r.json().catch(() => null);
      if (!r.ok) throw new Error(r.status === 401 ? "write token missing or invalid"
                                                  : (data && data.detail) || `HTTP ${r.status}`);
      onChanged(data);
    } catch (e) { setErr(e.message); }
    setBusy(false);
  };

  const save = () => {
    // Numbers go over the wire as numbers so the server's clamp sees a number,
    // not a string it would reject as the wrong type.
    const v = (field.kind === "int" || field.kind === "float") ? Number(draft) : draft;
    send("PATCH", `/v1/settings/${scope}`, { values: { [field.key]: v } });
  };
  const revert = () => send("DELETE", `/v1/settings/${scope}/${field.key}`);

  const bounds = (field.lo !== null && field.hi !== null) ? `${field.lo} – ${field.hi}` : "";
  return html`
    <div class="cw-set">
      <div class="cw-set-head">
        <label title=${field.key}>${field.label}</label>
        <span class="cw-tag ${field.origin === "db" ? "staged" : "skipped"}">
          ${ORIGIN_LABEL[field.origin] || field.origin}
        </span>
      </div>
      ${field.kind === "choice"
        ? html`<select value=${draft} onChange=${(e) => setDraft(e.target.value)}>
            ${field.choices.map((c) => html`<option value=${c}>${c}</option>`)}
          </select>`
        : html`<input type=${field.kind === "int" || field.kind === "float" ? "number" : "text"}
                 step=${field.kind === "float" ? "0.1" : "1"}
                 value=${draft} onInput=${(e) => setDraft(e.target.value)} />`}
      <div class="cw-set-foot">
        <span class="cw-set-help">${field.help}${bounds ? ` (${bounds})` : ""}</span>
        <span>
          ${dirty && html`<button class="cw-btn" disabled=${busy} onClick=${save}>Save</button>`}
          ${field.origin === "db" && html`
            <button class="cw-btn" disabled=${busy} onClick=${revert}>Revert</button>`}
        </span>
      </div>
      ${err && html`<div class="cw-note is-err" style="margin-top:6px">${err}</div>`}
    </div>`;
}

function ScopeCard({ scope, loop, counts, token, onChanged }) {
  const [busy, setBusy] = useState(false);
  const isGlobal = scope.scope === "_global";

  const act = async (path, body) => {
    setBusy(true);
    await fetch(path, {
      method: "POST",
      headers: { "Content-Type": "application/json",
                 ...(token ? { Authorization: `Bearer ${token}` } : {}) },
      body: body === undefined ? undefined : JSON.stringify(body),
    }).catch(() => {});
    setBusy(false);
  };

  const edited = scope.fields.filter((f) => f.origin === "db").length;
  return html`
    <div class="cw-card">
      <h2>
        ${isGlobal ? "Global (embed + crawl defaults)" : scope.scope}
        ${edited > 0 && html`<span class="cw-tag staged" style="margin-left:8px">${edited} edited</span>`}
      </h2>
      ${!isGlobal && html`
        <p class="cw-hint">
          ${counts !== undefined ? `${num(counts)} documents indexed. ` : ""}
          ${loop ? `Embed loop ${loop.state}; ingest ${loop.ingest_enabled ? "on" : "off"}.` : ""}
        </p>
        <div class="cw-btns" style="margin-top:0;margin-bottom:12px">
          <button class="cw-btn" disabled=${busy}
                  onClick=${() => act("/v1/system/refresh", { sources: [scope.scope] })}>
            Run ingest now
          </button>
          ${loop && html`
            <button class="cw-btn" disabled=${busy}
                    onClick=${() => act(`/v1/ingest/${scope.scope}`, { enabled: !loop.ingest_enabled })}>
              ${loop.ingest_enabled ? "Disable ingest" : "Enable ingest"}
            </button>`}
        </div>`}
      ${isGlobal && html`<p class="cw-hint">
        Applies to every source. Changes are picked up by running loops within
        ~30s — no restart, no redeploy.
      </p>`}
      <div class="cw-sets">
        ${scope.fields.map((f) => html`
          <${Field} key=${f.key} scope=${scope.scope} field=${f} token=${token}
                    onChanged=${onChanged} />`)}
      </div>
    </div>`;
}

export function SourcesTab({ token }) {
  const [scopes, setScopes] = useState([]);
  const [loops, setLoops] = useState({});
  const [counts, setCounts] = useState({});
  const [err, setErr] = useState("");

  const load = useCallback(() => {
    getJSON("/v1/settings")
      .then((d) => setScopes(d.scopes || []))
      .catch((e) => setErr(String(e)));
    getJSON("/v1/loops")
      .then((d) => setLoops(Object.fromEntries((d.loops || []).map((l) => [l.source, l]))))
      .catch(() => {});
    getJSON("/v1/stats")
      .then((d) => setCounts(d.documents || {}))
      .catch(() => {});
  }, []);
  useEffect(() => { load(); }, [load]);

  // A PATCH/DELETE returns the whole updated scope, so splice it in rather than
  // refetching everything — the form keeps focus and the badge flips instantly.
  const onChanged = (updated) => {
    if (!updated || !updated.scope) return;
    setScopes((prev) => prev.map((s) => (s.scope === updated.scope ? updated : s)));
  };

  if (err) return html`<div class="cw-note is-err">${err}</div>`;
  return html`
    <div>
      <p class="cw-hint" style="margin:0 0 16px">
        Settings are stored in the database and applied at run time. Anything not
        edited here still comes from <code>.env</code>. Secrets, connection strings
        and the embedding model are deliberately not editable.
      </p>
      ${scopes.map((s) => html`
        <${ScopeCard} key=${s.scope} scope=${s} loop=${loops[s.scope]}
                      counts=${liveDocs(counts, s.scope)} token=${token}
                      onChanged=${onChanged} />`)}
    </div>`;
}
