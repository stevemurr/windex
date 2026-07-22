// Shared frontend helpers for the Preact console (no build, vendored).
// Every component module imports from here.
import { h } from "preact";
import { useEffect, useRef } from "preact/hooks";
import htm from "htm";

// htm bound to Preact's h — write markup as html`<div>…</div>`, no JSX build.
export const html = htm.bind(h);

// --- fetch helpers ---
export const getJSON = (path) => fetch(path).then((r) => r.json());
export const post = (path, body) =>
  fetch(path, {
    method: "POST",
    headers: body !== undefined ? { "Content-Type": "application/json" } : {},
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });

// Poll `fn` immediately and every `ms`; cleans up on unmount. `fn` may change
// between renders without restarting the interval.
export function usePoll(fn, ms) {
  const saved = useRef(fn);
  saved.current = fn;
  useEffect(() => {
    let alive = true;
    const tick = () => alive && saved.current();
    tick();
    const id = setInterval(tick, ms);
    return () => { alive = false; clearInterval(id); };
  }, [ms]);
}

// relative time from a unix timestamp (seconds)
export function agoTs(ts) {
  const s = Math.max(0, Date.now() / 1000 - ts);
  if (s < 60) return "just now";
  if (s < 3600) return `${Math.round(s / 60)}m ago`;
  if (s < 86400) return `${Math.round(s / 3600)}h ago`;
  return `${Math.round(s / 86400)}d ago`;
}

// HTML-escape (for interpolating text into markup where needed)
export const esc = (s) => {
  const d = document.createElement("div");
  d.textContent = s ?? "";
  return d.innerHTML;
};

export const num = (n) => (n ?? 0).toLocaleString();

// Decoupled trigger so any section can pop the activity dock open to a specific
// log (e.g. Freshness "Update", Scheduled "Run"). The dock listens for this
// event, so no cross-component prop-drilling. name = /v1/logs/{name} key.
export const openDockLog = (name, label) =>
  window.dispatchEvent(new CustomEvent("windex:opendock", { detail: { name, label } }));
