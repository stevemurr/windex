"""Canonical upstream URLs for DevDocs pages — agents must link to OFFICIAL docs.

Two complementary mechanisms (verified live 2026-07-16):

1. **Per-page attribution link (primary).** DevDocs' attribution text filter
   appends ``<a href="<scraped url>" class="_attribution-link">`` to every page
   it scraped over HTTP (lib/docs/filters/core/attribution.rb) — the EXACT
   upstream URL with correct case, suffix, and host. This matters because
   DevDocs lowercases all page paths (normalize_paths.rb), which breaks
   reconstruction against case-sensitive upstreams (doc.rust-lang.org,
   react.dev, docs.ruby-lang.org, gnu.org). Locally-scraped docsets (go) carry
   no link, and a handful of file-scraped ones may carry an imperfect one —
   hence the guard + fallback.

2. **Maintained rule table (fallback).** ``{slug: (base_url, suffix_rule)}``
   with base URLs taken from DevDocs' open-source scraper definitions
   (lib/docs/scrapers/<name>.rb) — NOT the manifest ``links.home``. Rules group
   by scraper family:

   - ``"html"``  — sphinx/godoc-style static pages: append ``.html`` to the
     path (before any ``#anchor``). python, node, postgresql, …
   - ``"none"``  — path maps 1:1 to the URL: MDN family, git, cppreference
     (301s to its canonical short form), php (302s to the ``.php`` page).
   - ``"dir"``   — dirhtml-style sites where DevDocs' trailing-slash pages
     became ``…/index`` paths: strip the trailing ``index`` segment and keep
     the directory URL. flask, django, docker, kubernetes, go (pkg.go.dev).

A slug absent from the table (someone seeds beyond the default set) falls back
to the always-working DevDocs app URL rather than a guessed upstream.
"""

from urllib.parse import urlsplit

# slug -> (base_url, suffix_rule). Bases are version-pinned where upstream is
# (python, postgresql, ruby, django); refresh alongside the seed-list config
# when bumping a pinned slug. Case-sensitivity caveats (lowercased DevDocs
# paths vs. mixed-case upstream files) are rescued by the per-page attribution
# link for rust/react; bash and ruby pages are file-scraped, so their fallback
# URLs can 404 on mixed-case pages — known, documented limitation.
CANONICAL_RULES: dict[str, tuple[str, str]] = {
    "python~3.14": ("https://docs.python.org/3.14/", "html"),
    "javascript": ("https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference", "none"),
    "typescript": ("https://www.typescriptlang.org/", "html"),
    "node": ("https://nodejs.org/api/", "html"),
    "go": ("https://pkg.go.dev/", "dir"),
    "rust": ("https://doc.rust-lang.org/", "html"),
    "c": ("https://en.cppreference.com/w/c/", "none"),
    "react": ("https://react.dev/", "none"),
    "vue~3": ("https://vuejs.org/", "none"),
    "html": ("https://developer.mozilla.org/en-US/docs/Web/HTML", "none"),
    "css": ("https://developer.mozilla.org/en-US/docs/Web/CSS", "none"),
    "http": ("https://developer.mozilla.org/en-US/docs/Web/HTTP", "none"),
    "postgresql~18": ("https://www.postgresql.org/docs/18/", "html"),
    "git": ("https://git-scm.com/docs/", "none"),
    "bash": ("https://www.gnu.org/software/bash/manual/html_node/", "html"),
    "php": ("https://www.php.net/manual/en/", "none"),
    "ruby~3.4": ("https://docs.ruby-lang.org/en/3.4/", "html"),
    "django~6.1": ("https://docs.djangoproject.com/en/6.1/", "dir"),
    "flask": ("https://flask.palletsprojects.com/en/stable/", "dir"),
    "tailwindcss": ("https://tailwindcss.com/docs/", "none"),
    "docker": ("https://docs.docker.com/", "dir"),
    "kubernetes": ("https://kubernetes.io/docs/reference/kubernetes-api/", "dir"),
}


def usable_upstream_url(url: str | None) -> bool:
    """An attribution-link href we are willing to publish as canonical: http(s),
    a real public host (DevDocs scrapes some docsets from localhost mirrors)."""
    if not url:
        return False
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https"):
        return False
    host = parts.hostname or ""
    return "." in host and host != "127.0.0.1" and not host.endswith(".localhost")


def canonical_url(slug: str, path: str, upstream: str | None = None) -> str:
    """The official-docs URL for one DevDocs page (or index entry — entry paths
    carry real upstream ``#anchor``s, applied after the suffix rule)."""
    if usable_upstream_url(upstream):
        return upstream
    rule = CANONICAL_RULES.get(slug)
    if rule is None:
        # unknown slug: link to the DevDocs app itself — always resolves
        return f"https://devdocs.io/{slug}/{path}"
    base, suffix = rule
    path, _, anchor = path.partition("#")
    if suffix == "html" and path:
        path = f"{path}.html"
    elif suffix == "dir":
        if path == "index":
            path = ""
        elif path.endswith("/index"):
            path = path[: -len("index")]  # keep the directory's trailing slash
    url = base.rstrip("/") + "/" + path if path else base
    return f"{url}#{anchor}" if anchor else url
