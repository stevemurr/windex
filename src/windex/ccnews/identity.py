"""Pure document identity and exact-duplicate helpers."""

import hashlib
import re
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

_TRACKING_PARAMS = re.compile(
    r"^(utm_|fbclid|gclid|mc_cid|mc_eid|ref$)"
)
_WS = re.compile(r"\s+")


def canonical_url(url: str) -> str:
    parts = urlsplit(url.strip())
    query = [
        (key, value)
        for key, value in parse_qsl(parts.query)
        if not _TRACKING_PARAMS.match(key.lower())
    ]
    return urlunsplit((
        parts.scheme.lower() or "https",
        parts.netloc.lower(),
        parts.path.rstrip("/") or "/",
        urlencode(query),
        "",
    ))


def doc_id(canonical: str) -> str:
    return "news:" + hashlib.sha1(canonical.encode()).hexdigest()[:20]


def text_hash(text: str) -> str:
    normalized = _WS.sub(" ", text.lower()).strip()
    return hashlib.sha1(normalized.encode()).hexdigest()


def resolve_exact_duplicates(
    candidates: list[tuple[str, str]],
    existing_hashes: dict[str, str],
) -> dict[str, str | None]:
    """Map candidate ids to their existing/first-seen canonical document."""
    seen: dict[str, str] = {}
    resolved: dict[str, str | None] = {}
    for identifier, digest in candidates:
        canonical = existing_hashes.get(digest) or seen.get(digest)
        resolved[identifier] = canonical
        if canonical is None:
            seen[digest] = identifier
    return resolved
