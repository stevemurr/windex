"""README markdown → indexable text. Deliberately regex-based and lossy: the
goal is clean embedding/snippet text, not fidelity."""

import re

from windex.sanitize import strip_smuggled

_PATTERNS = [
    (re.compile(r"```.*?```", re.DOTALL), " "),          # fenced code blocks
    (re.compile(r"!\[[^\]]*\]\([^)]*\)"), " "),           # images / badges
    (re.compile(r"\[([^\]]*)\]\([^)]*\)"), r"\1"),        # links → anchor text
    (re.compile(r"<!--.*?-->", re.DOTALL), " "),          # html comments
    (re.compile(r"<[^>]+>"), " "),                        # html tags
    (re.compile(r"^\s{0,3}#{1,6}\s*", re.MULTILINE), ""),  # heading markers
    (re.compile(r"[*_`|>~]{1,3}"), " "),                  # md decoration
    (re.compile(r"^ {0,3}[-=]{3,}\s*$", re.MULTILINE), " "),  # rules
]
_WS = re.compile(r"[ \t]+")
_NL = re.compile(r"\n{3,}")


def clean_readme(md: str) -> str:
    # Strip smuggled/invisible code points first, so a Tags-block payload can't
    # survive markdown stripping and later blow the token budget (see sanitize).
    text = strip_smuggled(md)
    for pattern, repl in _PATTERNS:
        text = pattern.sub(repl, text)
    text = _WS.sub(" ", text)
    text = _NL.sub("\n\n", text)
    return text.strip()


def compose_doc(full_name: str, description: str | None, topics: list[str] | None,
                readme_text: str | None, max_chars: int) -> str:
    parts = [full_name.replace("/", " / ")]
    if description:
        parts.append(description)
    if topics:
        parts.append("Topics: " + ", ".join(topics))
    if readme_text:
        parts.append(readme_text)
    # Sanitize the whole composed doc (description/topics too) before the char
    # bound, so `chars ≈ tokens` holds and the truncation can't keep a payload.
    return strip_smuggled("\n\n".join(parts))[:max_chars]
