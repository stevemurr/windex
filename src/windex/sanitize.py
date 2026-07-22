"""Strip invisible/smuggled Unicode from text before it is embedded.

Motivation (2026-07-20): a GitHub README used the Unicode **Tags** block
(U+E0000-U+E007F) to smuggle thousands of tokens' worth of ASCII into a few
thousand *visible* characters. The embed path bounds documents by CHARACTER
count (a crude `chars ~= 4*tokens` heuristic), so the doc slipped under the char
cap while blowing past the model's token context window - the server answered
400 and the gh embed loop retried the identical batch forever.

Stripping these code points before truncation makes char-count ~= token-count
hold again (the visible text is what remains), and as a bonus removes zero-width
/ bidi / control noise that pollutes both embeddings and snippets. This is a
correctness+hygiene filter, not fidelity-preserving - the same spirit as
github/clean.py.
"""

import re

# (start, end) inclusive ranges of invisible / format / control code points that
# have no place in indexable text. Built from integer code points (not literal
# chars or string escapes) so the source stays pure ASCII and unmangleable.
# Deliberately KEEPS \t (0x09), \n (0x0A) and \r (0x0D).
_RANGES = [
    (0x00, 0x08), (0x0B, 0x0C), (0x0E, 0x1F),   # C0 controls (minus \t \n \r)
    (0x7F, 0x9F),                                # DEL + C1 controls
    (0xAD, 0xAD),                                # soft hyphen
    (0x200B, 0x200F),                           # zero-width space/joiners + LRM/RLM
    (0x202A, 0x202E),                           # bidi embeddings / overrides
    (0x2060, 0x2064), (0x206A, 0x206F),         # word joiner, invisible math, deprecated
    (0xFEFF, 0xFEFF), (0xFFF9, 0xFFFB),         # BOM/ZWNBSP, interlinear annotation
    (0xFE00, 0xFE0F),                           # variation selectors
    (0xE0000, 0xE007F),                         # TAGS block - the ASCII-smuggling vector
    (0xE0100, 0xE01EF),                         # variation selectors supplement
]
_SMUGGLED = re.compile("[" + "".join(f"{chr(a)}-{chr(b)}" for a, b in _RANGES) + "]")


def strip_smuggled(text: str) -> str:
    """Remove invisible/smuggled code points. Safe on empty/None-ish input."""
    if not text:
        return text
    return _SMUGGLED.sub("", text)
