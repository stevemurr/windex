"""Pure arXiv OAI-PMH record parsing.

The epoch-2 pipeline owns pagination, retries, staging, and indexing.  This
module contains only the upstream protocol shape shared by its fetch and
extract Modules.
"""

import xml.etree.ElementTree as ET

OAI_NS = "http://www.openarchives.org/OAI/2.0/"
ARXIV_NS = "http://arxiv.org/OAI/arXiv/"
_IDENTIFIER_PREFIX = "oai:arxiv.org:"


class OAIError(RuntimeError):
    """A non-recoverable OAI-PMH protocol error."""

    def __init__(self, code: str, message: str | None):
        self.code = code
        super().__init__(f"{code}: {message}")


def _q(namespace: str, tag: str) -> str:
    return f"{{{namespace}}}{tag}"


def paper_id(identifier: str) -> str:
    """Return the arXiv id from an OAI identifier."""
    value = identifier.strip()
    if value.lower().startswith(_IDENTIFIER_PREFIX):
        return value[len(_IDENTIFIER_PREFIX):]
    return value


def abs_url(identifier: str) -> str:
    return f"https://arxiv.org/abs/{identifier}"


def _text(element, namespace: str, tag: str, default: str = "") -> str:
    value = element.findtext(_q(namespace, tag))
    return value.strip() if value else default


def _collapse(value: str) -> str:
    return " ".join(value.split())


def _parse_authors(metadata) -> list[str]:
    authors: list[str] = []
    container = metadata.find(_q(ARXIV_NS, "authors"))
    if container is None:
        return authors
    for author in container.findall(_q(ARXIV_NS, "author")):
        keyname = _text(author, ARXIV_NS, "keyname")
        forenames = _text(author, ARXIV_NS, "forenames")
        suffix = _text(author, ARXIV_NS, "suffix")
        name = " ".join(part for part in (forenames, keyname, suffix) if part)
        if name:
            authors.append(name)
    return authors


def _parse_record(record) -> dict | None:
    header = record.find(_q(OAI_NS, "header"))
    if header is None:
        return None
    identifier = paper_id(_text(header, OAI_NS, "identifier"))
    if not identifier:
        return None
    if header.get("status") == "deleted":
        return {"id": identifier, "deleted": True}
    metadata = record.find(_q(OAI_NS, "metadata"))
    arxiv = metadata.find(_q(ARXIV_NS, "arXiv")) if metadata is not None else None
    if arxiv is None:
        return None
    identifier = _text(arxiv, ARXIV_NS, "id") or identifier
    categories = _text(arxiv, ARXIV_NS, "categories").split()
    return {
        "id": identifier,
        "deleted": False,
        "created": _text(arxiv, ARXIV_NS, "created"),
        "updated": _text(arxiv, ARXIV_NS, "updated") or None,
        "title": _collapse(_text(arxiv, ARXIV_NS, "title")),
        "abstract": _collapse(_text(arxiv, ARXIV_NS, "abstract")),
        "authors": _parse_authors(arxiv),
        "categories": categories,
        "primary_category": categories[0] if categories else "",
        "doi": _text(arxiv, ARXIV_NS, "doi") or None,
        "journal_ref": _text(arxiv, ARXIV_NS, "journal-ref") or None,
        "license": _text(arxiv, ARXIV_NS, "license") or None,
    }


def parse_records(xml_bytes: bytes) -> tuple[list[dict], str | None]:
    """Parse one ``ListRecords`` page into records and a resumption token."""
    root = ET.fromstring(xml_bytes)
    error = root.find(_q(OAI_NS, "error"))
    if error is not None:
        if error.get("code") == "noRecordsMatch":
            return [], None
        raise OAIError(error.get("code") or "unknown", (error.text or "").strip())
    listing = root.find(_q(OAI_NS, "ListRecords"))
    if listing is None:
        return [], None
    records = [
        parsed
        for record in listing.findall(_q(OAI_NS, "record"))
        if (parsed := _parse_record(record))
    ]
    token_element = listing.find(_q(OAI_NS, "resumptionToken"))
    token = (
        token_element.text.strip()
        if token_element is not None and token_element.text
        and token_element.text.strip()
        else None
    )
    return records, token
