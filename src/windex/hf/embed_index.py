"""Embed staged Hugging Face pages from clean parquet and upsert into Qdrant.

The driver is shared (windex.embed.pipeline); this is the hf-specific part.
Source 'hf' lands in the "hf" collection behind the hf_current alias. Docs,
courses and blog posts share one collection: they are one site, one crawl and
one id namespace, and an agent asking "how do I use a pipeline" should not have
to know whether the answer lives in a doc page or a blog post. `kind` and `root`
ride the payload as filters for when it does matter.

CHUNKING — deliberately NOT done, and the cost is MEASURED, not assumed.
Crawling docs/smolagents live (22 pages) gives: min 2,780 / median 7,499 /
max 32,236 chars. Against production's bound (WINDEX_EMBED_MAX_TOKENS=2048 →
8,192 chars) **45% of pages truncate and ~55% of corpus characters get
embedded**; `main_classes/pipelines` (141k chars) would embed its first 6%.
That is a real loss. It is still the right call here, and the number is worth
carrying to whoever revisits it:

  * **This is the index's existing trade-off, not an HF quirk.** Wiki articles
    and DevDocs reference pages are longer still and already accept exactly the
    same truncation through the same driver. HF is not special enough to be the
    one source that chunks — that would fork the id contract for one source's
    benefit: a chunked page is N documents (`…/pipelines#3`), so
    `/v1/docs/hf:docs/transformers/…` would return a fragment and the
    "one id = one page = one canonical URL" invariant the API rests on would
    hold everywhere except here.
  * **It is unembedded, not lost.** The FULL page text is staged to parquet, per
    the standing constraint that text and vectors are persisted so a model swap
    is re-embed + alias flip, never a re-crawl. If windex adopts chunking later
    it is a re-embed from staged parquet across ALL sources — no re-crawl, and
    nothing done here is wasted.
  * **Chunking is an index-wide decision.** It changes ids, the document
    contract and dedup for every source. Making it unilaterally inside the
    eighth source is the wrong place to decide it — but the 45% above is the
    kind of datum that should drive that decision for the whole index.

What we do instead: the page's own `# ` heading leads the embedded text (the
driver composes title + body), so a long reference page is still findable by
what it IS even when its tail is unembedded — and it always links to the
canonical URL, where the tail is one click away.
"""

import psycopg

from windex.config import Settings
from windex.embed.pipeline import SourceSpec, embed_pending as _embed_pending

SNIPPET_CHARS = 400
LICENSE_CHARS = 200

SPEC = SourceSpec(
    source="hf",
    collection="hf",
    columns=("id", "url", "title", "kind", "root", "version", "license",
             "published_at", "text"),
    text_field="text",
    payload=lambda r: {
        "url": r["url"],
        "title": r["title"],
        "snippet": (r["text"] or "")[:SNIPPET_CHARS],
        "kind": r["kind"],            # docs | learn | blog
        "root": r["root"],            # transformers | agents-course | blog
        "version": r["version"],      # recorded, never in the id
        # Reuses the existing payload/RESULT_FIELDS key the DevDocs source
        # introduced for the same purpose: whose licence this text is under.
        "attribution": (r["license"] or "")[:LICENSE_CHARS],
        # Blog posts are dated; reference pages are not. Omitted rather than
        # null so the datetime-indexed field only ever sees real timestamps.
        **({"published_at": r["published_at"]} if r["published_at"] else {}),
    },
)


def embed_pending(conn: psycopg.Connection, settings: Settings, limit: int = 100_000) -> int:
    return _embed_pending(conn, settings, SPEC, limit)
