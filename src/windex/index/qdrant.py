import re

from qdrant_client import QdrantClient
from qdrant_client import models as qm

DENSE = "dense"
SPARSE = "bm25"
SOURCES = ("news", "repos", "wiki", "arxiv", "smallweb", "docs", "hn", "hf")

# payload fields that search filters on, indexed at collection creation
PAYLOAD_INDEXES = {
    "news": {"doc_id": qm.PayloadSchemaType.KEYWORD,
             "published_at": qm.PayloadSchemaType.DATETIME,
             "lang": qm.PayloadSchemaType.KEYWORD},
    "repos": {"doc_id": qm.PayloadSchemaType.KEYWORD,
              "stars": qm.PayloadSchemaType.INTEGER,
              "language": qm.PayloadSchemaType.KEYWORD,
              "pushed_at": qm.PayloadSchemaType.DATETIME},
    "wiki": {"doc_id": qm.PayloadSchemaType.KEYWORD,
             "title": qm.PayloadSchemaType.KEYWORD,
             "published_at": qm.PayloadSchemaType.DATETIME,
             "incoming_links": qm.PayloadSchemaType.INTEGER},
    "arxiv": {"doc_id": qm.PayloadSchemaType.KEYWORD,
              "primary_category": qm.PayloadSchemaType.KEYWORD,
              "published_at": qm.PayloadSchemaType.DATETIME},
    "smallweb": {"doc_id": qm.PayloadSchemaType.KEYWORD,
                 "outlet": qm.PayloadSchemaType.KEYWORD,
                 "published_at": qm.PayloadSchemaType.DATETIME},
    "docs": {"doc_id": qm.PayloadSchemaType.KEYWORD,
             "framework": qm.PayloadSchemaType.KEYWORD,
             "version": qm.PayloadSchemaType.KEYWORD},
    "hn": {"doc_id": qm.PayloadSchemaType.KEYWORD,
           "points": qm.PayloadSchemaType.INTEGER,  # min_points filter + future ranking boost
           "published_at": qm.PayloadSchemaType.DATETIME},
    # HF docs, courses and blog share one collection; root/kind are what
    # separate them at query time. published_at is blog-only (reference pages
    # aren't dated) and simply absent from doc payloads.
    "hf": {"doc_id": qm.PayloadSchemaType.KEYWORD,
           "root": qm.PayloadSchemaType.KEYWORD,     # transformers | agents-course | blog
           "kind": qm.PayloadSchemaType.KEYWORD,     # docs | learn | blog
           "published_at": qm.PayloadSchemaType.DATETIME},
}


def slug(model_id: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", model_id.lower()).strip("-")


def collection_name(source: str, model_id: str) -> str:
    return f"{source}__{slug(model_id)}"


def alias_name(source: str) -> str:
    return f"{source}_current"


def client_from_url(url: str) -> QdrantClient:
    # 30s, not the client's 5s default: the int8 copies are mmap'd off the
    # external disk (always_ram=False below) and six embed loops' upserts keep
    # evicting them, so a COLD dense query measured 4.2-6.2s (2026-07-19) —
    # straddling the default and 500ing searches from a fresh serve process.
    # Warm queries are ~0s. Slow-but-answered beats a timeout; the Grafana
    # search-latency histogram now shows the cold tail directly.
    return QdrantClient(url=url, timeout=30)


def ensure_collection(client: QdrantClient, source: str, model_id: str, dim: int) -> str:
    """Create the per-model collection if missing and point the `<source>_current`
    alias at it if no alias exists yet. Model swap = create new collection,
    re-upsert from parquet, flip alias."""
    if dim <= 0:
        raise ValueError("embed_dim must be configured before creating collections")
    name = collection_name(source, model_id)
    if not client.collection_exists(name):
        client.create_collection(
            collection_name=name,
            on_disk_payload=True,  # snippets × millions of docs must not live in RAM
            vectors_config={
                DENSE: qm.VectorParams(size=dim, distance=qm.Distance.COSINE, on_disk=True)
            },
            sparse_vectors_config={
                SPARSE: qm.SparseVectorParams(modifier=qm.Modifier.IDF)
            },
            # always_ram=False: at backfill scale (~5.5M docs × 4096d) the int8
            # copies are ~22GB — mmap them from disk instead of fighting the
            # container's memory cap. Revisit (binary quant / MRL truncation)
            # if query latency matters more later.
            quantization_config=qm.ScalarQuantization(
                scalar=qm.ScalarQuantizationConfig(type=qm.ScalarType.INT8, always_ram=False)
            ),
        )
    # Only create what's missing: this runs on every embed_pending() pass across
    # 7 concurrent embed processes, and each call takes a collection lock that
    # competes with search (measured 2246 calls, ~45ms avg, 637ms max).
    existing_fields = set(client.get_collection(name).payload_schema or {})
    for field, schema in PAYLOAD_INDEXES.get(source, {}).items():
        if field not in existing_fields:
            client.create_payload_index(name, field_name=field, field_schema=schema)
    if not _alias_target(client, alias_name(source)):
        flip_alias(client, source, name)
    return name


def flip_alias(client: QdrantClient, source: str, target_collection: str) -> None:
    client.update_collection_aliases(
        change_aliases_operations=[
            qm.CreateAliasOperation(
                create_alias=qm.CreateAlias(
                    collection_name=target_collection, alias_name=alias_name(source)
                )
            )
        ]
    )


def _alias_target(client: QdrantClient, alias: str) -> str | None:
    for a in client.get_aliases().aliases:
        if a.alias_name == alias:
            return a.collection_name
    return None


def status(client: QdrantClient) -> dict:
    aliases = {a.alias_name: a.collection_name for a in client.get_aliases().aliases}
    out = {}
    for c in client.get_collections().collections:
        info = client.get_collection(c.name)
        out[c.name] = {"points": info.points_count, "status": str(info.status)}
    return {"collections": out, "aliases": aliases}
