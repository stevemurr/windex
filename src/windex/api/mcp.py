"""MCP front door: same service functions, same result objects as REST."""

from fastmcp import FastMCP

from windex.api import service
from windex.config import get_settings

mcp = FastMCP(
    "windex",
    instructions="Self-hosted web index: fresh news (CC-News), GitHub projects, "
    "Wikipedia articles, arXiv papers, Small Web personal blogs, programming "
    "documentation (DevDocs), Hacker News stories, and Hugging Face docs, "
    "courses and blog. Use search_index to find links; get_document to read a "
    "result's full text.",
)


@mcp.tool
def search_index(
    query: str,
    source: str = "all",
    limit: int = 10,
    min_stars: int | None = None,
    min_points: int | None = None,
    published_after: str | None = None,
    category: str | None = None,
    outlet: str | None = None,
    framework: str | None = None,
    root: str | None = None,
    kind: str | None = None,
    conversation_id: str | None = None,
) -> dict:
    """Search the index. source: news | github | wiki | arxiv | smallweb |
    docs | hn | hf | memory | all (memory = the user's pushed chat history; it is
    NOT included in 'all' and must be asked for explicitly). `conversation_id`
    scopes a source=memory search to one conversation. `category` filters arxiv
    results by primary category
    (e.g. cs.LG); `outlet` filters smallweb results by feed host (e.g.
    example.com); `framework` filters docs results by framework (e.g. python,
    react); `min_points` filters hn results by score (mirrors github's
    `min_stars`); `root` and `kind` filter hf results by doc root (e.g.
    transformers, diffusers, agents-course) and by page kind (docs | learn |
    blog). Docs results link to the official documentation and carry the
    upstream version and license attribution; hn results link to the HN
    discussion page and carry the external link as `target_url`; hf results are
    the canonical huggingface.co docs, courses and blog. Returns ranked results
    with stable ids, URLs, titles, and snippets."""
    from datetime import datetime

    return service.run_search(
        get_settings(),
        query,
        source=source,
        limit=limit,
        min_stars=min_stars,
        min_points=min_points,
        published_after=datetime.fromisoformat(published_after) if published_after else None,
        category=category,
        outlet=outlet,
        framework=framework,
        root=root,
        kind=kind,
        conversation_id=conversation_id,
    )


@mcp.tool
def get_document(doc_id: str) -> dict:
    """Fetch the stored full text and metadata for a search result id
    (news:<hash>, gh:owner/repo, wiki:<page_id>, arxiv:<paper_id>,
    smallweb:<hash>, docs:<slug>/<path>, hn:<item_id>,
    hf:docs/<root>/<path> | hf:blog/<slug>, or memory:<conversation_id>/<chunk_index>)."""
    doc = service.get_document(get_settings(), doc_id)
    return doc or {"error": f"unknown document id: {doc_id}"}


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
