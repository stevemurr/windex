"""MCP front door: same service functions, same result objects as REST."""

from fastmcp import FastMCP

from windex.api import service
from windex.config import get_settings

mcp = FastMCP(
    "windex",
    instructions="Self-hosted web index: fresh news (CC-News), GitHub projects, "
    "Wikipedia articles, arXiv papers, Small Web personal blogs, and programming "
    "documentation (DevDocs). Use search_index to find links; get_document to "
    "read a result's full text.",
)


@mcp.tool
def search_index(
    query: str,
    source: str = "all",
    limit: int = 10,
    min_stars: int | None = None,
    published_after: str | None = None,
    category: str | None = None,
    outlet: str | None = None,
    framework: str | None = None,
) -> dict:
    """Search the index. source: news | github | wiki | arxiv | smallweb |
    docs | all. `category` filters arxiv results by primary category (e.g.
    cs.LG); `outlet` filters smallweb results by feed host (e.g. example.com);
    `framework` filters docs results by framework (e.g. python, react). Docs
    results link to the official documentation and carry the upstream version
    and license attribution. Returns ranked results with stable ids, URLs,
    titles, and snippets."""
    from datetime import datetime

    return service.run_search(
        get_settings(),
        query,
        source=source,
        limit=limit,
        min_stars=min_stars,
        published_after=datetime.fromisoformat(published_after) if published_after else None,
        category=category,
        outlet=outlet,
        framework=framework,
    )


@mcp.tool
def get_document(doc_id: str) -> dict:
    """Fetch the stored full text and metadata for a search result id
    (news:<hash>, gh:owner/repo, wiki:<page_id>, arxiv:<paper_id>,
    smallweb:<hash>, or docs:<slug>/<path>)."""
    doc = service.get_document(get_settings(), doc_id)
    return doc or {"error": f"unknown document id: {doc_id}"}


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
