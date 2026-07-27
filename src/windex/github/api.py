"""GitHub REST/GraphQL request helpers used by epoch-2 fetch Modules."""

import json
import logging
import time

import httpx

log = logging.getLogger("windex.github.api")

SEARCH = "https://api.github.com/search/repositories"
GRAPHQL = "https://api.github.com/graphql"
RETRY_BUDGET = 30 * 60
README_EXPRESSIONS = {
    "readme_md": "HEAD:README.md",
    "readme_lower": "HEAD:readme.md",
    "readme_rst": "HEAD:README.rst",
    "readme_plain": "HEAD:README",
}
_REPO_FRAGMENT = """
fragment repoFields on Repository {
  databaseId
  nameWithOwner
  description
  stargazerCount
  pushedAt
  isArchived
  primaryLanguage { name }
  defaultBranchRef { name }
  repositoryTopics(first: 10) { nodes { topic { name } } }
""" + "".join(
    f'\n  {alias}: object(expression: "{expression}") {{ ... on Blob {{ text }} }}'
    for alias, expression in README_EXPRESSIONS.items()
) + "\n}"


def search_get(
    client: httpx.Client,
    token: str,
    params: dict,
    budget: float = RETRY_BUDGET,
) -> dict:
    """Issue a rate-limit-aware GitHub repository search request."""
    waited = 0.0
    secondary_hits = 0
    attempt = 0
    while True:
        try:
            response = client.get(
                SEARCH,
                params=params,
                headers={
                    "Authorization": f"bearer {token}",
                    "Accept": "application/vnd.github+json",
                },
            )
        except httpx.HTTPError as exc:
            wait = min(2**attempt * 5, 300)
            if waited + wait > budget:
                raise RuntimeError(
                    "search request failed after "
                    f"{waited:.0f}s of retry waiting "
                    f"(transport error: {exc!r})"
                ) from exc
            log.warning(
                "github search transport error (attempt %d): %r; waiting %.0fs",
                attempt,
                exc,
                wait,
            )
        else:
            if response.status_code in (403, 429):
                retry_after = int(response.headers.get("retry-after", 0) or 0)
                remaining = response.headers.get("x-ratelimit-remaining")
                reset = int(response.headers.get("x-ratelimit-reset", 0) or 0)
                if remaining == "0":
                    wait = max(reset - time.time(), 0) + 1
                else:
                    wait = min(max(retry_after, 60) * (2**secondary_hits), 900)
                    secondary_hits += 1
                log.warning(
                    "github search %d (attempt %d); waiting %.0fs",
                    response.status_code,
                    attempt,
                    wait,
                )
            elif response.status_code >= 500:
                wait = min(2**attempt * 5, 300)
            else:
                response.raise_for_status()
                return response.json()
            if waited + wait > budget:
                raise RuntimeError(
                    "search request failed after "
                    f"{waited:.0f}s of retry waiting "
                    f"(last status {response.status_code})"
                )
        time.sleep(wait)
        waited += wait
        attempt += 1


def build_graphql_query(full_names: list[str]) -> str:
    repositories = []
    for index, full_name in enumerate(full_names):
        owner, name = full_name.split("/", 1)
        repositories.append(
            f'r{index}: repository(owner: {json.dumps(owner)}, '
            f'name: {json.dumps(name)}) {{ ...repoFields }}'
        )
    return (
        _REPO_FRAGMENT
        + "\nquery {\n"
        + "\n".join(repositories)
        + "\n}"
    )


class TokenPool:
    def __init__(self, tokens: list[str]):
        if not tokens:
            raise ValueError(
                "no GitHub tokens configured (WINDEX_GITHUB_TOKENS)"
            )
        self.tokens = tokens
        self.index = 0

    def next(self) -> str:
        token = self.tokens[self.index % len(self.tokens)]
        self.index += 1
        return token


def graphql_post(
    client: httpx.Client,
    pool: TokenPool,
    query: str,
    retries: int = 5,
) -> dict:
    """POST a GraphQL batch, rotating tokens and retrying transient errors."""
    for attempt in range(retries):
        token = pool.next()
        try:
            response = client.post(
                GRAPHQL,
                json={"query": query},
                headers={"Authorization": f"bearer {token}"},
            )
        except httpx.HTTPError:
            time.sleep(2**attempt)
            continue
        if response.status_code in (502, 503, 504):
            time.sleep(2**attempt)
            continue
        if response.status_code in (403, 429):
            wait = int(response.headers.get("retry-after", 0))
            time.sleep(min(wait or 2**attempt * 5, 120))
            continue
        response.raise_for_status()
        body = response.json()
        if body.get("data") is not None:
            return body
        time.sleep(2**attempt)
    raise RuntimeError("GraphQL request failed after retries")
