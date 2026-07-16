"""Hydrate candidate repos via the GitHub GraphQL API: metadata + README in
batched queries (~40 repos aliased per request). The true stargazerCount is
read here — archive star_events was only the candidate signal. READMEs land in
parquet (staging/repos/readme/) keyed by repo; documents rows appear later at
the clean/embed step."""

import json
import time
from pathlib import Path

import httpx
import psycopg
import pyarrow as pa
import pyarrow.parquet as pq

API = "https://api.github.com/graphql"
BATCH = 40
README_EXPRESSIONS = {
    "readme_md": "HEAD:README.md",
    "readme_lower": "HEAD:readme.md",
    "readme_rst": "HEAD:README.rst",
    "readme_plain": "HEAD:README",
}
MAX_README_BYTES = 200_000

README_SCHEMA = pa.schema(
    [("repo_id", pa.int64()), ("full_name", pa.string()), ("readme", pa.string())]
)

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
    f'\n  {alias}: object(expression: "{expr}") {{ ... on Blob {{ text }} }}'
    for alias, expr in README_EXPRESSIONS.items()
) + "\n}"


def _build_query(full_names: list[str]) -> str:
    parts = []
    for i, fn in enumerate(full_names):
        owner, name = fn.split("/", 1)
        parts.append(
            f'r{i}: repository(owner: {json.dumps(owner)}, name: {json.dumps(name)}) {{ ...repoFields }}'
        )
    return _REPO_FRAGMENT + "\nquery {\n" + "\n".join(parts) + "\n}"


class TokenPool:
    def __init__(self, tokens: list[str]):
        if not tokens:
            raise ValueError("no GitHub tokens configured (WINDEX_GITHUB_TOKENS)")
        self.tokens = tokens
        self.i = 0

    def next(self) -> str:
        tok = self.tokens[self.i % len(self.tokens)]
        self.i += 1
        return tok


def _extract_readme(node: dict) -> str | None:
    for alias in README_EXPRESSIONS:
        blob = node.get(alias)
        if blob and blob.get("text"):
            return blob["text"][:MAX_README_BYTES]
    return None


def candidates(conn: psycopg.Connection, limit: int, min_star_events: int = 1) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT full_name FROM repos
            WHERE status = 'candidate' AND star_events >= %s AND full_name NOT LIKE '%%#stale:%%'
            ORDER BY star_events DESC LIMIT %s
            """,
            (min_star_events, limit),
        )
        return [r[0] for r in cur.fetchall()]


def hydrate(
    conn: psycopg.Connection,
    tokens: list[str],
    readme_dir: Path,
    star_threshold: int,
    limit: int = 10_000,
    min_star_events: int = 1,
) -> dict:
    from windex import db as wdb

    pool = TokenPool(tokens)
    readme_dir.mkdir(parents=True, exist_ok=True)
    stats = {"hydrated": 0, "below_threshold": 0, "gone": 0, "readmes": 0}
    stage_ctx = wdb.stage(conn, "gh_stage", "hydrating repos (metadata + READMEs)")
    stage_ctx.__enter__()
    stamp = time.strftime("%Y%m%d-%H%M%S")
    writer: pq.ParquetWriter | None = None
    parquet_name = f"{stamp}.parquet"

    try:
        with httpx.Client(timeout=60) as client:
            while True:
                names = candidates(conn, min(BATCH, limit), min_star_events)
                if not names or limit <= 0:
                    break
                limit -= len(names)
                data = _post(client, pool, _build_query(names))
                readme_rows = []
                with conn.cursor() as cur:
                    for i, fn in enumerate(names):
                        node = (data.get("data") or {}).get(f"r{i}")
                        if not node or not node.get("databaseId"):
                            stats["gone"] += 1
                            cur.execute(
                                "UPDATE repos SET status = 'gone' WHERE full_name = %s", (fn,)
                            )
                            continue
                        stars = node["stargazerCount"]
                        status = "hydrated" if stars >= star_threshold and not node["isArchived"] else "below_threshold"
                        stats[status if status in stats else "hydrated"] += 1
                        topics = [
                            t["topic"]["name"]
                            for t in (node.get("repositoryTopics") or {}).get("nodes") or []
                        ]
                        cur.execute(
                            """
                            UPDATE repos SET
                                stars = %s, description = %s, topics = %s,
                                primary_language = %s, default_branch = %s,
                                pushed_at = %s, readme_fetched_at = now(), status = %s,
                                full_name = %s
                            WHERE repo_id = %s
                            """,
                            (
                                stars,
                                node.get("description"),
                                topics,
                                (node.get("primaryLanguage") or {}).get("name"),
                                (node.get("defaultBranchRef") or {}).get("name"),
                                node.get("pushedAt"),
                                status,
                                node["nameWithOwner"],
                                node["databaseId"],
                            ),
                        )
                        readme = _extract_readme(node) if status == "hydrated" else None
                        if readme:
                            readme_rows.append(
                                (node["databaseId"], node["nameWithOwner"], readme)
                            )
                conn.commit()
                if readme_rows:
                    if writer is None:
                        writer = pq.ParquetWriter(readme_dir / parquet_name, README_SCHEMA)
                    writer.write_batch(
                        pa.record_batch(
                            [
                                pa.array([r[0] for r in readme_rows], pa.int64()),
                                pa.array([r[1] for r in readme_rows]),
                                pa.array([r[2] for r in readme_rows]),
                            ],
                            schema=README_SCHEMA,
                        )
                    )
                    stats["readmes"] += len(readme_rows)
    finally:
        if writer is not None:
            writer.close()
        stage_ctx.__exit__(None, None, None)
    stats["readme_file"] = parquet_name if writer is not None else None
    return stats


def _post(client: httpx.Client, pool: TokenPool, query: str, retries: int = 5) -> dict:
    for attempt in range(retries):
        token = pool.next()
        resp = client.post(API, json={"query": query}, headers={"Authorization": f"bearer {token}"})
        if resp.status_code in (502, 503):
            time.sleep(2**attempt)
            continue
        if resp.status_code == 403 or resp.status_code == 429:
            wait = int(resp.headers.get("retry-after", 0)) or 2**attempt * 5
            time.sleep(min(wait, 120))
            continue
        resp.raise_for_status()
        body = resp.json()
        # partial errors (e.g. missing repos) are fine; data node is still present
        if body.get("data") is not None:
            return body
        time.sleep(2**attempt)
    raise RuntimeError("GraphQL request failed after retries")
