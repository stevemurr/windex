"""Discover complete Wikimedia CirrusSearch snapshots."""

import re

import httpx

ROOT_URL = "https://dumps.wikimedia.org/other/cirrus_search_index/"
CONTENT_DIR_URL = ROOT_URL + "{date}/index_name={wiki}_content/"
SUCCESS_MARKER = "_SUCCESS"


def content_dir_url(date: str, wiki: str) -> str:
    return CONTENT_DIR_URL.format(date=date, wiki=wiki)


def shard_url(date: str, name: str, wiki: str) -> str:
    return content_dir_url(date, wiki) + name


def list_dates(client: httpx.Client) -> list[str]:
    """Return available snapshot dates, newest first."""
    response = client.get(ROOT_URL)
    response.raise_for_status()
    return sorted(
        set(re.findall(r'href="(\d{8})/"', response.text)),
        reverse=True,
    )


def list_content_dir(
    client: httpx.Client,
    date: str,
    wiki: str,
) -> tuple[bool, list[tuple[str, int]]]:
    """Return the completion marker and shard files for one snapshot."""
    response = client.get(content_dir_url(date, wiki))
    if response.status_code == 404:
        return False, []
    response.raise_for_status()
    complete = f'href="{SUCCESS_MARKER}"' in response.text
    pattern = re.compile(
        r'href="(' + re.escape(f"{wiki}_content-{date}-")
        + r'\d{5}\.json\.bz2)">[^<]*</a>\s+\S+\s+\S+\s+(\d+)'
    )
    files = [
        (match.group(1), int(match.group(2)))
        for match in pattern.finditer(response.text)
    ]
    return complete, sorted(files)


def latest_complete(
    client: httpx.Client,
    wiki: str,
) -> tuple[str | None, list[tuple[str, int]]]:
    """Return the newest complete snapshot and its shard list."""
    for date in list_dates(client):
        complete, files = list_content_dir(client, date, wiki)
        if complete and files:
            return date, files
    return None, []
