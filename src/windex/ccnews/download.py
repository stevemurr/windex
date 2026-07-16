import concurrent.futures as cf
from pathlib import Path

import httpx

from windex.ccnews.sync import DATA_URL


def download_one(client: httpx.Client, path: str, dest_dir: Path, retries: int = 3) -> Path:
    dest = dest_dir / Path(path).name
    if dest.exists() and dest.stat().st_size > 0:
        return dest
    part = dest.with_suffix(dest.suffix + ".part")
    url = DATA_URL.format(path=path)
    last: Exception | None = None
    for _ in range(retries):
        try:
            with client.stream("GET", url) as resp:
                resp.raise_for_status()
                with open(part, "wb") as f:
                    for chunk in resp.iter_bytes(1 << 20):
                        f.write(chunk)
            part.rename(dest)
            return dest
        except httpx.HTTPError as exc:
            last = exc
            part.unlink(missing_ok=True)
    raise RuntimeError(f"download failed: {path}") from last


def download_batch(paths: list[str], dest_dir: Path, concurrency: int = 4) -> list[Path]:
    dest_dir.mkdir(parents=True, exist_ok=True)
    with httpx.Client(timeout=httpx.Timeout(30, read=120), follow_redirects=True) as client:
        with cf.ThreadPoolExecutor(concurrency) as pool:
            return list(pool.map(lambda p: download_one(client, p, dest_dir), paths))
