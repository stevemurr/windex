# windex application image — the STEADY-STATE serve + embed-loops (rootless podman).
# The one-time data-migration CLI (init-db, ensure-collections, pg_restore) still runs
# via uv-on-host; everything long-running ends up in containers built from this image.
#
# Build:  podman build -t localhost/windex-app:latest -f Containerfile .
FROM docker.io/library/python:3.12-slim

# uv for fast, reproducible installs (pinned copy from the official image).
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Runtime libs a few pipeline deps load (lxml/trafilatura → libxml2; OpenMP for
# fasttext; libmagic for datatrove's WARC reader → ccnews extraction).
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates libgomp1 libmagic1 \
    && rm -rf /var/lib/apt/lists/*

# Manifests first for layer caching, then source.
COPY pyproject.toml README.md ./
COPY src ./src

# base + pipeline (embed-loops / CLI) + api (serve = uvicorn/fastapi/prometheus-client).
RUN uv sync --extra pipeline --extra api

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1

# Subcommand supplied per-service in compose (serve / embed-loop <source>).
ENTRYPOINT ["windex"]
