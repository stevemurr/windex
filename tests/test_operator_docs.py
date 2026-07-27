"""Primary operator docs must describe the contract-epoch 2 runtime."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).parents[1]
PRIMARY = (
    ROOT / "README.md",
    ROOT / "docs" / "operations.md",
    ROOT / "docs" / "custom-sources.md",
)

REMOVED_SURFACES = (
    "windex up",
    "windex down",
    "windex status",
    "embed-loop",
    "/v1/jobs",
    "/v1/control",
    "/v1/throttle",
    "/v1/stats",
    "/v1/logs",
    "/v1/crawl",
    "/v1/sources/{name}/docs",
    "Apple `container`",
    "launchd",
)


def test_primary_operator_docs_exclude_removed_surfaces():
    contents = "\n".join(path.read_text() for path in PRIMARY)
    for removed in REMOVED_SURFACES:
        assert removed not in contents


def test_primary_operator_docs_name_current_runtime_surfaces():
    contents = "\n".join(path.read_text() for path in PRIMARY)
    for current in (
        "podman-compose",
        "/admin/v1/health",
        "/admin/v1/sources/{name}/runs",
        "/v1/sources/{name}/ingest",
        "/admin/v1/module-health",
        "windex-source-scheduler",
        "windex-worker",
    ):
        assert current in contents


def test_primary_operator_doc_local_links_resolve():
    link = re.compile(r"\[[^]]+]\(([^)]+)\)")
    missing: list[str] = []
    for document in PRIMARY:
        for target in link.findall(document.read_text()):
            if (
                target.startswith(("http://", "https://", "mailto:", "#"))
                or "{" in target
            ):
                continue
            relative = target.split("#", 1)[0]
            if relative and not (document.parent / relative).resolve().exists():
                missing.append(f"{document.relative_to(ROOT)} -> {target}")
    assert not missing, "\n".join(missing)


def test_documented_commands_and_routes_exist_in_current_sources():
    cli = (ROOT / "src" / "windex" / "cli.py").read_text()
    api = (ROOT / "src" / "windex" / "api" / "canonical.py").read_text()
    app = (ROOT / "src" / "windex" / "api" / "app.py").read_text()
    compose = (ROOT / "compose.yaml").read_text()

    assert '@app.command("init-db")' in cli
    assert "@app.command()\ndef health(" in cli
    assert '"/sources/{name}/runs"' in api
    assert '"/sources/{name}/ingest"' in api
    assert '"/module-health"' in api
    assert '"/v1/health"' in app
    for service in (
        "windex-serve:",
        "windex-source-scheduler:",
        "windex-worker:",
        "windex-module-sandbox:",
    ):
        assert service in compose


def test_run_and_publication_examples_are_scoped_and_unambiguous():
    operations = (ROOT / "docs" / "operations.md").read_text()
    assert "/log-events/stream?run_id=$run" in operations
    assert "/events/stream?after=0" not in operations
    assert operations.count('-d "$body"') == 1
    assert "Identity errors are synchronous HTTP 422" not in operations
