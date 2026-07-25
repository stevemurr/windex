"""Contract-epoch 2 operational CLI."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

import typer
from rich.console import Console

from windex import db
from windex.config import get_settings

app = typer.Typer(
    no_args_is_help=True,
    help="windex — Pipeline and Source runtime",
)
console = Console()


def _ensure_generation(bootstrap_id: str) -> Path:
    settings = get_settings()
    generations = settings.data_root / "generations"
    target = generations / bootstrap_id
    current = generations / "current"
    target.mkdir(parents=True, exist_ok=True)
    for child in ("artifacts", "downloads", "staging"):
        target.joinpath(child).mkdir(exist_ok=True)
    if current.exists() and not current.is_symlink():
        raise RuntimeError(f"generation selector is not a symlink: {current}")
    if current.is_symlink() and current.resolve() != target.resolve():
        raise RuntimeError(
            "active filesystem generation does not match database bootstrap "
            f"{bootstrap_id!r}; run the reviewed cutover/resume procedure")
    if not current.exists():
        temporary = generations / f".current-{bootstrap_id}"
        temporary.symlink_to(target.name)
        os.replace(temporary, current)
    return target


@app.command("init-db")
def init_db() -> None:
    """Initialize an empty canonical database or verify the current epoch."""
    settings = get_settings()
    with db.connect(settings.pg_dsn) as conn:
        metadata = db.init_db(conn)
    generation = _ensure_generation(metadata["bootstrap_id"])
    console.print(
        f"[green]canonical schema ready[/green] epoch={metadata['contract_epoch']} "
        f"generation={generation}")


@app.command()
def serve(host: str = "127.0.0.1", port: int = 8100) -> None:
    """Run the public and authenticated admin HTTP applications."""
    import uvicorn

    uvicorn.run("windex.api.app:app", host=host, port=port)


@app.command("serve-mcp")
def serve_mcp() -> None:
    """Run the search MCP server over stdio."""
    from windex.api.mcp import main

    main()


@app.command()
def health(embed: bool = typer.Option(False, help="Also probe the embedder")) -> None:
    """Check canonical storage dependencies."""
    from windex.db.canonical import inspect_generation
    from windex.index import qdrant

    settings = get_settings()
    failed = False
    try:
        with db.connect(settings.pg_dsn) as conn, conn.cursor() as cur:
            metadata = inspect_generation(conn)
            cur.execute("SELECT count(*) FROM sources WHERE archived_at IS NULL")
            sources = cur.fetchone()[0]
        if metadata is None:
            raise RuntimeError("canonical schema metadata is absent")
        console.print(
            f"[green]postgres ok[/green] epoch={metadata['contract_epoch']} "
            f"sources={sources}")
    except Exception as exc:  # noqa: BLE001
        console.print(f"[red]postgres failed[/red] {exc}")
        failed = True
    try:
        client = qdrant.client_from_url(settings.qdrant_url)
        collections = len(client.get_collections().collections)
        client.close()
        console.print(f"[green]qdrant ok[/green] collections={collections}")
    except Exception as exc:  # noqa: BLE001
        console.print(f"[red]qdrant failed[/red] {exc}")
        failed = True
    if embed:
        from windex.embed import build_embedder

        embedder = build_embedder(settings)
        try:
            if not embedder.ping():
                raise RuntimeError("probe returned false")
            console.print(f"[green]embedder ok[/green] model={embedder.model_id}")
        except Exception as exc:  # noqa: BLE001
            console.print(f"[red]embedder failed[/red] {exc}")
            failed = True
        finally:
            embedder.close()
    if failed:
        raise typer.Exit(1)


@app.command()
def search(
    query: str,
    source: str = "all",
    limit: int = typer.Option(10, min=1, max=50),
    mode: str = typer.Option("hybrid"),
) -> None:
    """Query the canonical public search service."""
    from windex.api import service

    settings = get_settings()
    service.validate_source(settings, source)
    console.print_json(json.dumps(
        service.run_search(
            settings, query, source=source, limit=limit, mode=mode),
        default=str,
    ))


@app.command("source-pipeline-cutover")
def source_pipeline_cutover(
    bootstrap_id: str = typer.Option(..., help="Exact reviewed bootstrap ID"),
    confirm: str | None = typer.Option(
        None, help="Exact confirmation printed by the dry run"),
    execute_reset: bool = typer.Option(
        False, "--execute", help="Perform the irreversible reset"),
    reset_qdrant: bool = typer.Option(
        True, "--reset-qdrant/--keep-qdrant",
        help="Delete only manifest-owned Qdrant resources"),
) -> None:
    """Dry-run or execute the fail-closed contract-epoch reset."""
    import psycopg

    from windex.db.cutover import UnsafeCutover, execute, preflight

    settings = get_settings()
    try:
        if not execute_reset:
            try:
                conn = db.connect(settings.pg_dsn)
            except psycopg.OperationalError:
                conn = None
            try:
                manifest = preflight(
                    settings, bootstrap_id=bootstrap_id, conn=conn)
            finally:
                if conn is not None:
                    conn.close()
            console.print_json(json.dumps(manifest))
            return
        if confirm is None:
            raise UnsafeCutover("--confirm is required with --execute")
        result = execute(
            settings, bootstrap_id=bootstrap_id, confirmation=confirm,
            reset_qdrant=reset_qdrant)
        console.print_json(json.dumps(result))
    except UnsafeCutover as exc:
        console.print(f"[red]cutover refused:[/red] {exc}")
        raise typer.Exit(2)


@app.command("source-pipeline-quarantine")
def source_pipeline_quarantine(
    bootstrap_id: str = typer.Option(..., help="Verified bootstrap ID"),
    confirm: str = typer.Option(
        ..., help="Exact QUARANTINE confirmation from the cutover manifest"),
) -> None:
    """Quarantine the prior generation after Source/search verification."""
    from windex.db.cutover import UnsafeCutover, quarantine_previous

    try:
        result = quarantine_previous(
            get_settings(), bootstrap_id=bootstrap_id, confirmation=confirm)
    except UnsafeCutover as exc:
        console.print(f"[red]quarantine refused:[/red] {exc}")
        raise typer.Exit(2)
    console.print_json(json.dumps(result))


@app.command("module-approve")
def module_approve(
    name: str,
    version: int,
    acknowledge_local_code: bool = typer.Option(
        False, "--acknowledge-local-code",
        help="Required acknowledgement for executable local code"),
) -> None:
    """Loopback recovery approval for one fixture-tested Module version."""
    if not acknowledge_local_code:
        console.print(
            "[red]refused:[/red] pass --acknowledge-local-code after reviewing "
            "the immutable source digest")
        raise typer.Exit(2)
    from windex.modules import admin as module_store

    try:
        with db.connect(get_settings().pg_dsn) as conn:
            result = module_store.approve(
                conn, name, version, approved_by="loopback CLI")
    except module_store.ModuleAdminError as exc:
        console.print(f"[red]approval refused:[/red] {exc}")
        raise typer.Exit(2)
    console.print(
        f"[green]approved[/green] {name}@{version} "
        f"{result['source_digest']}")


@app.command("source-scheduler")
def source_scheduler(
    interval: float = typer.Option(10.0, min=0.25),
    once: bool = typer.Option(False),
) -> None:
    """Dispatch canonical Source triggers with self-healing retries."""
    from windex.source.scheduler import (
        arm_unplanned,
        maintain_partitions,
        prune_expired_artifacts,
        tick,
    )

    settings = get_settings()
    last_maintenance: float | None = None
    while True:
        try:
            with db.connect(settings.pg_dsn) as conn:
                if (
                    last_maintenance is None
                    or time.monotonic() - last_maintenance >= 3600
                ):
                    maintain_partitions(conn)
                    prune_expired_artifacts(conn, settings)
                    last_maintenance = time.monotonic()
                arm_unplanned(conn)
                result = tick(conn)
            if result.fired or result.coalesced or result.failed:
                console.print({
                    "fired": result.fired,
                    "coalesced": result.coalesced,
                    "skipped": result.skipped,
                    "failed": result.failed,
                })
        except Exception as exc:  # noqa: BLE001
            console.print(f"[red]Source scheduler tick failed:[/red] {exc}")
        if once:
            return
        time.sleep(interval)


@app.command()
def worker(
    slots: int = typer.Option(0, help="Slot subprocesses (0 = configured default)"),
    lanes: str = typer.Option("", help="Comma-separated lanes (default: all)"),
    slice_seconds: float = typer.Option(0.0, help="Maximum seconds per task slice"),
    name: str = typer.Option("", help="Pool name used in lease worker IDs"),
    inline: bool = typer.Option(False, "--inline", help="Run one debug slot"),
) -> None:
    """Run the canonical leased, sliced Pipeline worker pool."""
    import logging

    from windex.worker import config_from_env, default_resolve
    from windex.worker.slot import slot_main
    from windex.worker.supervisor import Pool

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    settings = get_settings()
    cfg = config_from_env().with_overrides(
        slots=slots or None,
        slice_seconds=slice_seconds or None,
        name=name or None,
        lanes=tuple(item.strip() for item in lanes.split(",") if item.strip())
        or None,
    )
    if inline:
        from windex.worker import control
        from windex.worker.preconditions import evaluate

        control.write(
            cfg.control_path, satisfied=evaluate(settings),
            blocked_lanes=(), generation=0)
        raise typer.Exit(slot_main(settings.pg_dsn, default_resolve, cfg, 0))
    Pool(settings.pg_dsn, default_resolve, cfg, settings=settings).run()


if __name__ == "__main__":
    app()
