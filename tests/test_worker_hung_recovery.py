from __future__ import annotations

import multiprocessing as mp
import signal
import time
from pathlib import Path

import pytest

from windex.worker import control
from windex.worker import supervisor as supervisor_module
from windex.worker.config import PoolConfig, config_from_env
from windex.worker.control import SlotStatus
from windex.worker.supervisor import Pool, RunningTask


def _pool(tmp_path: Path, **overrides: object) -> Pool:
    cfg = PoolConfig(
        state_dir=tmp_path,
        slice_seconds=10.0,
        heartbeat_seconds=1.0,
        hung_grace_seconds=5.0,
        stop_grace_seconds=3.0,
    ).with_overrides(**overrides)
    return Pool("unused", lambda _module: None, cfg, precond=set)


def _register(
    pool: Pool,
    *,
    pid: int = 4242,
    task_id: int = 7,
    generation: int = 1,
    started_at: float = 100.0,
) -> str:
    worker = f"test/0/{pid}"
    pool.slots[0] = SlotStatus(index=0, pid=pid, worker=worker)
    control.write_active_slice(
        pool.cfg.active_slice_path(0),
        pid=pid,
        worker=worker,
        task_id=task_id,
        generation=generation,
        started_at=started_at,
    )
    return worker


def test_overdue_noncooperative_slice_is_really_killed(tmp_path: Path) -> None:
    """The safety mechanism is outside the wedged process, not a GIL-bound timer."""
    # The dummy is only a signal target; spawn avoids forking pytest's threads.
    ctx = mp.get_context("spawn")
    proc = ctx.Process(target=time.sleep, args=(30,))
    proc.start()
    assert proc.pid is not None
    pool = _pool(tmp_path)
    worker = _register(
        pool,
        pid=proc.pid,
        started_at=time.monotonic() - 16.0,
    )
    try:
        pool._enforce_hung_policy(
            {worker: RunningTask(task_id=7, worker=worker)})
        proc.join(timeout=5)
        assert not proc.is_alive()
        assert proc.exitcode == -signal.SIGKILL
        assert pool._forced_exits == {0: True}
    finally:
        if proc.is_alive():
            proc.kill()
            proc.join(timeout=5)


def test_zero_grace_is_immediate_not_disabled(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool = _pool(tmp_path, hung_grace_seconds=0.0)
    worker = _register(pool)
    signals: list[tuple[int, int]] = []
    monkeypatch.setattr(
        pool, "_signal", lambda pid, sig: signals.append((pid, sig)))

    pool._enforce_hung_policy(
        {worker: RunningTask(task_id=7, worker=worker)},
        now=110.0,
    )

    assert signals == [(4242, signal.SIGKILL)]
    assert pool._forced_exits == {0: True}


@pytest.mark.parametrize(
    ("cancelled", "paused"),
    [(True, False), (False, True)],
)
def test_cancel_and_pause_are_bounded_without_spending_attempt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    cancelled: bool,
    paused: bool,
) -> None:
    pool = _pool(tmp_path)
    worker = _register(pool, started_at=100.0)
    signals: list[tuple[int, int]] = []
    monkeypatch.setattr(
        pool, "_signal", lambda pid, sig: signals.append((pid, sig)))
    pending = {
        worker: RunningTask(
            task_id=7,
            worker=worker,
            yield_requested=True,
            cancelled=cancelled,
            paused=paused,
        ),
    }

    pool._enforce_hung_policy(pending, now=101.0)
    assert signals == []
    pool._enforce_hung_policy(pending, now=106.0)

    assert signals == [(4242, signal.SIGKILL)]
    assert pool._forced_exits == {0: False}


def test_normal_repeated_slices_reset_the_deadline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pool = _pool(tmp_path)
    worker = _register(pool, generation=1, started_at=100.0)
    signals: list[tuple[int, int]] = []
    monkeypatch.setattr(
        pool, "_signal", lambda pid, sig: signals.append((pid, sig)))
    pending = {worker: RunningTask(task_id=7, worker=worker)}

    pool._enforce_hung_policy(pending, now=114.9)
    control.write_active_slice(
        pool.cfg.active_slice_path(0),
        pid=4242,
        worker=worker,
        task_id=7,
        generation=2,
        started_at=114.0,
    )
    # This is well beyond generation one's hard deadline, but generation two is
    # a fresh cooperative slice of the same task on the same worker.
    pool._enforce_hung_policy(pending, now=125.0)

    assert signals == []
    assert pool.slots[0].slice_generation == 2
    assert pool._forced_exits == {}


@pytest.mark.parametrize("penalize", [False, True])
def test_forced_exit_classification_reaches_task_release(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    penalize: bool,
    ) -> None:
    class Dead:
        exitcode = -signal.SIGKILL
        pid = 4242

        @staticmethod
        def is_alive() -> bool:
            return False

        @staticmethod
        def join(timeout: float) -> None:
            assert timeout == 1

    class Connection:
        def __enter__(self) -> Connection:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

    pool = _pool(tmp_path)
    worker = _register(pool)
    pool._procs[0] = Dead()  # type: ignore[assignment]
    pool._forced_exits[0] = penalize
    releases: list[tuple[str, bool]] = []
    monkeypatch.setattr(
        supervisor_module.db, "connect", lambda _dsn: Connection())
    monkeypatch.setattr(
        supervisor_module.C,
        "release_worker",
        lambda _conn, released_worker, *, penalize: (
            releases.append((released_worker, penalize)) or []
        ),
    )

    assert pool._reap() == 1

    assert releases == [(worker, penalize)]


def test_shutdown_has_an_independent_hard_bound(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class NeverStops:
        exitcode = None

        @staticmethod
        def is_alive() -> bool:
            return True

        @staticmethod
        def join(timeout: float) -> None:
            assert timeout == 5

    pool = _pool(tmp_path, stop_grace_seconds=0.0)
    _register(pool)
    pool._procs[0] = NeverStops()  # type: ignore[assignment]
    signals: list[tuple[int, int]] = []
    monkeypatch.setattr(
        pool, "_signal", lambda pid, sig: signals.append((pid, sig)))
    monkeypatch.setattr(pool, "_reap", lambda release=True: 0)

    pool.shutdown()

    assert signals == [(4242, signal.SIGKILL)]


def test_hung_policy_environment_is_complete_and_validated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("WINDEX_WORKER_SLICE_SECONDS", "45")
    monkeypatch.setenv("WINDEX_WORKER_SLICE_UNITS", "200")
    monkeypatch.setenv("WINDEX_WORKER_HEARTBEAT_SECONDS", "2.5")
    monkeypatch.setenv("WINDEX_WORKER_HUNG_GRACE_SECONDS", "7.5")
    monkeypatch.setenv("WINDEX_WORKER_STOP_GRACE_SECONDS", "4")

    cfg = config_from_env()

    assert cfg.slice_seconds == 45
    assert cfg.slice_units == 200
    assert cfg.heartbeat_seconds == 2.5
    assert cfg.hung_grace_seconds == 7.5
    assert cfg.stop_grace_seconds == 4

    monkeypatch.setenv("WINDEX_WORKER_HUNG_GRACE_SECONDS", "-1")
    with pytest.raises(ValueError, match="hung_grace_seconds"):
        config_from_env()
