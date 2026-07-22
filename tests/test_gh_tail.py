import gzip
import json
from datetime import date

from windex.github import tail


def test_hour_names_covers_range_exclusive():
    names = tail.hour_names(date(2026, 7, 13), date(2026, 7, 15))
    assert len(names) == 48
    assert names[0] == "2026-07-13-0.json.gz" and names[-1] == "2026-07-14-23.json.gz"


def test_sync_hours_explicit_range(pg):
    n = tail.sync_hours(pg, start=date(2024, 10, 1), end=date(2024, 10, 2))
    assert n == 24
    assert tail.sync_hours(pg, start=date(2024, 10, 1), end=date(2024, 10, 2)) == 0


def test_count_watch_events_filters_and_aggregates(tmp_path):
    events = [
        {"type": "WatchEvent", "repo": {"id": 1, "name": "a/x"}},
        {"type": "WatchEvent", "repo": {"id": 1, "name": "a/x"}},
        {"type": "PushEvent", "repo": {"id": 2, "name": "b/y"}},
        {"type": "IssueCommentEvent", "repo": {"id": 3, "name": "c/z"},
         "payload": {"comment": {"body": "I love WatchEvent strings"}}},  # pre-filter trap
        {"type": "WatchEvent", "repo": {"id": 4, "name": "d/w"}},
    ]
    path = tmp_path / "2026-07-14-0.json.gz"
    with gzip.open(path, "wt") as f:
        for ev in events:
            f.write(json.dumps(ev) + "\n")
        f.write("not json at all\n")
    counts = tail.count_watch_events(path)
    assert counts == {1: ("a/x", 2), 4: ("d/w", 1)}


def test_upsert_counts_accumulates_and_handles_rename(pg):
    tail.upsert_counts(pg, {1: ("owner/repo", 2)})
    tail.upsert_counts(pg, {1: ("owner/repo", 3)})
    with pg.cursor() as cur:
        cur.execute("SELECT star_events FROM repos WHERE repo_id = 1")
        assert cur.fetchone()[0] == 5
    # different repo_id claims the same full_name (delete + recreate on GitHub)
    tail.upsert_counts(pg, {2: ("owner/repo", 1)})
    with pg.cursor() as cur:
        cur.execute("SELECT repo_id FROM repos WHERE full_name = 'owner/repo'")
        assert cur.fetchone()[0] == 2
        cur.execute("SELECT full_name FROM repos WHERE repo_id = 1")
        assert "#stale:" in cur.fetchone()[0]


def test_upsert_counts_conflict_preserves_earlier_rows(pg):
    """A full_name UniqueViolation on one repo must roll back only THAT row, not
    the whole hour's transaction — conn.rollback() discarded the star_events
    already accumulated for every earlier repo in the same file (then scan marked
    the hour done, losing them forever)."""
    with pg.cursor() as cur:
        cur.execute("INSERT INTO repos (repo_id, full_name, star_events) VALUES (999, 'o/x', 0)")
    pg.commit()
    # 100 and 200 upsert cleanly; 300 collides with 999 on full_name 'o/x'
    tail.upsert_counts(pg, {100: ("o/a", 5), 200: ("o/b", 3), 300: ("o/x", 2)})
    with pg.cursor() as cur:
        cur.execute("SELECT repo_id, star_events FROM repos "
                    "WHERE repo_id IN (100, 200, 300) ORDER BY repo_id")
        assert dict(cur.fetchall()) == {100: 5, 200: 3, 300: 2}  # earlier rows survived
        cur.execute("SELECT full_name FROM repos WHERE repo_id = 999")
        assert "#stale:" in cur.fetchone()[0]  # incumbent suffixed, 300 won the name
