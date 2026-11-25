"""SQLite-backed byte-range cache and persisted cold/warm/probe counters.

A fresh ``Cache(path)`` against a path that doesn't exist yet starts with
zero cached entries and zero counters ("empty-start"). Everything here is
local disk I/O; nothing in this module makes a network call.

Entries are keyed by the exact (file path, byte range) CHELSA's own client
requests — the GeoTIFF layout is static, so the same row/header read always
asks for the same range. Size is bounded: the oldest-touched entries (by
``last_access``) are evicted once the total cached byte count would exceed
``max_bytes``, so a long-running proxy can't grow its SQLite file without
limit.
"""

from __future__ import annotations

import sqlite3
import threading
import time
from dataclasses import dataclass


@dataclass(frozen=True)
class CachedRange:
    status: int
    body: bytes


class Cache:
    """Thread-safe SQLite cache of complete, successful upstream range reads.

    Counters are persisted in the same database so a before/after delta of
    :meth:`stats` is valid across a proxy restart, not just within one
    process's lifetime.
    """

    COUNTER_NAMES = ("cold_misses", "warm_hits", "probe_sends")

    # Bump this whenever a past bug could have written an entry this
    # version's code would not trust — e.g. MINECRAFT-571 55866 found that
    # a 206 was accepted and cached solely because its body length matched
    # the request, without checking the upstream's own Content-Range
    # endpoints actually matched too (an equal-length, wrong-offset 206
    # would have been silently cached under the wrong key). Existing rows
    # from before that check existed cannot be retroactively verified, so
    # a version bump wipes all of them rather than leaving unverified data
    # in place — a one-time invalidation, not an ongoing migration scheme.
    SCHEMA_VERSION = 2

    def __init__(self, path: str, max_bytes: int = 2 << 30) -> None:
        self._lock = threading.Lock()
        self._max_bytes = max_bytes
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS entries ("
            " key TEXT PRIMARY KEY,"
            " status INTEGER NOT NULL,"
            " body BLOB NOT NULL,"
            " size INTEGER NOT NULL,"
            " last_access REAL NOT NULL"
            ")"
        )
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS counters ("
            " name TEXT PRIMARY KEY,"
            " value INTEGER NOT NULL"
            ")"
        )
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS meta ("
            " key TEXT PRIMARY KEY,"
            " value TEXT NOT NULL"
            ")"
        )
        for name in self.COUNTER_NAMES:
            self._conn.execute(
                "INSERT OR IGNORE INTO counters (name, value) VALUES (?, 0)",
                (name,),
            )
        row = self._conn.execute(
            "SELECT value FROM meta WHERE key = 'schema_version'"
        ).fetchone()
        stored_version = int(row[0]) if row is not None else 0
        if stored_version < self.SCHEMA_VERSION:
            # Every existing entry predates a correctness check this
            # version relies on — none of them can be trusted retroactively.
            self._conn.execute("DELETE FROM entries")
            self._conn.execute(
                "INSERT OR REPLACE INTO meta (key, value) VALUES ('schema_version', ?)",
                (str(self.SCHEMA_VERSION),),
            )
        # A cached entry is never stored with status 200 under the current
        # contract (server.py always normalizes a sliced success to 206 —
        # see MINECRAFT-571 55861). An entry with status 200 can only be a
        # leftover from the earlier, incompatible prototype behavior that
        # kept 200 after slicing, which a real RangeFiles-style client
        # would double-slice into an empty/wrong body. Purge any such rows
        # on open rather than ever risk serving one — redundant with the
        # version-bump wipe above today, but kept as defense in depth
        # against a future dev/test DB that got entries written outside
        # this schema's own migration path.
        self._conn.execute("DELETE FROM entries WHERE status = 200")
        self._conn.commit()

    @staticmethod
    def key(path: str, start: int, end_inclusive: int) -> str:
        return f"{path}:{start}-{end_inclusive}"

    def get(self, key: str) -> CachedRange | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT status, body FROM entries WHERE key = ?", (key,)
            ).fetchone()
            if row is not None:
                self._conn.execute(
                    "UPDATE entries SET last_access = ? WHERE key = ?",
                    (time.time(), key),
                )
                self._conn.commit()
        if row is None:
            return None
        status, body = row
        return CachedRange(status=status, body=bytes(body))

    def put(self, key: str, response: CachedRange) -> None:
        """Store a complete, successful (200/206) range read. Callers must
        never call this for an error, transient, negative, or truncated
        response — this module does not itself filter on status.
        """
        size = len(response.body)
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO entries (key, status, body, size, last_access)"
                " VALUES (?, ?, ?, ?, ?)",
                (key, response.status, response.body, size, time.time()),
            )
            self._evict_locked()
            self._conn.commit()

    def _evict_locked(self) -> None:
        total = self._conn.execute("SELECT COALESCE(SUM(size), 0) FROM entries").fetchone()[0]
        if total <= self._max_bytes:
            return
        rows = self._conn.execute("SELECT key, size FROM entries ORDER BY last_access ASC").fetchall()
        for key, size in rows:
            if total <= self._max_bytes:
                break
            self._conn.execute("DELETE FROM entries WHERE key = ?", (key,))
            total -= size

    def record_cold_miss(self) -> None:
        self._incr("cold_misses")

    def record_warm_hit(self) -> None:
        self._incr("warm_hits")

    def record_probe_send(self) -> None:
        self._incr("probe_sends")

    def _incr(self, name: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE counters SET value = value + 1 WHERE name = ?", (name,)
            )
            self._conn.commit()

    def stats(self) -> dict[str, int]:
        with self._lock:
            rows = self._conn.execute("SELECT name, value FROM counters").fetchall()
            cached_bytes = self._conn.execute(
                "SELECT COALESCE(SUM(size), 0) FROM entries"
            ).fetchone()[0]
            cached_entries = self._conn.execute("SELECT COUNT(*) FROM entries").fetchone()[0]
        out = {name: value for name, value in rows}
        out["cached_bytes"] = cached_bytes
        out["cached_entries"] = cached_entries
        return out

    def close(self) -> None:
        with self._lock:
            self._conn.close()
