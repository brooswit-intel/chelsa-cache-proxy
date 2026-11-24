from __future__ import annotations

import os
import tempfile
import threading
import unittest
import urllib.error
import urllib.request

from chelsa_cache_proxy.server import Config, ProxyServer
from chelsa_cache_proxy.cache import Cache

from fake_upstream import FakeUpstream

DEM_PATH = "/input/dem_geo.sdat"
KOPPEN_PATH = "/climatologies/1981-2010/bio/CHELSA_kg2_1981-2010_V.2.1.tif"


def _get(port: int, path: str, range_header: str | None) -> tuple[int, bytes]:
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", method="GET")
    if range_header is not None:
        req.add_header("Range", range_header)
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read()


def _stats(port: int) -> dict:
    import json

    with urllib.request.urlopen(f"http://127.0.0.1:{port}/_internal/stats", timeout=5) as resp:
        return json.loads(resp.read())


class ProxyTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.sqlite_path = os.path.join(self.tmpdir.name, "cache.sqlite3")
        self.dem_bytes = bytes(range(256)) * 4  # 1024 bytes, deterministic
        self.upstream = FakeUpstream({DEM_PATH: self.dem_bytes}).start()
        self.config = Config(
            bind_host="127.0.0.1",
            bind_port=0,
            sqlite_path=self.sqlite_path,
            upstream_base=self.upstream.base_url,
        )

    def _start_server(self, config: Config | None = None) -> ProxyServer:
        config = config or self.config
        cache = Cache(config.sqlite_path, max_bytes=config.max_cache_bytes)
        server = ProxyServer(config, cache)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.shutdown)
        self.addCleanup(server.server_close)
        return server

    def tearDown(self) -> None:
        self.upstream.stop()
        self.tmpdir.cleanup()

    def test_start_empty(self) -> None:
        server = self._start_server()
        stats = _stats(server.server_address[1])
        self.assertEqual(stats["cold_misses"], 0)
        self.assertEqual(stats["warm_hits"], 0)
        self.assertEqual(stats["cached_entries"], 0)

    def test_cold_then_warm(self) -> None:
        server = self._start_server()
        port = server.server_address[1]
        status, body = _get(port, DEM_PATH, "bytes=0-7")
        self.assertEqual(status, 206)
        self.assertEqual(body, self.dem_bytes[0:8])
        self.assertEqual(self.upstream.call_count, 1)

        status2, body2 = _get(port, DEM_PATH, "bytes=0-7")
        self.assertEqual(status2, 206)
        self.assertEqual(body2, self.dem_bytes[0:8])
        # Served from the SQLite cache: no second upstream send.
        self.assertEqual(self.upstream.call_count, 1)

        stats = _stats(port)
        self.assertEqual(stats["cold_misses"], 1)
        self.assertEqual(stats["warm_hits"], 1)

    def test_restart_keeps_warm_cache(self) -> None:
        server = self._start_server()
        port = server.server_address[1]
        _get(port, DEM_PATH, "bytes=16-31")
        server.shutdown()
        server.server_close()

        # A fresh process against the same SQLite file would see this; here
        # a fresh ProxyServer/Cache object simulates the restart without an
        # actual process relaunch.
        second = self._start_server()
        port2 = second.server_address[1]
        status, body = _get(port2, DEM_PATH, "bytes=16-31")
        self.assertEqual(status, 206)
        self.assertEqual(body, self.dem_bytes[16:32])
        # Still just the one call from before the "restart".
        self.assertEqual(self.upstream.call_count, 1)
        self.assertEqual(_stats(port2)["warm_hits"], 1)

    def test_error_response_is_never_cached(self) -> None:
        server = self._start_server()
        port = server.server_address[1]
        self.upstream.fail_next = True
        status, _ = _get(port, DEM_PATH, "bytes=0-7")
        # The real upstream status (503) is forwarded through unmodified,
        # not remapped — only genuinely unreachable (no response at all)
        # becomes this proxy's own 502.
        self.assertEqual(status, 503)
        self.assertEqual(self.upstream.call_count, 1)

        # Retried: not served from a bogus cached failure, goes upstream again.
        status2, body2 = _get(port, DEM_PATH, "bytes=0-7")
        self.assertEqual(status2, 206)
        self.assertEqual(body2, self.dem_bytes[0:8])
        self.assertEqual(self.upstream.call_count, 2)
        self.assertEqual(_stats(port)["warm_hits"], 0)

    def test_genuinely_missing_file_is_not_cached_as_success(self) -> None:
        # Not in ALLOWED_PATHS at all -> rejected before ever reaching upstream.
        server = self._start_server()
        port = server.server_address[1]
        status, _ = _get(port, "/not/a/real/chelsa/path", "bytes=0-7")
        self.assertEqual(status, 404)
        self.assertEqual(self.upstream.call_count, 0)

    def test_concurrent_miss_is_deduplicated(self) -> None:
        server = self._start_server()
        port = server.server_address[1]
        results = []

        def fetch():
            results.append(_get(port, DEM_PATH, "bytes=0-7"))

        threads = [threading.Thread(target=fetch) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)

        self.assertEqual(len(results), 8)
        for status, body in results:
            self.assertEqual(status, 206)
            self.assertEqual(body, self.dem_bytes[0:8])
        # Eight concurrent identical misses collapse into one upstream send.
        self.assertEqual(self.upstream.call_count, 1)

    def test_probe_range_always_goes_live_and_is_never_cached(self) -> None:
        server = self._start_server()
        port = server.server_address[1]

        status, body = _get(port, DEM_PATH, "bytes=0-0")
        self.assertEqual(status, 206)
        self.assertEqual(body, self.dem_bytes[0:1])
        self.assertEqual(self.upstream.call_count, 1)
        self.assertEqual(_stats(port)["probe_sends"], 1)
        # A probe must never satisfy, or be satisfied by, the data cache.
        self.assertEqual(_stats(port)["cached_entries"], 0)

        # A second probe still goes live (TTL 0 — never infinite freshness).
        status2, _ = _get(port, DEM_PATH, "bytes=0-0")
        self.assertEqual(status2, 206)
        self.assertEqual(self.upstream.call_count, 2)

        # Now that an ordinary data read for the same bytes happens, it is
        # cached as ordinary data, separate from the probe counter.
        status3, _ = _get(port, DEM_PATH, "bytes=0-7")
        self.assertEqual(status3, 206)
        self.assertEqual(self.upstream.call_count, 3)
        self.assertEqual(_stats(port)["cold_misses"], 1)
        self.assertEqual(_stats(port)["probe_sends"], 2)

    def test_probe_fails_live_without_bypass_then_recovers(self) -> None:
        server = self._start_server()
        port = server.server_address[1]
        # Prewarm ordinary data so a probe-vs-data confusion would show up
        # as a stale success instead of a live failure.
        _get(port, DEM_PATH, "bytes=0-0")
        self.assertEqual(_stats(port)["probe_sends"], 1)

        self.upstream.down = True
        status, _ = _get(port, DEM_PATH, "bytes=0-0")
        self.assertEqual(status, 502)

        self.upstream.down = False
        status2, body2 = _get(port, DEM_PATH, "bytes=0-0")
        self.assertEqual(status2, 206)
        self.assertEqual(body2, self.dem_bytes[0:1])


if __name__ == "__main__":
    unittest.main()
