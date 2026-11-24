"""HTTP server: routes CHELSA v2 byte-range reads through the SQLite cache,
forwarding only cache misses to the fixed upstream.

Native request contract reproduced from the real client (Earth Platform
alpha.73 / combined-73, commit 88b13c33954550c6ac660dceb743a4288a9aafac,
``io.earthplatform.geography.globe.ClimateSource`` + ``RangeFiles``):

- ``GET <path>`` with an exact ``Range: bytes=<start>-<end>`` header (end
  inclusive). There is no query string and no other method in use.
- Exactly five files are ever read, all relative to
  ``https://os.zhdk.cloud.switch.ch/chelsav2/GLOBAL/`` (dataset
  ``chelsa-v2.1-1981-2010``, CC0 1.0 — Karger et al. 2017, Scientific Data
  4:170122; no terms restriction blocks caching or republishing bytes, so
  this is not held on an unresolved attribution/terms question):
  - ``climatologies/1981-2010/bio/CHELSA_kg2_1981-2010_V.2.1.tif``
  - ``climatologies/1981-2010/bio/CHELSA_bio1_1981-2010_V.2.1.tif``
  - ``climatologies/1981-2010/bio/CHELSA_bio10_1981-2010_V.2.1.tif``
  - ``climatologies/1981-2010/bio/CHELSA_bio12_1981-2010_V.2.1.tif``
  - ``input/dem_geo.sdat``
- The client treats HTTP 404/403 as "this row/file is missing" (never
  retried as transient) and 200/206 as success; a 200 whose body is larger
  than the requested range is sliced down to it client-side. This proxy
  forwards status and body through unmodified and lets the client keep
  making that distinction — it does not reinterpret 403 as "transient" or
  vice versa.
- A dedicated reachability probe (``HostBackoff.check``) issues
  ``GET <path>`` with exactly ``Range: bytes=0-0`` — one byte, offset
  zero — against whichever file URI the host is already failing on. Per
  the cross-lane direction to route probes through the configured local
  proxy with an explicit TTL of 0 (never infinite freshness, never stale
  success): this exact shape is detected and always forwarded live to the
  real upstream, never served from or written to the data cache. It is
  counted separately (``probe_sends``) so it never inflates or deflates
  the ordinary cold/warm data-read counters.
"""

from __future__ import annotations

import http.client
import os
import threading
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit, urlunsplit

from .cache import Cache, CachedRange

ALLOWED_PATHS = {
    "/climatologies/1981-2010/bio/CHELSA_kg2_1981-2010_V.2.1.tif",
    "/climatologies/1981-2010/bio/CHELSA_bio1_1981-2010_V.2.1.tif",
    "/climatologies/1981-2010/bio/CHELSA_bio10_1981-2010_V.2.1.tif",
    "/climatologies/1981-2010/bio/CHELSA_bio12_1981-2010_V.2.1.tif",
    "/input/dem_geo.sdat",
}

ATTRIBUTION = "CHELSA V2.1 climatologies 1981-2010, Karger et al. 2017, Scientific Data 4:170122 (CC0 1.0)"

_PROBE_RANGE = "bytes=0-0"


class RangeNotSatisfiable(Exception):
    pass


def _parse_range(header: str | None) -> tuple[int, int]:
    """Parses ``bytes=<start>-<end>`` (end inclusive). Raises RangeNotSatisfiable
    for anything else — CHELSA's own client never sends a suffix range, an
    open-ended range, or multiple ranges, so this proxy does not guess at them.
    """
    if not header or not header.startswith("bytes="):
        raise RangeNotSatisfiable(header)
    spec = header[len("bytes="):]
    if "," in spec:
        raise RangeNotSatisfiable(header)
    start_s, _, end_s = spec.partition("-")
    if not start_s or not end_s:
        raise RangeNotSatisfiable(header)
    try:
        start, end = int(start_s), int(end_s)
    except ValueError as exc:
        raise RangeNotSatisfiable(header) from exc
    if start < 0 or end < start:
        raise RangeNotSatisfiable(header)
    return start, end


class Config:
    def __init__(
        self,
        bind_host: str = "127.0.0.1",
        bind_port: int = 8833,
        sqlite_path: str = "./chelsa-cache.sqlite3",
        upstream_base: str = "https://os.zhdk.cloud.switch.ch/chelsav2/GLOBAL",
        max_cache_bytes: int = 2 << 30,
        timeout_seconds: float = 30.0,
    ) -> None:
        self.bind_host = bind_host
        self.bind_port = bind_port
        self.sqlite_path = sqlite_path
        self.upstream_base = upstream_base.rstrip("/")
        self.max_cache_bytes = max_cache_bytes
        self.timeout_seconds = timeout_seconds

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            bind_host=os.environ.get("PROXY_BIND_HOST", "127.0.0.1"),
            bind_port=int(os.environ.get("PROXY_BIND_PORT", "8833")),
            sqlite_path=os.environ.get("PROXY_SQLITE_PATH", "./chelsa-cache.sqlite3"),
            upstream_base=os.environ.get(
                "CHELSA_UPSTREAM_BASE", "https://os.zhdk.cloud.switch.ch/chelsav2/GLOBAL"
            ),
            max_cache_bytes=int(os.environ.get("PROXY_MAX_CACHE_BYTES", str(2 << 30))),
            timeout_seconds=float(os.environ.get("PROXY_UPSTREAM_TIMEOUT_SECONDS", "30")),
        )


def _forward(config: Config, path: str, range_header: str):
    """Makes one actual network send to the fixed upstream for one exact byte
    range of one allowlisted path.

    Always returns a 3-tuple and never raises for a network/transport
    failure — every way the send can go wrong is folded into the return
    value so the caller has exactly one place to decide whether anything is
    cacheable.

    Returns (status, body, complete):
    - ``status`` is 0 if no response was ever received at all.
    - ``complete`` is False whenever the response must not be cached as
      success: a non-200/206 status, or a declared length that the actually
      delivered body didn't match (a genuine mid-transfer truncation).
    """
    base = urlsplit(config.upstream_base)
    url = urlunsplit((base.scheme, base.netloc, base.path + path, "", ""))
    req = urllib.request.Request(
        url,
        headers={"Range": range_header, "User-Agent": "chelsa-cache-proxy/0.1"},
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=config.timeout_seconds) as resp:
            expected_length = resp.headers.get("Content-Length")
            try:
                raw = resp.read()
            except (http.client.HTTPException, OSError):
                return resp.status, b"", False
            complete = expected_length is None or len(raw) == int(expected_length)
            return resp.status, raw, complete
    except urllib.error.HTTPError as exc:
        # 404/403 land here too: a real, complete answer from the upstream,
        # just not a 2xx one. Never cached as success; the status is
        # forwarded through unmodified so the client keeps making its own
        # missing-vs-transient distinction.
        return exc.code, exc.read(), False
    except (urllib.error.URLError, OSError):
        # Covers both "never connected at all" (URLError: refused/DNS/
        # timeout before any bytes) and "connected but the peer dropped us
        # before a status line arrived" (e.g. http.client.RemoteDisconnected,
        # an OSError subclass) — neither is a real HTTP response.
        return 0, b"", False


def make_handler(cache: Cache, config: Config):
    locks_guard = threading.Lock()
    inflight: dict[str, threading.Lock] = {}

    def singleflight_fetch(key: str, path: str, range_header: str):
        with locks_guard:
            lock = inflight.setdefault(key, threading.Lock())
        with lock:
            cached = cache.get(key)
            if cached is not None:
                return "warm", cached.status, cached.body
            status, body, complete = _forward(config, path, range_header)
            if status in (200, 206) and complete:
                cache.put(key, CachedRange(status=status, body=body))
                return "cold", status, body
            return "uncached", status, body
        # Lock is released; stale per-path lock objects are intentionally
        # left in the dict (bounded by the 5 allowlisted paths, not by
        # request volume) rather than torn down under a race.

    class Handler(BaseHTTPRequestHandler):
        server_version = "chelsa-cache-proxy/0.1"

        def log_message(self, fmt, *args):  # noqa: D401 - silence default stderr logging
            pass

        def do_GET(self) -> None:
            parsed = urlsplit(self.path)
            if parsed.path == "/_internal/stats":
                self._respond_json(200, cache.stats() | {"attribution": ATTRIBUTION})
                return
            if parsed.path not in ALLOWED_PATHS:
                self._respond_plain(404, b"path not in the allowlisted CHELSA request set")
                return
            range_header = self.headers.get("Range")
            try:
                start, end = _parse_range(range_header)
            except RangeNotSatisfiable:
                self._respond_plain(416, b"unsupported or missing Range header")
                return

            if range_header == _PROBE_RANGE:
                # Reachability probe: TTL 0, explicitly — never served from
                # or written to the data cache, always a live round trip.
                status, body, _complete = _forward(config, parsed.path, range_header)
                cache.record_probe_send()
                if status == 0:
                    self._respond_plain(502, b"upstream unreachable")
                    return
                self._respond_bytes(status, body)
                return

            key = Cache.key(parsed.path, start, end)
            outcome, status, body = singleflight_fetch(key, parsed.path, range_header)
            if outcome == "warm":
                cache.record_warm_hit()
                self._respond_bytes(status, body)
                return
            if outcome == "cold":
                cache.record_cold_miss()
                self._respond_bytes(status, body)
                return
            # uncached: error, transient failure, or truncated — never
            # stored, always retryable on the next request.
            cache.record_cold_miss()
            if status == 0:
                self._respond_plain(502, b"upstream unreachable")
                return
            self._respond_bytes(status, body)

        def _respond_bytes(self, status: int, body: bytes) -> None:
            self.send_response(status)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _respond_plain(self, status: int, body: bytes) -> None:
            self.send_response(status)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _respond_json(self, status: int, obj) -> None:
            import json

            body = json.dumps(obj).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    return Handler


class ProxyServer(ThreadingHTTPServer):
    """A ThreadingHTTPServer that also owns (and closes) the cache's sqlite
    connection, so callers have one object to shut down cleanly.
    """

    daemon_threads = True

    def __init__(self, config: Config, cache: Cache) -> None:
        super().__init__((config.bind_host, config.bind_port), make_handler(cache, config))
        self.proxy_cache = cache

    def server_close(self) -> None:
        super().server_close()
        self.proxy_cache.close()


def run(config: Config | None = None) -> ProxyServer:
    config = config or Config.from_env()
    cache = Cache(config.sqlite_path, max_bytes=config.max_cache_bytes)
    return ProxyServer(config, cache)


def main() -> None:
    server = run()
    try:
        server.serve_forever()
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
