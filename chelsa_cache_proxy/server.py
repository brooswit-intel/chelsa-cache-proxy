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
  ``GET <path>`` with exactly ``Range: bytes=0-0`` against whichever file
  URI the host is already failing on. Earlier revisions of this proxy
  special-cased that exact byte-range shape on the ordinary data-serving
  path — MINECRAFT-571 comment 55737 corrected that design generally (it
  was also wrong on the sibling elevation-cache-proxy, see its comment
  55766): the TTL-0 resource must be a *dedicated* health-check endpoint,
  not method/range-sniffing on the tile/data resource, because a real
  ``bytes=0-0`` *data* request (same shape, different intent) would
  otherwise never cache either. This proxy now exposes a separate
  ``GET /__health`` endpoint for that purpose (see below); the ordinary
  data path no longer inspects the Range value at all. Wiring the real
  client's ``HostBackoff`` to call ``/__health`` instead of probing a data
  file URI directly, when this proxy is configured, is shared-infra work
  (``HostBackoff`` is used by every source, not just CHELSA) and is left to
  the MINECRAFT-580 coordinated integration lane, not duplicated here.

``GET /__health`` always makes one fresh live round trip to the real
upstream (a 1-byte read of the fixed DEM file) and never reads from or
writes to the data cache in either direction — TTL 0, explicitly. It is
counted separately (``probe_sends``) so it never inflates or deflates the
ordinary ``cold_misses``/``warm_hits`` data counters.
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

# The dedicated TTL-0 health endpoint always probes this one fixed, small
# file — it does not need to vary by whichever path a client happens to be
# failing on, since it never touches the data cache either way.
HEALTH_PROBE_PATH = "/input/dem_geo.sdat"
HEALTH_PROBE_RANGE = "bytes=0-0"

# Defensive ceiling on any single ordinary-data forward: real CHELSA rows
# and headers are a few hundred KiB at most (MAX_ROW_BYTES/MAX_HEADER_BYTES
# in ClimateSource.java are 1 MiB / 2 MiB), so this is generous headroom,
# not a tight budget — it exists to bound worst-case memory use if the
# upstream ever ignores Range on one of the multi-hundred-MB GeoTIFFs or
# the tens-of-GiB raw DEM file, not to reject anything CHELSA actually
# sends in practice.
_MAX_FORWARD_BYTES = 64 << 20


class RangeNotSatisfiable(Exception):
    pass


def _parse_content_range_endpoints(header: str | None) -> tuple[int, int] | None:
    """Parses a response ``Content-Range: bytes <start>-<end>/<total-or-*>``
    header and returns its ``(start, end)`` endpoints, or ``None`` if the
    header is missing or not in that exact shape. Does not look at the
    ``/<total-or-*>`` part — only the endpoints matter for verifying a 206
    actually answers the range that was requested.
    """
    if not header or not header.startswith("bytes "):
        return None
    range_part = header[len("bytes "):].split("/", 1)[0]
    start_s, _, end_s = range_part.partition("-")
    try:
        return int(start_s), int(end_s)
    except ValueError:
        return None


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


def _forward(config: Config, path: str, range_header: str, start: int, length: int):
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
    - On a real ``200`` (the upstream ignored the Range request and
      returned the whole file), ``body`` is sliced down to
      ``[start, start + length)`` here before the caller ever sees it, so
      a small-range request can never cache (or return) an entire
      multi-megabyte file under one byte-range cache key — AND the
      reported ``status`` is normalized to ``206``, never left as ``200``.
      MINECRAFT-571 comment 55861 found that an earlier revision sliced
      the body but kept reporting ``200``: the real client's own
      ``RangeFiles.java`` has identical defensive logic
      (``if (status == 200 && (offset != 0 || body.length > length)) body
      = Arrays.copyOfRange(...)``) that re-slices *again* on seeing a raw
      ``200`` — double-slicing an already-sliced 10-byte body at its own
      original absolute offset produced an empty array even from a warm
      proxy cache. Reporting ``206`` tells the real client the body is
      already exactly what it asked for, so it never re-slices.
    - A real ``206`` is validated against both the *requested* length and
      the upstream's own ``Content-Range`` response endpoints, not just
      its declared ``Content-Length``. Length alone is not enough:
      MINECRAFT-571 comment 55866 found that an equal-length,
      wrong-offset 206 (answering ``bytes=10-19`` with ``bytes 20-29`` of
      the same length) would still pass a length-only check and get
      cached under the wrong key. A missing or mismatched ``Content-Range``
      is rejected before being trusted as a match, regardless of length.
    - A declared or actually-delivered body over ``_MAX_FORWARD_BYTES`` is
      never fully buffered: the real DEM file is the whole global raw-float
      grid (tens of GiB) and the GeoTIFF files are each hundreds of MB —
      if the upstream ever ignores Range on one of those, an unbounded
      read would pull the entire file into memory just to serve one small
      request. The sibling MINECRAFT-577 lanes (HRDEM's own 1-10 GB
      mosaics) hit the identical shape of risk and added the same kind of
      ceiling (MINECRAFT-571 55839/55846). Treated as truncated/uncacheable,
      never as success.
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
            if expected_length is not None and int(expected_length) > _MAX_FORWARD_BYTES:
                return resp.status, b"", False
            try:
                raw = resp.read(_MAX_FORWARD_BYTES + 1)
            except (http.client.HTTPException, OSError):
                return resp.status, b"", False
            if len(raw) > _MAX_FORWARD_BYTES:
                return resp.status, b"", False
            complete = expected_length is None or len(raw) == int(expected_length)
            status = resp.status
            if status == 200 and complete:
                if start != 0 or len(raw) > length:
                    raw = raw[start:start + length]
                # Always report this as 206: it is now exactly the
                # requested slice, and a raw 200 would make a real
                # RangeFiles-style client re-slice an already-sliced body
                # at its original absolute offset (55861).
                status = 206
                complete = len(raw) == length
            elif status == 206:
                # A 206 is only a match for this cache key if it both
                # delivered the requested length AND its own Content-Range
                # response header names the exact endpoints that were
                # asked for. Length alone is not enough: MINECRAFT-571
                # comment 55866 found that an equal-length, wrong-offset
                # 206 (e.g. answering bytes=10-19 with bytes 20-29 of the
                # same length) would still pass a length-only check and be
                # cached under the wrong key. A missing or mismatched
                # Content-Range means this is rejected before being
                # trusted as a match, regardless of body length.
                endpoints = _parse_content_range_endpoints(resp.headers.get("Content-Range"))
                complete = complete and len(raw) == length and endpoints == (start, start + length - 1)
            return status, raw, complete
    except urllib.error.HTTPError as exc:
        # 404/403 land here too: a real, complete answer from the upstream,
        # just not a 2xx one. Never cached as success; the status is
        # forwarded through unmodified so the client keeps making its own
        # missing-vs-transient distinction. Body is bounded the same as a
        # success response (55861) — an error body has no reason to be
        # huge, but nothing stops a misbehaving upstream from sending one.
        body = exc.read(_MAX_FORWARD_BYTES + 1)
        if len(body) > _MAX_FORWARD_BYTES:
            body = b""
        return exc.code, body, False
    except (urllib.error.URLError, OSError):
        # Covers both "never connected at all" (URLError: refused/DNS/
        # timeout before any bytes) and "connected but the peer dropped us
        # before a status line arrived" (e.g. http.client.RemoteDisconnected,
        # an OSError subclass) — neither is a real HTTP response.
        return 0, b"", False


# Bound on how many bytes the health probe will ever read, regardless of
# what the upstream sends: the 1-byte request plus one extra byte is enough
# to tell a well-behaved 206 apart from an origin that ignored Range and is
# about to hand back the entire multi-megabyte DEM file. MINECRAFT-571
# comment 55839 found exactly this gap (an unbounded read before
# validation) on the sibling WorldCover/JRC lanes' own health checks.
_HEALTH_PROBE_READ_CAP = 2


def _health_probe(config: Config) -> int:
    """A live reachability check against the fixed upstream, bounded to a
    couple of bytes regardless of what the response actually contains or
    claims to contain. Returns the real HTTP status, or 0 if no response
    was ever received at all. Never reads the data cache, never writes to
    it, and never risks reading an entire file into memory just to answer
    "is it up" — status is checked and, on a real 2xx answer, only a
    capped read follows (no unbounded ``resp.read()``).
    """
    base = urlsplit(config.upstream_base)
    url = urlunsplit((base.scheme, base.netloc, base.path + HEALTH_PROBE_PATH, "", ""))
    req = urllib.request.Request(
        url,
        headers={"Range": HEALTH_PROBE_RANGE, "User-Agent": "chelsa-cache-proxy/0.1"},
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=config.timeout_seconds) as resp:
            status = resp.status
            if status in (200, 206):
                try:
                    resp.read(_HEALTH_PROBE_READ_CAP)
                except (http.client.HTTPException, OSError):
                    pass
            return status
    except urllib.error.HTTPError as exc:
        try:
            exc.read(_HEALTH_PROBE_READ_CAP)
        except (http.client.HTTPException, OSError):
            pass
        return exc.code
    except (urllib.error.URLError, OSError):
        return 0


def make_handler(cache: Cache, config: Config):
    locks_guard = threading.Lock()
    # Keyed by exact (path, byte range) — CHELSA's addressable range space
    # is effectively unbounded (every distinct row/header/chunk ever read
    # is its own key), so a lock entry per key ever seen would grow without
    # bound over a long-running process (MINECRAFT-571 55861). Each entry
    # is [Lock, waiter_count]; it is removed as soon as no thread is still
    # using it, so the dict's size is bounded by concurrency, not by how
    # many distinct ranges have ever been requested.
    inflight: dict[str, list] = {}

    def singleflight_fetch(key: str, path: str, range_header: str, start: int, length: int):
        with locks_guard:
            entry = inflight.get(key)
            if entry is None:
                entry = [threading.Lock(), 0]
                inflight[key] = entry
            entry[1] += 1
        try:
            with entry[0]:
                cached = cache.get(key)
                if cached is not None:
                    return "warm", cached.status, cached.body
                status, body, complete = _forward(config, path, range_header, start, length)
                if status in (200, 206) and complete:
                    cache.put(key, CachedRange(status=status, body=body))
                    return "cold", status, body
                return "uncached", status, body
        finally:
            with locks_guard:
                entry[1] -= 1
                if entry[1] == 0 and inflight.get(key) is entry:
                    del inflight[key]

    class Handler(BaseHTTPRequestHandler):
        server_version = "chelsa-cache-proxy/0.1"

        def log_message(self, fmt, *args):  # noqa: D401 - silence default stderr logging
            pass

        def _health_status(self) -> int:
            """The dedicated TTL-0 probe: always a fresh live round trip,
            never reads from or writes to the data cache in either
            direction, regardless of what Range a caller passes (the
            ordinary data path never special-cases Range value at all —
            see the module docstring / MINECRAFT-571 55737).

            Normalized to exactly 200 (healthy) or 502 (not healthy) —
            never the underlying fetch's raw status (206 on a real
            success, or CHELSA's native 404/403 for a missing/forbidden
            byte range). Per John's 55789 finding on EarthStatus.reach()
            accepting "anything below 500" as healthy against a probe
            endpoint: a caller doing that check must have an exact "only
            200 means healthy" contract to check against. Matches the
            sibling elevation-cache-proxy's /__health contract.

            Uses the bounded ``_health_probe`` rather than the ordinary
            ``_forward`` used by the ``ALLOWED_PATHS`` data routes — see
            55839: an unbounded read here would mean every liveness check
            downloads the entire multi-megabyte DEM file if the upstream
            ever ignores the probe's Range request.
            """
            status = _health_probe(config)
            cache.record_probe_send()
            return 200 if status in (200, 206) else 502

        def do_HEAD(self) -> None:
            # EarthStatus.reach()'s own health check sends a plain HEAD (no
            # Range) to /__health, distinct from HostBackoff.check()'s GET
            # Range:bytes=0-0 recovery probe against the data path itself
            # (55816) — only /__health needs HEAD support. Status only, no
            # body, per plain HTTP HEAD semantics (MINECRAFT-571 55789/
            # 55808/55816/55820 — matches the fleet-wide /__health
            # do_HEAD convention added across the other proxy lanes).
            parsed = urlsplit(self.path)
            if parsed.path != "/__health":
                self._respond_plain(404, b"")
                return
            status = self._health_status()
            self.send_response(status)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_GET(self) -> None:
            parsed = urlsplit(self.path)
            if parsed.path == "/_internal/stats":
                self._respond_json(200, cache.stats() | {"attribution": ATTRIBUTION})
                return
            if parsed.path == "/__health":
                status = self._health_status()
                if status == 200:
                    self._respond_plain(200, b"")
                    return
                self._respond_plain(502, b"upstream unreachable or unhealthy")
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

            key = Cache.key(parsed.path, start, end)
            outcome, status, body = singleflight_fetch(key, parsed.path, range_header, start, end - start + 1)
            # A matching Content-Range is sent on every 206, cold or warm
            # alike (55861) — computed from the request itself, not stored
            # per entry, so cold and warm replies are trivially consistent
            # with each other by construction. Total length is unknown to
            # this proxy (it never reads a whole file), hence "*".
            content_range = f"bytes {start}-{end}/*" if status == 206 else None
            if outcome == "warm":
                cache.record_warm_hit()
                self._respond_bytes(status, body, content_range)
                return
            if outcome == "cold":
                cache.record_cold_miss()
                self._respond_bytes(status, body, content_range)
                return
            # uncached: error, transient failure, or truncated — never
            # stored, always retryable on the next request.
            cache.record_cold_miss()
            if status == 0:
                self._respond_plain(502, b"upstream unreachable")
                return
            if status in (200, 206):
                # The real client (RangeFiles.java) accepts ANY 200/206 as
                # success without itself re-checking the range — it trusts
                # this proxy's status code completely. 55870 found that
                # forwarding an invalid-but-200/206 response through as
                # "success" hands the client wrong bytes it has no way to
                # detect on its own (MINECRAFT-571 55874). A 200/206 that
                # failed this proxy's own validation must never reach the
                # client labeled as success — it becomes a retryable
                # failure instead, exactly like "upstream unreachable".
                self._respond_plain(502, b"invalid upstream range response")
                return
            # A real 404/403/5xx/etc. is forwarded through unmodified — the
            # client's own logic already knows what to do with those
            # (missing-vs-transient for 404/403, retry for anything else).
            self._respond_bytes(status, body, content_range)

        def _respond_bytes(self, status: int, body: bytes, content_range: str | None = None) -> None:
            self.send_response(status)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(len(body)))
            if content_range is not None:
                self.send_header("Content-Range", content_range)
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

    # Exposed for tests to confirm the single-flight lock map's size is
    # bounded by in-flight requests, not by how many distinct ranges have
    # ever been requested (MINECRAFT-571 55861) — not used by request
    # handling itself.
    Handler.inflight = inflight
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
