# chelsa-cache-proxy

A small, separately-deployable local SQLite byte-range cache proxy for
CHELSA V2's public object store. It sits in front of the real CHELSA
upstream and caches successful, complete range reads so repeat terrain
generation (Earth Platform, MINECRAFT-575 under the shared MINECRAFT-571
delivery) doesn't re-fetch the same GeoTIFF rows/elevation chunks from the
provider.

Status: **first slice**, offline-verified only. No live call has been made
against the real `os.zhdk.cloud.switch.ch` upstream; see "Provider terms"
below. The target repository `brooswit-intel/chelsa-cache-proxy` does not
exist yet (confirmed by Wroosbit, MINECRAFT-571 comment 55730) — this is
local, unpushed implementation per MINECRAFT-575's "continue local
implementation while repo setup is pending" direction.

## Why

- Earth Platform's `ClimateSource` (alpha.73 / combined-73, commit
  `88b13c33954550c6ac660dceb743a4288a9aafac`) reads CHELSA's public object
  store directly with HTTP range requests on every cold region. A local
  proxy gives one process a persistent, restart-proof cache of those exact
  byte ranges, so regeneration reads the same bytes offline.
- CHELSA V2.1 is released CC0 1.0 — there is no terms restriction blocking
  caching or republishing the bytes, so (unlike a provider with unclear
  terms) this slice is not blocked on a terms question; see "Provider
  terms" for what *is* still unconfirmed.

## Native request contract (reproduced from the real client)

Read directly from `io.earthplatform.geography.globe.ClimateSource` +
`RangeFiles` at the pinned alpha.73 commit:

- `GET <path>` with an exact `Range: bytes=<start>-<end>` header (end
  inclusive). No query string, no other method.
- Exactly five files, all relative to
  `https://os.zhdk.cloud.switch.ch/chelsav2/GLOBAL/` (dataset
  `chelsa-v2.1-1981-2010`):
  - `climatologies/1981-2010/bio/CHELSA_kg2_1981-2010_V.2.1.tif`
  - `climatologies/1981-2010/bio/CHELSA_bio1_1981-2010_V.2.1.tif`
  - `climatologies/1981-2010/bio/CHELSA_bio10_1981-2010_V.2.1.tif`
  - `climatologies/1981-2010/bio/CHELSA_bio12_1981-2010_V.2.1.tif`
  - `input/dem_geo.sdat`
- `404`/`403` mean "this row/file is genuinely missing" to the real
  client, and are never retried as transient; `200`/`206` mean success.
  This proxy forwards status and body through unmodified so the client
  keeps making that distinction itself — it does not reinterpret one as
  the other.
- A dedicated reachability probe (`HostBackoff.check`) sends
  `GET <path>` with exactly `Range: bytes=0-0` against whichever file URI
  the host is currently failing on. **This proxy does not special-case that
  byte-range shape on the ordinary data path.** An earlier revision did,
  and MINECRAFT-571 comment 55737 corrected that generally (the sibling
  elevation-cache-proxy made and then fixed the same mistake — see its
  comment 55766): a real `bytes=0-0` *data* request is the same shape but
  a different intent, and would otherwise never be cacheable. The TTL-0
  resource is instead the dedicated `GET /__health` endpoint below; wiring
  the real client's `HostBackoff` to call it (instead of probing a data
  file URI directly) when this proxy is configured is shared-infra work
  across every source, not CHELSA-specific, and is left to the
  MINECRAFT-580 coordinated integration lane.

## Health endpoint

`GET /__health` always makes one fresh live round trip to the real
upstream (a 1-byte read of the fixed DEM file) and never reads from or
writes to the data cache in either direction — TTL 0, explicitly, as
opposed to the ordinary data paths' TTL -1 (no time-based expiry). It is
counted separately (`probe_sends`) so it never inflates or deflates the
ordinary `cold_misses`/`warm_hits` data counters.

The response is normalized to exactly **`200`** (healthy) or **`502`**
(not healthy) — never the underlying fetch's raw status (a real success is
actually `206`; CHELSA's native `404`/`403` mean missing/forbidden on that
byte range). This matters because a caller is expected to check for
exactly `200`, not merely "anything below `500`" — MINECRAFT-571 comment
55789 found the mod's `EarthStatus.reach()` doing exactly that loose check
against a `/__health` endpoint, which would misread a `404`/`403` as
healthy. This endpoint's contract is deliberately narrow so that mistake
can't recur here: there is nothing but `200`/`502` to check against.
Matches the sibling `elevation-cache-proxy`'s own `/__health` contract.

## Running it

```sh
python3 -m chelsa_cache_proxy
```

Config is via environment variables, all optional:

| Var | Default | Meaning |
|---|---|---|
| `PROXY_BIND_HOST` | `127.0.0.1` | Bind address. Loopback-only by default. |
| `PROXY_BIND_PORT` | `8833` | Listen port. |
| `PROXY_SQLITE_PATH` | `./chelsa-cache.sqlite3` | Cache database file. A missing file starts empty. |
| `CHELSA_UPSTREAM_BASE` | `https://os.zhdk.cloud.switch.ch/chelsav2/GLOBAL` | Fixed, allowlisted HTTPS upstream origin. Point this at a fake/local upstream for any testing. |
| `PROXY_MAX_CACHE_BYTES` | `2147483648` (2 GiB) | Bound on total cached bytes; oldest-touched entries are evicted first once exceeded. |
| `PROXY_UPSTREAM_TIMEOUT_SECONDS` | `30` | Per-request timeout talking to the upstream. |

## Caching rules

- Only an exact, allowlisted path (the five files above) is served at all;
  anything else is rejected with `404` before any upstream send.
- Only a **complete, successful (`200`/`206`)** range read is cached.
  Errors, transient failures, and truncated reads (a declared
  `Content-Length` the delivered body didn't match) are never cached and
  remain retryable on the next request.
- Cache key is the exact `(path, byte range)` the client asked for — this
  is deterministic because the GeoTIFF/DEM layout is static, so the same
  row/chunk read always asks for the same range.
- If the upstream ignores the `Range` request and answers `200` with the
  whole file instead of `206` with just the requested bytes — the real
  CHELSA client defends against exactly this in `RangeFiles.java`, and the
  sibling MINECRAFT-573/JRC lane's review (MINECRAFT-581) found a proxy
  that didn't — this proxy slices the body down to the exact requested
  `[start, start+length)` range itself before caching or returning
  anything, so a small-range request can never cache (or return) an
  entire multi-megabyte file under one byte-range cache key.
- The ordinary data path never inspects the Range *value* to decide
  cacheability — a `bytes=0-0` data request is cached exactly like any
  other range. The TTL-0, always-live behavior lives only in the separate
  `/__health` endpoint (see above).
- Concurrent identical-range misses are deduplicated (single-flight): N
  simultaneous requests for the same uncached range produce exactly one
  upstream send, and every waiter gets that one result.
- The cache is bounded (`PROXY_MAX_CACHE_BYTES`); exceeding it evicts the
  least-recently-accessed entries, not a blanket flush.

## Metrics

`GET /_internal/stats` returns:

```json
{"cold_misses": 0, "warm_hits": 0, "probe_sends": 0, "cached_bytes": 0, "cached_entries": 0, "attribution": "..."}
```

All three counters are cumulative since the SQLite file was created (they
survive a process restart, unlike pure in-memory counters):

- `warm_hits` — a data range served entirely from SQLite, no network send.
- `cold_misses` — a data range that went out over the network (success,
  error, or truncated — every attempted data send is counted here exactly
  once, regardless of outcome).
- `probe_sends` — a `GET /__health` call, counted separately so it never
  inflates or deflates the data counters above.

Taking a before/after delta of this endpoint is the supported way to
assert "N cold, 0 warm" on a fresh region and "0 cold, N warm" on a repeat
read, without inferring it from logs.

## Provider terms

CHELSA V2.1 is released **CC0 1.0** (Karger et al. 2017, *Scientific Data*
4:170122) — no terms restriction blocks caching or republishing these
bytes. `GET /_internal/stats` echoes the attribution string so a consumer
surfacing it stays correct without re-deriving it. What is **not** yet
confirmed: the real upstream's exact behavior on a malformed/out-of-range
request, and its actual rate-limit/5xx shape under sustained load — this
slice's retry/backoff behavior has only been exercised against the local
`tests/fake_upstream.py`, never the live object store.

## Security notes

- Binds to loopback by default. There is no authentication in front of
  this proxy — anything that can reach `PROXY_BIND_HOST:PROXY_BIND_PORT`
  can use it. Don't bind it to a non-loopback address without adding that.
- No credentials are involved at all (CHELSA's public object store needs
  none), so there is nothing upstream-secret for this proxy to leak.

## Testing

```sh
python3 -m unittest discover -s tests -v
```

Tests spin up a local fake CHELSA-shaped upstream on loopback (no network
access, no live provider) and exercise:

- start-empty (fresh SQLite file has zero cached entries),
- cold-then-warm (a repeat identical range is served from cache with zero
  additional upstream sends),
- restart (a fresh `Cache`/`ProxyServer` object against the same SQLite
  file still serves the warm entry — simulating a process restart),
- error-not-cached (an upstream error is forwarded through unmodified and
  is never cached; the identical request is retried live next time),
- genuinely-missing-path (a path outside the allowlist is rejected before
  any upstream send, `404`),
- concurrent-miss-dedup (eight simultaneous identical-range misses
  collapse into exactly one upstream send),
- ordinary-bytes-0-0-is-cached (a real one-byte data request at offset
  zero is cached like any other range — proving the data path doesn't
  method/range-sniff for probe-shaped requests),
- health-endpoint-always-live (`/__health` always goes live, is never
  cached, and is counted separately from ordinary data reads),
- health-endpoint-fails-live-without-bypass-then-recovers (`/__health`
  against a down upstream fails live — no stale success — while the
  ordinary data cache stays warm through the same outage, and a
  subsequent health check after the upstream recovers succeeds),
- health-endpoint-never-leaks-a-non-5xx-status (a real, complete, non-2xx
  upstream answer still normalizes to `502`, never passed through as
  something a loose "status < 500" caller could misread as healthy),
- upstream-ignoring-range-is-sliced-down (an origin that answers a Range
  request with a full `200` instead of `206` is sliced down to the exact
  requested range before caching, never cached or returned as the whole
  file).
