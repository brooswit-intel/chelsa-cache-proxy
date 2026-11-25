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
  This proxy forwards `404`/`403` status and body through unmodified so
  the client keeps making that distinction itself.
- **This proxy always reports a successful data read as `206`, never
  `200`.** The real client (`RangeFiles.java`) has its own defensive
  re-slicing logic that triggers only on a raw `200` (`if (status == 200
  && (offset != 0 || body.length > length)) body =
  Arrays.copyOfRange(...)`). An earlier revision of this proxy sliced an
  origin's Range-ignoring `200` down to the requested bytes but kept
  reporting `200` — the real client then sliced the *already-sliced*
  body a second time at its own original absolute offset, producing an
  empty array even from a warm proxy cache (MINECRAFT-571 comment
  55861). Reporting `206` tells the client the body is already exactly
  what it asked for.
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

Both `GET /__health` and `HEAD /__health` are supported — `HEAD` returns
status only, no body, matching `EarthStatus.reach()`'s own plain HEAD
probe (MINECRAFT-571 55789/55808/55816/55820). This is distinct from
`HostBackoff.check()`'s recovery probe, which sends `GET` with
`Range: bytes=0-0` directly against the data path itself (see "Native
request contract" above and 55816) — `/__health` is not involved in that
one at all. A `HEAD` on any other path is `404`; this proxy's data paths
are never read with `HEAD`.

`GET /__health` always makes one fresh live round trip to the real
upstream (a 1-byte read of the fixed DEM file) and never reads from or
writes to the data cache in either direction — TTL 0, explicitly, as
opposed to the ordinary data paths' TTL -1 (no time-based expiry). It is
counted separately (`probe_sends`) so it never inflates or deflates the
ordinary `cold_misses`/`warm_hits` data counters.

The probe's own read is bounded to a couple of bytes, regardless of what
the upstream actually sends: MINECRAFT-571 comment 55839 found the sibling
WorldCover/JRC health checks doing an unbounded read before validating
status/range, meaning a liveness check could end up downloading an entire
multi-megabyte file if the origin ever ignored the probe's Range request.
This endpoint uses a dedicated bounded fetch (`_health_probe`), separate
from the ordinary data path's `_forward` (which legitimately needs the
full body to validate and cache a real data read).

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
  entire multi-megabyte file under one byte-range cache key. The reported
  status is normalized to `206` (see "Native request contract" above),
  never left as `200`.
- A real `206` is validated against the length actually *requested*, not
  merely against its own declared `Content-Length` — a `206` that answers
  with the wrong range (same declared and delivered length, just the
  wrong bytes) is still forwarded through to the client as-is, but is
  never written to the cache under this key (MINECRAFT-571 55861).
- Every `206` response — cold or warm, sliced-from-200 or a real upstream
  `206` — carries a `Content-Range: bytes <start>-<end>/*` header computed
  from the request itself, so cold and warm replies for the same range
  are trivially identical (the total length after `/` is unknown to this
  proxy, which never reads a whole file, hence `*`).
- The ordinary data path never inspects the Range *value* to decide
  cacheability — a `bytes=0-0` data request is cached exactly like any
  other range. The TTL-0, always-live behavior lives only in the separate
  `/__health` endpoint (see above).
- Concurrent identical-range misses are deduplicated (single-flight): N
  simultaneous requests for the same uncached range produce exactly one
  upstream send, and every waiter gets that one result. The lock map
  entry for a key is removed as soon as no thread is using it — CHELSA's
  addressable range space is effectively unbounded (every distinct row/
  header ever read is its own key), so keeping one lock per key ever
  seen, rather than per key currently in flight, would grow without bound
  over a long-running process (MINECRAFT-571 55861).
- A real (non-"never reached the upstream") error response's body is
  bounded the same way a success response's is (`_MAX_FORWARD_BYTES`) —
  the `HTTPError` branch's read was previously unbounded, the same risk
  shape as the success path (55861).
- The cache is bounded (`PROXY_MAX_CACHE_BYTES`); exceeding it evicts the
  least-recently-accessed entries, not a blanket flush.
- A single forwarded response — declared or actually delivered — over
  64 MiB (`_MAX_FORWARD_BYTES`) is rejected before being fully read, never
  cached. The real DEM file is the whole global raw-float grid and the
  GeoTIFF files are each hundreds of MB; if the upstream ever ignores
  Range on one of those, an unbounded read would pull the entire file
  into memory for one small request — the sibling MINECRAFT-577 lanes
  (HRDEM's own 1-10 GB mosaics) hit the identical risk and added the same
  kind of ceiling (MINECRAFT-571 55839/55846). This is generous headroom
  for CHELSA's own actual row/header sizes (a few hundred KiB at most),
  not a tight budget.

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

## Cache migration

On open, the cache purges any stored entry with `status = 200`
(`DELETE FROM entries WHERE status = 200`). Under the current contract no
entry is ever written with `status` `200` — a sliced success is always
normalized to `206` (see "Native request contract" above) — so a `200`
row can only be a leftover from the earlier, incompatible prototype
behavior, which a real client would double-slice into an empty or wrong
body (MINECRAFT-571 55861). This runs on every `Cache(...)` construction;
it is a no-op once no such rows remain.

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
  file),
- health-endpoint-supports-head (`HEAD /__health` matches
  `EarthStatus.reach()`'s plain HEAD probe: status only, no body; `HEAD`
  on anything else is `404`),
- health-probe-does-not-read-a-range-ignoring-whole-file (against a large
  fixture file with Range ignored, the probe's connection closes long
  before the whole body goes out, and the data cache stays untouched),
- declared-oversized-response-is-rejected-before-reading (a declared
  Content-Length over the 64 MiB ceiling is rejected and never cached,
  with the real status still forwarded through; a normal request right
  after still works),
- 206-sends-matching-content-range-cold-and-warm and
  sliced-200-also-gets-a-matching-content-range (every successful data
  response — real 206 or sliced-200-reported-as-206 — carries a
  `Content-Range` identical across cold and warm replies),
- 206-with-wrong-length-is-rejected-not-cached (a real 206 whose body
  doesn't match the requested length is forwarded through as-is but never
  cached),
- http-error-body-is-bounded (the error-response read is capped the same
  way a success response's is),
- inflight-lock-map-does-not-grow-without-bound (the single-flight lock
  map's size stays at zero between requests across many distinct ranges,
  rather than growing with every unique range ever seen).
