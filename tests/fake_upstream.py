"""A loopback-only, credential-free fake CHELSA-shaped upstream for tests.

Nothing here ever reaches the real CHELSA object store
(``os.zhdk.cloud.switch.ch``); it serves a handful of allowlisted paths with
range-request semantics close enough to a real HTTP file server to exercise
this proxy's caching, concurrency, and probe behavior without any live
provider call.
"""

from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, HTTPServer


class FakeUpstream:
    def __init__(self, files: dict[str, bytes]) -> None:
        """``files`` maps an allowlisted path (e.g. ``/input/dem_geo.sdat``)
        to its full byte content.
        """
        self.files = files
        self.call_count = 0
        self.range_calls: list[tuple[str, str | None]] = []
        self.fail_next = False
        self.down = False
        # Simulates an origin that ignores the Range header entirely and
        # always returns the whole file with 200 — real CHELSA client code
        # defends against exactly this (RangeFiles.java), and the sibling
        # MINECRAFT-573/JRC lane's review found a proxy that didn't
        # (MINECRAFT-581).
        self.ignore_range_next = False
        # How many bytes of the last ignore-Range 200 response the client
        # actually consumed before closing the connection (or the full
        # body, if it read to the end) — proves a bounded-read caller
        # (the /__health probe) never pulls in a huge body just to check
        # liveness. Written in small chunks so an early client disconnect
        # (BrokenPipeError) is observed well before the whole body would
        # otherwise have gone out.
        self.last_sent_bytes = 0
        # Declares a Content-Length far larger than the actual body sent —
        # simulates an upstream claiming a response bigger than this
        # proxy's defensive ceiling, to prove it's rejected before any
        # read is attempted (not just after a slow full download).
        self.huge_content_length_next = False
        # Serves a real 206 whose body is a different length than the
        # requested range (its own declared Content-Length matches its own
        # — wrongly sized — body, so the ordinary truncation check alone
        # wouldn't catch this; only validating against the *requested*
        # length does, per MINECRAFT-571 55861).
        self.wrong_length_206_next = False
        # Serves a real 206 with the CORRECT requested length but the
        # WRONG actual bytes (and a Content-Range header naming the real,
        # wrong offsets it served) -- the exact "equal-length, wrong-offset"
        # case MINECRAFT-571 comment 55866 found was not being checked.
        self.wrong_offset_206_next: tuple[int, int] | None = None
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, fmt, *args) -> None:  # noqa: D401
                pass

            def do_GET(self) -> None:
                outer.call_count += 1
                range_header = self.headers.get("Range")
                outer.range_calls.append((self.path, range_header))

                if outer.down:
                    self.connection.close()
                    return

                if outer.huge_content_length_next:
                    outer.huge_content_length_next = False
                    payload = b"short body, bogus declared length"
                    self.send_response(206)
                    self.send_header("Content-Type", "application/octet-stream")
                    self.send_header("Content-Length", str(200 << 20))  # 200 MiB, a lie
                    self.end_headers()
                    self.wfile.write(payload)
                    return

                if outer.fail_next:
                    outer.fail_next = False
                    payload = b"simulated upstream failure"
                    self.send_response(503)
                    self.send_header("Content-Type", "text/plain")
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                    return

                content = outer.files.get(self.path)
                if content is None:
                    payload = b"not found"
                    self.send_response(404)
                    self.send_header("Content-Type", "text/plain")
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                    return

                ignore_range = outer.ignore_range_next
                if ignore_range:
                    outer.ignore_range_next = False
                if not range_header or not range_header.startswith("bytes=") or ignore_range:
                    self.send_response(200)
                    self.send_header("Content-Type", "application/octet-stream")
                    self.send_header("Content-Length", str(len(content)))
                    self.end_headers()
                    sent = 0
                    chunk_size = 64
                    try:
                        for offset in range(0, len(content), chunk_size):
                            self.wfile.write(content[offset:offset + chunk_size])
                            sent += min(chunk_size, len(content) - offset)
                    except (BrokenPipeError, ConnectionResetError):
                        pass
                    outer.last_sent_bytes = sent
                    return

                start_s, _, end_s = range_header[len("bytes="):].partition("-")
                start, end = int(start_s), int(end_s)
                body = content[start:end + 1]

                if outer.wrong_offset_206_next is not None:
                    wrong_start, wrong_end = outer.wrong_offset_206_next
                    outer.wrong_offset_206_next = None
                    body = content[wrong_start:wrong_end + 1]
                    self.send_response(206)
                    self.send_header("Content-Type", "application/octet-stream")
                    self.send_header("Content-Length", str(len(body)))
                    self.send_header(
                        "Content-Range", f"bytes {wrong_start}-{wrong_end}/{len(content)}"
                    )
                    self.end_headers()
                    self.wfile.write(body)
                    return

                if outer.wrong_length_206_next:
                    outer.wrong_length_206_next = False
                    body = body + b"\x00"  # one byte too many, still a real 206
                self.send_response(206)
                self.send_header("Content-Type", "application/octet-stream")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Content-Range", f"bytes {start}-{end}/{len(content)}")
                self.end_headers()
                self.wfile.write(body)

        self._httpd = HTTPServer(("127.0.0.1", 0), Handler)
        self.port = self._httpd.server_address[1]
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True)

    def start(self) -> "FakeUpstream":
        self._thread.start()
        return self

    def stop(self) -> None:
        self._httpd.shutdown()
        self._httpd.server_close()

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"
