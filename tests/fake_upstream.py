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

                if not range_header or not range_header.startswith("bytes="):
                    self.send_response(200)
                    self.send_header("Content-Type", "application/octet-stream")
                    self.send_header("Content-Length", str(len(content)))
                    self.end_headers()
                    self.wfile.write(content)
                    return

                start_s, _, end_s = range_header[len("bytes="):].partition("-")
                start, end = int(start_s), int(end_s)
                body = content[start:end + 1]
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
