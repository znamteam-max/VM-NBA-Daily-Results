#!/usr/bin/env python3
"""Backup bridge to NBA's own data on the owner's servers (Stockholm, Warsaw) — the same API as the Cloudflare
Worker nba-data-bridge: GET /<cdn.nba.com|stats.nba.com|www.nba.com>/<path>?<query> with header x-bridge-key.

Why raw TLS + HTTP/1.1: Akamai in front of NBA answers 403 to curl/urllib3-style clients from these data-centre
addresses, but a plain HTTP/1.1 request over a Python TLS socket gets 200 (checked 2026-10-22 from both servers).
Not an open proxy: without the key nothing is forwarded; only NBA's data hosts, only GET, only safe path characters.
Stdlib only. Key and certificate come from systemd credentials (see nba-relay.service).
"""
from __future__ import annotations

import gzip
import hmac
import os
import re
import socket
import ssl
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

HOSTS = {"cdn.nba.com", "stats.nba.com", "www.nba.com"}
SAFE_PATH = re.compile(r"[A-Za-z0-9/._~\-?=&%:+,]*")  # no spaces, CR/LF or quotes: nothing can be injected upstream
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
MAX_BODY = 20 * 1024 * 1024
UPSTREAM_TIMEOUT = 20
CREDS = Path(os.environ.get("CREDENTIALS_DIRECTORY", "/etc/nba-relay"))
KEY = (CREDS / "key").read_text(encoding="utf-8").strip().encode("utf-8")
if len(KEY) < 32:  # an empty or truncated key file must never open the relay to everyone
    sys.exit("nba-relay: key missing or too short")
MAX_CONNECTIONS = 32  # one CPU core on these VPN servers: refuse extra connections instead of piling up threads
CONNECTION_DEADLINE = 20  # seconds for a whole connection (TLS handshake + request), not per read
CONTROL_CHARS = {c: f"\\x{c:02x}" for c in [*range(0x20), *range(0x7F, 0xA0)]}


def upstream(host: str, path: str) -> tuple[int, str, bytes]:
    """One GET to NBA over a fresh TLS socket, HTTP/1.1, Connection: close."""
    extra = "x-nba-stats-origin: stats\r\nx-nba-stats-token: true\r\n" if host == "stats.nba.com" else ""
    request = (f"GET {path} HTTP/1.1\r\nHost: {host}\r\nUser-Agent: {UA}\r\n"
               "Accept: application/json, text/plain, */*\r\nAccept-Language: en-US,en;q=0.9\r\n"
               f"Referer: https://www.nba.com/\r\nOrigin: https://www.nba.com\r\n{extra}"
               "Accept-Encoding: identity\r\nConnection: close\r\n\r\n")
    raw = socket.create_connection((host, 443), timeout=UPSTREAM_TIMEOUT)
    with ssl.create_default_context().wrap_socket(raw, server_hostname=host) as sock:
        sock.settimeout(UPSTREAM_TIMEOUT)
        sock.sendall(request.encode("ascii"))
        chunks, size = [], 0
        while True:
            part = sock.recv(65536)
            if not part:
                break
            size += len(part)
            if size > MAX_BODY:
                raise ValueError("upstream answer too large")
            chunks.append(part)
    head, _, body = b"".join(chunks).partition(b"\r\n\r\n")
    lines = head.decode("iso-8859-1").split("\r\n")
    status = int(lines[0].split()[1])
    headers = {k.strip().lower(): v.strip() for k, _, v in (l.partition(":") for l in lines[1:])}
    if headers.get("transfer-encoding", "").lower() == "chunked":
        body = dechunk(body)
    return status, headers.get("content-type", "application/json"), body


def dechunk(data: bytes) -> bytes:
    out, pos = [], 0
    while True:
        end = data.index(b"\r\n", pos)
        length = int(data[pos:end].split(b";")[0], 16)
        if length == 0:
            return b"".join(out)
        out.append(data[end + 2:end + 2 + length])
        pos = end + 2 + length + 2


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "nba-relay"
    sys_version = ""

    def _send(self, status: int, body: bytes, ctype: str = "text/plain; charset=utf-8") -> None:
        if len(body) > 1024 and "gzip" in self.headers.get("Accept-Encoding", ""):
            body, encoding = gzip.compress(body, 5), "gzip"
        else:
            encoding = None
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        if encoding:
            self.send_header("Content-Encoding", encoding)
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)
        self.close_connection = True

    def do_GET(self) -> None:  # noqa: N802
        started = time.time()
        given = self.headers.get("x-bridge-key", "").encode("utf-8", "surrogateescape")
        if not hmac.compare_digest(given, KEY):
            return self._send(403, b"forbidden")
        _, host, rest = (self.path.split("/", 2) + ["", ""])[:3]
        rest = "/" + rest
        if host not in HOSTS or not SAFE_PATH.fullmatch(rest):
            return self._send(400, b"host or path not allowed")
        try:
            status, ctype, body = upstream(host, rest)
        except Exception as exc:  # timeouts, resets, malformed answers
            self.log_message("upstream %s failed: %s", host, type(exc).__name__)
            return self._send(502, b"upstream failed")
        self.log_message("%s %s -> %d %d bytes %.1f s", host, rest.split("?")[0], status, len(body), time.time() - started)
        self._send(status, body, ctype)

    def do_POST(self) -> None:  # noqa: N802
        self._send(405, b"GET only")

    do_PUT = do_DELETE = do_PATCH = do_HEAD = do_OPTIONS = do_POST

    def log_message(self, fmt: str, *args) -> None:  # one short line per request, never the key
        line = (fmt % args).translate(CONTROL_CHARS)  # no raw terminal escapes from strangers' request lines
        sys.stderr.write(f"{self.client_address[0]} {line}\n")


class Server(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 64

    def __init__(self, address, context: ssl.SSLContext):
        super().__init__(address, Handler)
        self.context = context
        self.slots = threading.BoundedSemaphore(MAX_CONNECTIONS)

    def get_request(self):
        try:
            return super().get_request()
        except OSError:  # e.g. out of file descriptors: back off instead of spinning a full core
            time.sleep(0.5)
            raise

    def process_request(self, request, client_address) -> None:
        if not self.slots.acquire(blocking=False):
            self.shutdown_request(request)  # too many connections at once: drop this one
            return
        super().process_request(request, client_address)

    def process_request_thread(self, request, client_address) -> None:
        try:
            super().process_request_thread(request, client_address)
        finally:
            self.slots.release()

    def finish_request(self, request, client_address) -> None:
        request.settimeout(10)  # TLS handshake in the worker thread, so a slow client cannot block accept()
        deadline = threading.Timer(CONNECTION_DEADLINE, lambda: _close(request))
        deadline.daemon = True
        deadline.start()
        try:
            tls = self.context.wrap_socket(request, server_side=True)
            super().finish_request(tls, client_address)
        except (ssl.SSLError, OSError):
            pass
        finally:
            deadline.cancel()

    def handle_error(self, request, client_address) -> None:  # one line, no traceback per broken client
        exc = sys.exc_info()[1]
        if not isinstance(exc, (ConnectionError, ssl.SSLError, TimeoutError)):
            sys.stderr.write(f"{client_address[0]} error {type(exc).__name__}\n")


def _close(sock) -> None:
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass


def main() -> None:
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_cert_chain(CREDS / "cert.pem", CREDS / "cert.key")
    port = int(os.environ.get("RELAY_PORT", "8790"))
    Server(("0.0.0.0", port), context).serve_forever()


if __name__ == "__main__":
    main()
