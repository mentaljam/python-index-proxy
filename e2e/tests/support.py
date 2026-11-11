"""Local package repository, real proxy process, and isolated package clients."""

from __future__ import annotations

import base64
import csv
import hashlib
import html
import io
import json
import os
import socket
import subprocess
import sys
import threading
import time
from contextlib import suppress
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Literal, Protocol, TextIO, cast, override
from urllib.parse import unquote, urlsplit
from urllib.request import ProxyHandler, build_opener
from zipfile import ZIP_STORED, ZipFile

import pytest


def isolated_env(home: Path) -> dict[str, str]:
    """Do not inherit indexes, proxies, Python paths, or the active uv environment."""
    home.mkdir(parents=True, exist_ok=True)
    env = {
        key: os.environ[key]
        for key in ("PATH", "SYSTEMROOT", "LANG")
        if key in os.environ
    }
    env.update(
        HOME=str(home),
        USERPROFILE=str(home),
        XDG_CONFIG_HOME=str(home / "config"),
        XDG_CACHE_HOME=str(home / "cache"),
        PIP_CONFIG_FILE=os.devnull,
        UV_PYTHON_DOWNLOADS="never",
        UV_HTTP_TIMEOUT="10",
        UV_HTTP_RETRIES="0",
        NO_PROXY="127.0.0.1,localhost",
    )
    return env


def run(
    command: list[str], *, cwd: Path, env: dict[str, str], timeout: int = 60
) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(  # noqa: S603 - Harness-built argument list; no shell.
            command,
            cwd=cwd,
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as error:
        pytest.fail(
            f"Command timed out: {command!r}\nstdout: {error.stdout}\nstderr: {error.stderr}"
        )


@dataclass(frozen=True, slots=True)
class Wheel:
    name: str
    version: str
    filename: str
    content: bytes
    metadata: bytes

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.content).hexdigest()


def make_wheel(
    name: str,
    version: str,
    source: str,
    *,
    requires: tuple[str, ...] = (),
    payload: bytes | None = None,
) -> Wheel:
    """Build a pure Python wheel locally, including a complete RECORD (PEP 427)."""
    module = name.replace("-", "_")
    dist_info = f"{module}-{version}.dist-info"
    metadata = (
        f"Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n"
        + "".join(f"Requires-Dist: {requirement}\n" for requirement in requires)
        + "\n"
    ).encode()
    files = {
        f"{module}/__init__.py": source.encode(),
        f"{dist_info}/METADATA": metadata,
        f"{dist_info}/WHEEL": (
            b"Wheel-Version: 1.0\nGenerator: pipr-e2e\n"
            b"Root-Is-Purelib: true\nTag: py3-none-any\n"
        ),
    }
    if payload is not None:
        files[f"{module}/payload.bin"] = payload
    record = io.StringIO(newline="")
    writer = csv.writer(record, lineterminator="\n")
    for path, data in files.items():
        digest = (
            base64.urlsafe_b64encode(hashlib.sha256(data).digest())
            .rstrip(b"=")
            .decode()
        )
        writer.writerow((path, f"sha256={digest}", len(data)))
    writer.writerow((f"{dist_info}/RECORD", "", ""))
    files[f"{dist_info}/RECORD"] = record.getvalue().encode()
    buffer = io.BytesIO()
    with ZipFile(buffer, "w", compression=ZIP_STORED) as archive:
        for path, data in files.items():
            archive.writestr(path, data)
    return Wheel(
        name,
        version,
        f"{module}-{version}-py3-none-any.whl",
        buffer.getvalue(),
        metadata,
    )


@dataclass(frozen=True, slots=True)
class Reply:
    status: int
    body: bytes = b""
    content_type: str = "text/html; charset=utf-8"
    location: str | None = None
    transfer: TransferGate | None = None
    content_encoding: str | None = None


class TransferGate:
    """Pause a response after an observable prefix, with bounded shutdown."""

    __slots__ = ("release", "started", "timeout")

    def __init__(self, timeout: float = 15) -> None:
        self.started = threading.Event()
        self.release = threading.Event()
        self.timeout = timeout

    def send(self, handler: BaseHTTPRequestHandler, body: bytes) -> None:
        midpoint = len(body) // 2
        handler.wfile.write(body[:midpoint])
        handler.wfile.flush()
        self.started.set()
        if not self.release.wait(self.timeout):
            handler.close_connection = True
            return
        handler.wfile.write(body[midpoint:])


class Upstream:
    __slots__ = ("lock", "requests", "routes", "server", "thread", "url")

    def __init__(self) -> None:  # noqa: C901 - Local HTTP handler keeps fixture state isolated.
        self.routes: dict[str, Reply] = {}
        self.requests: list[str] = []
        self.lock = threading.Lock()
        upstream = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_GET(self) -> None:
                self.respond(head=False)

            def do_HEAD(self) -> None:
                self.respond(head=True)

            def respond(self, *, head: bool) -> None:
                parsed = urlsplit(self.path)
                path = unquote(parsed.path)
                if parsed.query:
                    path += "?" + parsed.query
                with upstream.lock:
                    if not head:
                        upstream.requests.append(path)
                    reply = upstream.routes.get(path, Reply(404, b"not found"))
                self.send_response(reply.status)
                self.send_header("Content-Type", reply.content_type)
                self.send_header("Content-Length", str(len(reply.body)))
                if reply.content_encoding is not None:
                    self.send_header("Content-Encoding", reply.content_encoding)
                if reply.location is not None:
                    self.send_header("Location", reply.location)
                self.end_headers()
                if head:
                    return
                with suppress(BrokenPipeError, ConnectionResetError):
                    if reply.transfer is None:
                        self.wfile.write(reply.body)
                    else:
                        reply.transfer.send(self, reply.body)

            @override
            def log_message(self, format: str, *args: object) -> None:
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}"

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)

    def reply(self, path: str, response: Reply) -> None:
        with self.lock:
            self.routes[path] = response

    def journal(self) -> list[str]:
        with self.lock:
            return list(self.requests)

    def artifact_requests(self) -> list[str]:
        return [path for path in self.journal() if urlsplit(path).path.endswith(".whl")]

    def publish(
        self,
        wheel: Wheel,
        *,
        metadata: bool = True,
        href: str | None = None,
        digest: str | None = None,
        unquoted: bool = False,
    ) -> None:
        artifact_path = f"/files/{wheel.filename}"
        metadata_path = artifact_path + ".metadata"
        self.reply(artifact_path, Reply(200, wheel.content, "application/octet-stream"))
        self.reply(
            metadata_path, Reply(200, wheel.metadata, "application/octet-stream")
        )
        link = href or f"{artifact_path}#sha256={digest or wheel.sha256}"
        metadata_hash = hashlib.sha256(wheel.metadata).hexdigest()
        attributes = (
            f' data-core-metadata="sha256={metadata_hash}"'
            f' data-dist-info-metadata="sha256={metadata_hash}"'
            if metadata
            else ""
        )
        # An unquoted href without '=' is valid HTML; it deliberately omits a fragment.
        anchor = (
            f"href={artifact_path}"
            if unquoted
            else f'href="{html.escape(link, quote=True)}"'
        )
        page = (
            "<!DOCTYPE html>\n<html><body>\n"
            f"<a {anchor}{attributes}>{wheel.filename}</a>\n"
            "</body></html>\n"
        ).encode()
        self.reply(f"/simple/{wheel.name}/", Reply(200, page))

    def deny_artifacts(self) -> None:
        with self.lock:
            for path in list(self.routes):
                if urlsplit(path).path.endswith((".whl", ".metadata")):
                    self.routes[path] = Reply(503, b"artifact upstream is unavailable")

    def deny_all(self) -> None:
        with self.lock:
            self.routes = {
                path: Reply(503, b"upstream is unavailable") for path in self.routes
            }


class Proxy:
    __slots__ = (
        "binary",
        "config",
        "log_file",
        "opener",
        "path_prefix",
        "port",
        "process",
        "reservation",
        "root",
        "upstream",
        "url",
    )

    def __init__(  # noqa: PLR0913 - Explicit process, storage and source isolation options.
        self,
        binary: Path,
        root: Path,
        upstream: Upstream,
        path_prefix: str = "/",
        *,
        storage_root: Path | None = None,
        index_templates: tuple[str, ...] | None = None,
        public_url: str | None = None,
    ) -> None:
        self.binary = binary
        self.root = root
        self.upstream = upstream
        self.path_prefix = path_prefix
        self.process: subprocess.Popen[bytes] | None = None
        self.log_file: TextIO | None = None
        # Reserve a port while writing configuration. Release immediately before spawn.
        self.reservation: socket.socket | None = socket.socket()
        self.reservation.bind(("127.0.0.1", 0))
        self.port = self.reservation.getsockname()[1]
        self.url = f"http://127.0.0.1:{self.port}{path_prefix}"
        self.config = root / "settings.toml"
        templates = (
            index_templates
            if index_templates is not None
            else (upstream.url + "/simple/{package}/",)
        )
        self.config.write_text(
            f'bind = "127.0.0.1:{self.port}"\n'
            f"path_prefix = {json.dumps(path_prefix)}\n"
            + (
                f"public_url = {json.dumps(public_url)}\n"
                if public_url is not None
                else ""
            )
            + "[indices]\n"
            f"default = {json.dumps(templates)}\n"
            "[storage.fs]\n"
            f"root = {json.dumps(str(storage_root if storage_root is not None else root / 'artifacts'))}\n"
        )
        self.opener = build_opener(ProxyHandler({}))

    @property
    def index_url(self) -> str:
        return self.url + "default/"

    def start(self) -> None:
        if self.reservation is not None:
            self.reservation.close()
            self.reservation = None
        self.log_file = self.root.joinpath("proxy.log").open("a")
        env = isolated_env(self.root / "proxy-home")
        env.update(RUST_LOG="python_index_proxy=debug", NO_COLOR="1")
        self.process = subprocess.Popen(  # noqa: S603 - Known binary and test config; no shell.
            [str(self.binary), str(self.config)],
            cwd=self.root,
            env=env,
            stdout=self.log_file,
            stderr=subprocess.STDOUT,
        )
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                pytest.fail(f"Proxy exited before readiness\n{self.diagnostics()}")
            try:
                with self.opener.open(self.url, timeout=0.5) as response:
                    if response.status == 200:
                        return
            except OSError:
                time.sleep(0.05)
        pytest.fail(f"Proxy did not become ready\n{self.diagnostics()}")

    def stop(self) -> None:
        if self.process is not None:
            if self.process.poll() is None:
                self.process.terminate()
                try:
                    self.process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait(timeout=5)
            self.process = None
        if self.log_file is not None:
            self.log_file.close()
            self.log_file = None
        if self.reservation is not None:
            self.reservation.close()
            self.reservation = None

    def restart(self) -> None:
        self.stop()
        self.start()

    def get(self, url: str) -> bytes:
        with self.opener.open(url, timeout=10) as response:
            return cast("bytes", response.read())

    def diagnostics(self) -> str:
        log = self.root / "proxy.log"
        return (
            f"Proxy log:\n{log.read_text() if log.exists() else '(not started)'}\n"
            f"Upstream requests:\n{json.dumps(self.upstream.journal(), indent=2)}"
        )


class ProxyFactory(Protocol):
    def __call__(
        self,
        path_prefix: str = "/",
        *,
        storage_root: Path | None = None,
        index_templates: tuple[str, ...] | None = None,
        public_url: str | None = None,
    ) -> Proxy: ...


class Client:
    __slots__ = ("counter", "kind", "root", "uv")

    def __init__(self, kind: Literal["pip", "uv"], root: Path, uv: str) -> None:
        self.kind = kind
        self.root = root
        self.uv = uv
        self.counter = 0

    def environment(self) -> Path:
        self.counter += 1
        directory = self.root / f"{self.kind}-{self.counter}"
        command = [sys.executable, "-I", "-m", "venv"]
        if self.kind == "uv":
            command.append("--without-pip")
        result = run(
            [*command, str(directory)],
            cwd=self.root,
            env=isolated_env(self.root / "client-home"),
        )
        assert result.returncode == 0, result.stdout + result.stderr
        return (
            directory.joinpath("Scripts", "python.exe")
            if os.name == "nt"
            else directory.joinpath("bin", "python")
        )

    def install(
        self, python: Path, index_url: str, requirement: str
    ) -> subprocess.CompletedProcess[str]:
        if self.kind == "pip":
            command = [
                str(python),
                "-I",
                "-m",
                "pip",
                "--disable-pip-version-check",
                "--no-cache-dir",
                "--no-input",
                "install",
                "--retries",
                "0",
                "--timeout",
                "10",
            ]
        else:
            command = [
                self.uv,
                "--no-config",
                "--no-cache",
                "--no-python-downloads",
                "pip",
                "install",
                "--python",
                str(python),
            ]
        return run(
            [*command, "--only-binary=:all:", "--index-url", index_url, requirement],
            cwd=self.root,
            env=isolated_env(self.root / "client-home"),
        )

    def inspect(self, python: Path) -> dict[str, str]:
        result = run(
            [
                str(python),
                "-I",
                "-c",
                (
                    "import json; from importlib.metadata import version; "
                    "from pipr_e2e_app import VALUE; "
                    "print(json.dumps({'app': version('pipr-e2e-app'), "
                    "'dependency': version('pipr-e2e-dep'), 'value': VALUE}))"
                ),
            ],
            cwd=self.root,
            env=isolated_env(self.root / "client-home"),
        )
        assert result.returncode == 0, result.stdout + result.stderr
        return cast("dict[str, str]", json.loads(result.stdout))

    def assert_absent(self, python: Path, name: str) -> None:
        result = run(
            [
                str(python),
                "-I",
                "-c",
                (
                    "from importlib.metadata import distributions; "
                    f"assert {name!r} not in [d.metadata['Name'] for d in distributions()]"
                ),
            ],
            cwd=self.root,
            env=isolated_env(self.root / "client-home"),
        )
        assert result.returncode == 0, result.stdout + result.stderr


def require_success(result: subprocess.CompletedProcess[str], proxy: Proxy) -> None:
    assert result.returncode == 0, (
        f"Command: {result.args!r}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}\n"
        + proxy.diagnostics()
    )
