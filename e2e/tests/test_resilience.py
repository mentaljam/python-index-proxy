"""Cross-process contracts not exercised by the Rust unit tests."""

from __future__ import annotations

import html
import http.client
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import override
from urllib.error import HTTPError
from urllib.parse import unquote, urljoin, urlsplit

import pytest

from support import (
    Client,
    Proxy,
    Reply,
    TransferGate,
    Upstream,
    Wheel,
    isolated_env,
    make_wheel,
    require_success,
    run,
)
from test_clients import EXPECTED, Links


def artifact_url(proxy: Proxy, wheel: Wheel) -> str:
    parser = Links()
    data = proxy.get(f"{proxy.index_url}{wheel.name}/").decode()
    parser.feed(data)
    assert len(parser.hrefs) == 1
    return urljoin(proxy.index_url, parser.hrefs[0]).split("#", 1)[0]


def large_app(upstream: Upstream) -> Wheel:
    wheel = make_wheel(
        "pipr-e2e-app",
        "1.0.0",
        "from pipr_e2e_dep import VALUE as DEP\nVALUE = 'app:' + DEP\n",
        requires=("pipr-e2e-dep==2.0.0",),
        payload=bytes(range(256)) * (64 * 1024),
    )
    upstream.publish(wheel, metadata=False)
    return wheel


@pytest.mark.usefixtures("wheels")
def test_cancelled_download_allows_clean_client_retry(
    client: Client, proxy: Proxy, upstream: Upstream
) -> None:
    wheel = large_app(upstream)
    gate = TransferGate()
    upstream.reply(
        f"/files/{wheel.filename}",
        Reply(200, wheel.content, "application/octet-stream", transfer=gate),
    )
    url = urlsplit(artifact_url(proxy, wheel))
    assert url.hostname is not None
    connection = http.client.HTTPConnection(url.hostname, url.port, timeout=10)
    temporary = proxy.root.joinpath("artifacts", ".tmp")
    try:
        connection.request("GET", url.path)
        response = connection.getresponse()
        assert response.status == 200
        assert gate.started.wait(10), "upstream transfer must have started"
        assert response.read(1) == wheel.content[:1]
        # Disconnect after observing proxy output, while upstream is still paused.
        assert not gate.release.is_set()
        assert list(temporary.rglob("*")), (
            "streaming write must be active before disconnect"
        )
        response.close()
        connection.close()
        deadline = time.monotonic() + 5
        while any(temporary.rglob("*")) and time.monotonic() < deadline:
            threading.Event().wait(0.05)
        assert not list(temporary.rglob("*")), (
            "disconnect must abort without waiting for upstream"
        )
        assert not [
            path
            for path in proxy.root.joinpath("artifacts").rglob("*")
            if path.is_file() and temporary not in path.parents
        ], "cancelled download must not publish an incomplete artifact"
    finally:
        connection.close()
        gate.release.set()

    # Cancellation aborts the first fill. A fresh client must install correct bytes
    # and populate a usable cache entry.
    python = client.environment()
    require_success(
        client.install(python, proxy.index_url, "pipr-e2e-app==1.0.0"), proxy
    )
    assert client.inspect(python) == EXPECTED
    assert proxy.get(artifact_url(proxy, wheel)) == wheel.content
    temporary = proxy.root / "artifacts" / ".tmp"
    deadline = time.monotonic() + 5
    while any(temporary.rglob("*")) and time.monotonic() < deadline:
        threading.Event().wait(0.05)
    assert not list(temporary.rglob("*")), "cancelled transfer left temporary artifacts"
    upstream.deny_artifacts()
    assert proxy.get(artifact_url(proxy, wheel)) == wheel.content


class RequestObserver:
    """Transparent local HTTP hop that observes overlapping client requests."""

    __slots__ = ("artifacts", "condition", "index_url", "server", "thread")

    def __init__(self, proxy: Proxy, filename: str) -> None:
        self.condition = threading.Condition()
        self.artifacts = 0
        observer = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                self.forward()

            def do_HEAD(self) -> None:
                self.forward()

            def forward(self) -> None:
                connection = http.client.HTTPConnection(
                    "127.0.0.1", proxy.port, timeout=20
                )
                try:
                    connection.request(self.command, self.path)
                    if (
                        self.path.startswith("/packages/")
                        and unquote(urlsplit(self.path).path).endswith("/" + filename)
                        and self.command == "GET"
                    ):
                        with observer.condition:
                            observer.artifacts += 1
                            observer.condition.notify_all()
                    response = connection.getresponse()
                    self.send_response(response.status)
                    for name, value in response.getheaders():
                        if name.lower() not in {"connection", "transfer-encoding"}:
                            self.send_header(name, value)
                    self.end_headers()
                    with suppress(BrokenPipeError, ConnectionResetError):
                        while chunk := response.read(64 * 1024):
                            self.wfile.write(chunk)
                finally:
                    connection.close()

            @override
            def log_message(self, format: str, *args: object) -> None:
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.index_url = f"http://127.0.0.1:{self.server.server_port}/default/"

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


@pytest.mark.usefixtures("wheels")
def test_overlapping_client_installs_complete_with_identical_artifacts(
    client: Client, proxy: Proxy, upstream: Upstream, tmp_path: Path
) -> None:
    wheel = large_app(upstream)
    # Advertise metadata so uv does not intentionally cancel wheel range probes
    # before installation. The shared GET below represents a complete download.
    upstream.publish(wheel, metadata=True)
    gate = TransferGate()
    upstream.reply(
        f"/files/{wheel.filename}",
        Reply(200, wheel.content, "application/octet-stream", transfer=gate),
    )
    second_root = tmp_path / "second-client"
    second_root.mkdir()
    second = Client(client.kind, second_root, client.uv)
    environments = [
        (candidate, candidate.environment()) for candidate in (client, second)
    ]
    observer = RequestObserver(proxy, wheel.filename)
    try:
        # Absolute artifact links must traverse the same observed public HTTP hop.
        proxy.stop()
        public_url = observer.index_url.removesuffix("default/")
        proxy.config.write_text(
            f"public_url = {json.dumps(public_url)}\n" + proxy.config.read_text()
        )
        proxy.start()
        with ThreadPoolExecutor(max_workers=2) as workers:
            pending = [
                workers.submit(
                    candidate.install, python, observer.index_url, "pipr-e2e-app==1.0.0"
                )
                for candidate, python in environments
            ]
            try:
                assert gate.started.wait(10), (
                    "upstream wheel must be paused mid-transfer"
                )
                with observer.condition:
                    assert observer.condition.wait_for(
                        lambda: observer.artifacts >= 2, timeout=10
                    ), "both clients must request artifacts while upstream is paused"
                assert not any(result.done() for result in pending)
            finally:
                gate.release.set()
            for result in pending:
                require_success(result.result(timeout=60), proxy)
        for candidate, python in environments:
            assert candidate.inspect(python) == EXPECTED
        assert proxy.get(artifact_url(proxy, wheel)) == wheel.content
        assert upstream.artifact_requests().count(f"/files/{wheel.filename}") == 1, (
            "overlapping installs must share one artifact fill within a replica"
        )
        upstream.deny_artifacts()
        assert proxy.get(artifact_url(proxy, wheel)) == wheel.content
    finally:
        gate.release.set()
        observer.close()


@pytest.mark.parametrize("attribute", ["requires-python", "yanked"])
def test_clients_skip_ineligible_candidates(
    client: Client, proxy: Proxy, upstream: Upstream, attribute: str, tmp_path: Path
) -> None:
    compatible = make_wheel("pipr-e2e-choice", "1.0.0", "VALUE = 'compatible'\n")
    ineligible = make_wheel("pipr-e2e-choice", "2.0.0", "VALUE = 'ineligible'\n")
    for wheel in (compatible, ineligible):
        upstream.publish(wheel, metadata=False)
    restriction = (
        ' data-requires-python="&gt;=999"'
        if attribute == "requires-python"
        else ' data-yanked="broken release"'
    )
    anchors = []
    for wheel in (compatible, ineligible):
        extra = restriction if wheel is ineligible else ""
        anchors.append(
            f'<a href="/files/{html.escape(wheel.filename)}#sha256={wheel.sha256}"{extra}>{wheel.filename}</a>'
        )
    upstream.reply(
        "/simple/pipr-e2e-choice/",
        Reply(200, ("<html><body>" + "".join(anchors) + "</body></html>").encode()),
    )
    # An unconstrained request must ignore yanked releases; exact pins may use them.
    for index in (upstream.url + "/simple/", proxy.index_url):
        before = len(upstream.artifact_requests())
        python = client.environment()
        result = client.install(python, index, "pipr-e2e-choice")
        require_success(result, proxy)
        inspection = run(
            [
                str(python),
                "-I",
                "-c",
                "import json; from importlib.metadata import version; from pipr_e2e_choice import VALUE; print(json.dumps([version('pipr-e2e-choice'), VALUE]))",
            ],
            cwd=tmp_path,
            env=isolated_env(tmp_path / "inspect-home"),
        )
        require_success(inspection, proxy)
        assert json.loads(inspection.stdout) == ["1.0.0", "compatible"]
        requests = upstream.artifact_requests()[before:]
        assert f"/files/{compatible.filename}" in requests
        assert f"/files/{ineligible.filename}" not in requests


def test_upstream_unavailability_is_not_package_absence(
    proxy: Proxy, upstream: Upstream
) -> None:
    path = "/simple/pipr-e2e-outage/"
    upstream.reply(path, Reply(503, b"temporary upstream failure"))
    with pytest.raises(HTTPError) as error:
        proxy.get(proxy.index_url + "pipr-e2e-outage/")
    assert 500 <= error.value.code < 600, (
        "upstream outage must not become package-not-found"
    )
    wheel = make_wheel("pipr-e2e-outage", "1.0.0", "VALUE = 'available'\n")
    upstream.publish(wheel, metadata=False)
    assert proxy.get(artifact_url(proxy, wheel)) == wheel.content
