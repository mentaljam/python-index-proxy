"""Local process shutdown with completed and stalled HTTP transfers."""

import socket
import time
from concurrent.futures import ThreadPoolExecutor
from http.client import IncompleteRead
from typing import cast

import pytest

from support import Proxy, Reply, TransferGate, Upstream
from test_process_contracts import links


def wait_for_listener_close(proxy: Proxy) -> None:
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        try:
            connection = socket.create_connection(
                ("127.0.0.1", proxy.port), timeout=0.1
            )
        except OSError:
            return
        connection.close()
        time.sleep(0.01)
    pytest.fail("SIGTERM did not stop accepting connections")


def wait_for_temporary_upload(proxy: Proxy) -> None:
    directory = proxy.root / "artifacts" / ".tmp"
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if directory.exists() and any(directory.iterdir()):
            return
        time.sleep(0.01)
    pytest.fail("stalled artifact never opened a temporary upload")


def test_sigterm_exits_idle_process_without_waiting_for_grace(proxy: Proxy) -> None:
    assert proxy.process is not None
    proxy.process.terminate()
    assert proxy.process.wait(timeout=3) == 0, proxy.diagnostics()


def test_sigterm_allows_inflight_transfer_to_finish_within_grace(
    proxy: Proxy, upstream: Upstream
) -> None:
    body = b"complete artifact" * 800_000
    gate = TransferGate()
    upstream.reply("/simple/pkg/", Reply(200, b"<a href='/files/pkg.whl'>pkg</a>"))
    upstream.reply("/files/pkg.whl", Reply(200, body, transfer=gate))
    artifact = links(proxy, "pkg")[0]
    with ThreadPoolExecutor(max_workers=1) as pool:
        download = pool.submit(proxy.get, artifact)
        try:
            assert gate.started.wait(5)
            wait_for_temporary_upload(proxy)
            assert proxy.process is not None
            proxy.process.terminate()
            wait_for_listener_close(proxy)
            assert proxy.process.poll() is None, (
                "active transfer must retain its grace period"
            )
            gate.release.set()
            assert download.result(timeout=5) == body
            assert proxy.process.wait(timeout=5) == 0, proxy.diagnostics()
        finally:
            gate.release.set()


def test_sigterm_cancels_stalled_fill_and_cleans_up_before_exit(
    proxy: Proxy, upstream: Upstream
) -> None:
    # Only shutdown may release the transfer: the ordinary upstream deadline
    # must not mask a missing cancellation hook at the 30-second process bound.
    proxy.stop()
    with proxy.config.open("a") as config:
        config.write("\n[upstream]\nread_timeout = '120s'\n")
    proxy.start()
    body = b"stalled artifact" * 900_000
    gate = TransferGate(timeout=40)
    upstream.reply("/simple/pkg/", Reply(200, b"<a href='/files/pkg.whl'>pkg</a>"))
    upstream.reply("/files/pkg.whl", Reply(200, body, transfer=gate))
    artifact = links(proxy, "pkg")[0]

    def read_until_shutdown() -> bytes:
        with proxy.opener.open(artifact, timeout=40) as response:
            return cast("bytes", response.read())

    with ThreadPoolExecutor(max_workers=1) as pool:
        download = pool.submit(read_until_shutdown)
        try:
            assert gate.started.wait(5)
            wait_for_temporary_upload(proxy)
            assert proxy.process is not None
            started = time.monotonic()
            proxy.process.terminate()
            wait_for_listener_close(proxy)
            assert proxy.process.wait(timeout=31) == 0, proxy.diagnostics()
            assert time.monotonic() - started >= 24, (
                "transfer must receive its 25-second grace"
            )
            with pytest.raises((IncompleteRead, ConnectionError)):
                download.result(timeout=3)
            directory = proxy.root / "artifacts"
            assert list((directory / ".tmp").iterdir()) == []
            assert not any(
                path.is_file()
                for path in directory.rglob("*")
                if ".tmp" not in path.parts
            )
            assert not gate.release.is_set(), (
                "cleanup must finish before upstream resumes"
            )
        finally:
            gate.release.set()
    upstream.reply("/files/pkg.whl", Reply(200, body))
    proxy.restart()
    assert proxy.get(artifact) == body
    assert upstream.artifact_requests() == ["/files/pkg.whl", "/files/pkg.whl"]
