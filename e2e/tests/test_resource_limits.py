"""Resource admission and failed index downloads through the real process."""

import gzip
from concurrent.futures import ThreadPoolExecutor
from urllib.error import HTTPError

import pytest

from support import Proxy, Reply, TransferGate, Upstream
from test_process_contracts import links


def configure(proxy: Proxy, settings: str) -> None:
    proxy.stop()
    with proxy.config.open("a") as config:
        config.write("\n" + settings)
    proxy.start()


def assert_busy(proxy: Proxy, url: str) -> None:
    with pytest.raises(HTTPError) as error:
        proxy.get(url)
    assert error.value.code == 503
    assert error.value.headers["Retry-After"] == "1"


def test_index_refresh_budget_preserves_cache_and_same_package_waiters(
    proxy: Proxy, upstream: Upstream
) -> None:
    configure(proxy, "[limits]\nmax_index_refreshes = 1\n")
    body = b"<a href='/files/pkg.whl'>pkg.whl</a>"
    upstream.reply("/simple/cached/", Reply(200, body))
    cached = proxy.get(proxy.index_url + "cached/")
    gate = TransferGate()
    upstream.reply("/simple/active/", Reply(200, body, transfer=gate))
    upstream.reply("/simple/other/", Reply(200, body))
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(proxy.get, proxy.index_url + "active/")
        try:
            assert gate.started.wait(5), "first index refresh never reached upstream"
            waiter = pool.submit(proxy.get, proxy.index_url + "active/")
            assert_busy(proxy, proxy.index_url + "other/")
            assert "/simple/other/" not in upstream.journal()
            assert proxy.get(proxy.index_url + "cached/") == cached
        finally:
            gate.release.set()
        assert first.result(timeout=5) == waiter.result(timeout=5)
    assert upstream.journal().count("/simple/active/") == 1
    assert proxy.get(proxy.index_url + "other/")
    assert upstream.journal().count("/simple/other/") == 1


def test_artifact_budget_preserves_cached_reads_and_same_object_waiters(
    proxy: Proxy, upstream: Upstream
) -> None:
    configure(proxy, "[limits]\nmax_artifact_downloads = 1\n")
    for name in ("cached", "active", "other"):
        upstream.reply(
            f"/simple/{name}/",
            Reply(200, f"<a href='/files/{name}.whl'>{name}.whl</a>".encode()),
        )
        upstream.reply(f"/files/{name}.whl", Reply(200, name.encode()))
    saved = {name: links(proxy, name)[0] for name in ("cached", "active", "other")}
    assert proxy.get(saved["cached"]) == b"cached"
    gate = TransferGate()
    body = b"active" * 100_000
    upstream.reply("/files/active.whl", Reply(200, body, transfer=gate))
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(proxy.get, saved["active"])
        try:
            assert gate.started.wait(5), "first artifact fill never reached upstream"
            waiter = pool.submit(proxy.get, saved["active"])
            assert_busy(proxy, saved["other"])
            assert "/files/other.whl" not in upstream.journal()
            assert proxy.get(saved["cached"]) == b"cached"
        finally:
            gate.release.set()
        assert first.result(timeout=5) == body
        assert waiter.result(timeout=5) == body
    assert upstream.journal().count("/files/active.whl") == 1
    assert proxy.get(saved["other"]) == b"other"


@pytest.mark.parametrize("compressed", [False, True])
def test_oversized_index_returns_502_without_caching_partial_results(
    proxy: Proxy, upstream: Upstream, *, compressed: bool
) -> None:
    configure(proxy, "[limits]\nmax_index_page_size = '128 B'\n")
    body = b"<a href='/files/pkg.whl'>pkg.whl</a>" + b" " * 1024
    wire = gzip.compress(body) if compressed else body
    if compressed:
        assert len(wire) < 128 < len(body)
    upstream.reply(
        "/simple/pkg/",
        Reply(200, wire, content_encoding="gzip" if compressed else None),
    )
    for _ in range(2):
        with pytest.raises(HTTPError) as error:
            proxy.get(proxy.index_url + "pkg/")
        assert error.value.code == 502
    assert upstream.journal().count("/simple/pkg/") == 2
    upstream.reply("/simple/pkg/", Reply(200, b"<a href='/files/pkg.whl'>pkg.whl</a>"))
    assert len(links(proxy, "pkg")) == 1


@pytest.mark.parametrize("artifact", [False, True])
def test_stalled_upstream_returns_502_then_recovers(
    proxy: Proxy, upstream: Upstream, *, artifact: bool
) -> None:
    configure(proxy, "[upstream]\nread_timeout = '100ms'\n")
    page = b"<a href='/files/pkg.whl'>pkg.whl</a>"
    upstream.reply("/simple/pkg/", Reply(200, page))
    saved = links(proxy, "pkg")[0] if artifact else proxy.index_url + "pkg/"
    route = "/files/pkg.whl" if artifact else "/simple/pkg/"
    body = b"wheel bytes" if artifact else page
    gate = TransferGate()
    upstream.reply(route, Reply(200, body, transfer=gate))
    try:
        with pytest.raises(HTTPError) as error:
            proxy.get(saved)
        assert gate.started.is_set(), "timeout must occur after body transfer starts"
        assert error.value.code == 502
    finally:
        gate.release.set()
    upstream.reply(route, Reply(200, body))
    recovered = proxy.get(saved)
    if artifact:
        assert recovered == body
    else:
        assert b"pkg.whl" in recovered
