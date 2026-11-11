"""External contracts migrated from in-process Rust state/HTTP tests."""

from collections.abc import Iterator
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import unquote, urljoin, urlsplit

import pytest

from support import Proxy, ProxyFactory, Reply, Upstream, Wheel, make_wheel
from test_clients import Links


def links(proxy: Proxy, package: str) -> list[str]:
    parser = Links()
    parser.feed(proxy.get(proxy.index_url + package + "/").decode())
    return [urljoin(proxy.index_url, href) for href in parser.hrefs]


@pytest.fixture
def second_upstream() -> Iterator[Upstream]:
    server = Upstream()
    try:
        yield server
    finally:
        server.close()


def test_saved_uncached_url_recovers_source_after_process_restart(
    proxy: Proxy, upstream: Upstream
) -> None:
    wheel = make_wheel("pipr-e2e-context", "1.0.0+cpu", "VALUE = 'context'\n")
    query = "?download=1&x=2"
    upstream.publish(
        wheel, href=f"/files/{wheel.filename}{query}#sha256={wheel.sha256}"
    )
    upstream.reply(
        f"/files/{wheel.filename}{query}",
        Reply(200, wheel.content, "application/octet-stream"),
    )
    upstream.reply(
        f"/files/{wheel.filename}.metadata{query}",
        Reply(200, wheel.metadata, "application/octet-stream"),
    )
    # Losing the query must not accidentally reach another working fixture route.
    upstream.reply(f"/files/{wheel.filename}", Reply(404))
    upstream.reply(f"/files/{wheel.filename}.metadata", Reply(404))
    saved_links = links(proxy, wheel.name)
    assert len(saved_links) == 1
    saved = saved_links[0]
    assert urlsplit(saved).fragment == f"sha256={wheel.sha256}"
    assert unquote(urlsplit(saved).path).endswith("/" + wheel.filename)
    before = upstream.journal()
    assert before == [f"/simple/{wheel.name}/"]
    assert not upstream.artifact_requests()
    proxy.restart()

    # No new index request is made by the test/client before using the old URL.
    assert upstream.journal() == before
    assert proxy.get(saved) == wheel.content
    assert upstream.journal().count(f"/simple/{wheel.name}/") == 2
    assert upstream.artifact_requests() == [f"/files/{wheel.filename}{query}"]
    path = urlsplit(saved).path
    assert proxy.get(proxy.url.rstrip("/") + path + ".metadata") == wheel.metadata
    assert f"/files/{wheel.filename}.metadata{query}" in upstream.journal()

    # The context and exact filename constrain recovery; old routes are removed.
    parts = path.split("/")
    identifier = parts[4]
    for wrong_path in (
        path.rsplit("/", 1)[0] + "/wrong.whl",
        "/".join([*parts[:3], "other", *parts[4:]]),
        path.replace("/default/", "/unknown/"),
        path.replace(identifier, "0" * 64),
        f"/packages/{identifier}/{wheel.filename}",
    ):
        assert wrong_path != path
        with pytest.raises(HTTPError) as error:
            proxy.get(proxy.url.rstrip("/") + wrong_path)
        assert error.value.code == 404, wrong_path


def test_two_processes_recover_sources_and_share_ready_artifacts(
    proxy_factory: ProxyFactory,
    upstream: Upstream,
    wheels: tuple[Wheel, Wheel],
    tmp_path: Path,
) -> None:
    wheel, _ = wheels
    storage = tmp_path / "shared-artifacts"
    first = proxy_factory(storage_root=storage)
    second = proxy_factory(storage_root=storage)
    assert first.root != second.root
    assert first.process is not None
    assert second.process is not None
    assert first.process.pid != second.process.pid
    saved_links = links(first, wheel.name)
    assert len(saved_links) == 1
    assert not upstream.artifact_requests()
    assert upstream.journal().count(f"/simple/{wheel.name}/") == 1

    # Only the origin changes, as a Service could send the same path to another replica.
    path = urlsplit(saved_links[0]).path
    assert second.get(second.url.rstrip("/") + path) == wheel.content
    assert upstream.journal().count(f"/simple/{wheel.name}/") == 2
    assert upstream.artifact_requests() == [f"/files/{wheel.filename}"]
    before = upstream.journal()
    upstream.deny_all()
    assert first.get(first.url.rstrip("/") + path) == wheel.content
    assert upstream.journal() == before


@pytest.mark.parametrize("skipped_status", [403, 404, 410])
@pytest.mark.parametrize(
    "skipped_first", [True, False], ids=["skipped-first", "skipped-last"]
)
def test_multiple_upstreams_skip_client_errors_and_recover_cold_artifacts(
    proxy_factory: ProxyFactory,
    upstream: Upstream,
    second_upstream: Upstream,
    *,
    skipped_status: int,
    skipped_first: bool,
) -> None:
    wheel = make_wheel("pipr-e2e-multi", "1.0.0", "VALUE = 'available'\n")
    upstream.publish(wheel)
    page_path = f"/simple/{wheel.name}/"
    second_upstream.reply(page_path, Reply(skipped_status))
    sources = (upstream, second_upstream)
    if skipped_first:
        sources = sources[::-1]
    proxy = proxy_factory(
        index_templates=tuple(source.url + "/simple/{package}/" for source in sources)
    )
    candidates = links(proxy, wheel.name)
    assert len(candidates) == 1
    assert unquote(urlsplit(candidates[0]).path).endswith("/" + wheel.filename)
    assert links(proxy, wheel.name) == candidates
    assert upstream.journal().count(page_path) == 1
    assert second_upstream.journal() == [page_path]
    proxy.restart()
    assert proxy.get(candidates[0]) == wheel.content
    assert upstream.journal().count(page_path) == 2
    assert second_upstream.journal() == [page_path, page_path]


def test_multiple_upstreams_return_404_when_all_client_errors_are_skipped(
    proxy_factory: ProxyFactory, upstream: Upstream, second_upstream: Upstream
) -> None:
    page_path = "/simple/missing/"
    upstream.reply(page_path, Reply(403))
    second_upstream.reply(page_path, Reply(410))
    proxy = proxy_factory(
        index_templates=tuple(
            source.url + "/simple/{package}/" for source in (upstream, second_upstream)
        )
    )
    with pytest.raises(HTTPError) as error:
        proxy.get(proxy.index_url + "missing/")
    assert error.value.code == 404
    assert upstream.journal() == [page_path]
    assert second_upstream.journal() == [page_path]


@pytest.mark.parametrize("failed_status", [408, 429, 503])
@pytest.mark.parametrize(
    "failed_first", [True, False], ids=["failed-first", "failed-last"]
)
def test_multiple_upstreams_fail_closed_then_recover_full_index(
    proxy_factory: ProxyFactory,
    upstream: Upstream,
    second_upstream: Upstream,
    *,
    failed_status: int,
    failed_first: bool,
) -> None:
    first = make_wheel("pipr-e2e-multi", "1.0.0", "VALUE = 'first'\n")
    second = make_wheel("pipr-e2e-multi", "2.0.0", "VALUE = 'second'\n")
    upstream.publish(first)
    page_path = f"/simple/{first.name}/"
    second_upstream.publish(second)
    sources = (upstream, second_upstream)
    if failed_first:
        sources = sources[::-1]
    proxy = proxy_factory(
        index_templates=tuple(source.url + "/simple/{package}/" for source in sources)
    )
    saved_links = links(proxy, first.name)
    assert len(saved_links) == 2
    saved = next(
        url
        for url in saved_links
        if unquote(urlsplit(url).path).endswith("/" + first.filename)
    )
    assert not upstream.artifact_requests()
    assert not second_upstream.artifact_requests()
    second_upstream.reply(page_path, Reply(failed_status, b"temporarily unavailable"))
    proxy.restart()
    with pytest.raises(HTTPError) as error:
        proxy.get(proxy.index_url + first.name + "/")
    assert error.value.code == 502
    # Cold artifact recovery must obey the same strict policy as the page route.
    with pytest.raises(HTTPError) as artifact_error:
        proxy.get(saved)
    assert artifact_error.value.code == 502
    assert not upstream.artifact_requests()
    assert not second_upstream.artifact_requests()

    second_upstream.publish(second)
    candidates = links(proxy, first.name)
    assert sorted(
        unquote(urlsplit(url).path).rsplit("/", 1)[1] for url in candidates
    ) == [
        first.filename,
        second.filename,
    ]
    downloaded = {
        unquote(urlsplit(url).path).rsplit("/", 1)[1]: proxy.get(url)
        for url in candidates
    }
    assert downloaded == {
        first.filename: first.content,
        second.filename: second.content,
    }
    assert second_upstream.journal().count(page_path) == 4
    assert upstream.journal().count(page_path) >= 2
