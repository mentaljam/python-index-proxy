"""Request summaries through the real service and credential-safe diagnostics."""

import json
from urllib.error import HTTPError
from urllib.parse import urljoin
from urllib.request import Request

import pytest

from support import ProxyFactory, Reply, Upstream
from test_clients import Links


def test_json_summaries_cover_index_and_artifact_cache_without_credentials(
    proxy_factory: ProxyFactory, upstream: Upstream
) -> None:
    source = upstream.url.replace("http://", "http://private-user:private-password@")
    proxy = proxy_factory(
        index_templates=(source + "/simple/{package}/?token=private-token",)
    )
    proxy.stop()
    log = proxy.root / "proxy.log"
    log.unlink()
    proxy.config.write_text('log_format = "json"\n' + proxy.config.read_text())
    proxy.start()
    upstream.reply(
        "/simple/pkg/?token=private-token",
        Reply(200, b'<a href="/files/pkg.whl?signature=private-signature">wheel</a>'),
    )
    upstream.reply(
        "/files/pkg.whl?signature=private-signature", Reply(200, b"artifact")
    )
    ids: list[str] = []
    pages: list[bytes] = []
    for _ in range(2):
        request = Request(  # noqa: S310 - Harness-local HTTP service.
            proxy.index_url + "Pkg/?token=private-client-token",
            headers={
                "X-Request-ID": "private-client-id",
                "Authorization": "Bearer private-authorization",
            },
        )
        with proxy.opener.open(request, timeout=5) as response:
            ids.append(response.headers["X-Request-ID"])
            pages.append(response.read())
    links = Links()
    links.feed(pages[0].decode())
    assert len(links.hrefs) == 1
    artifact = urljoin(proxy.index_url, links.hrefs[0])
    for _ in range(2):
        with proxy.opener.open(artifact, timeout=5) as response:
            ids.append(response.headers["X-Request-ID"])
            assert response.read() == b"artifact"
    upstream.reply("/simple/failure/?token=private-token", Reply(503))
    with pytest.raises(HTTPError) as raised:
        proxy.get(proxy.index_url + "failure/")
    assert raised.value.code == 502
    ids.append(raised.value.headers["X-Request-ID"])
    raised.value.close()
    proxy.stop()

    text = log.read_text()
    for secret in (
        "private-user",
        "private-password",
        "private-token",
        "private-signature",
        "private-client-token",
        "private-client-id",
        "private-authorization",
    ):
        assert secret not in text
    events = [json.loads(line) for line in text.splitlines()]
    summaries = [
        event["fields"]
        for event in events
        if event["fields"]["message"] == "HTTP request finished"
    ]
    assert len(ids) == len(set(ids))
    for request_id, cache, result in zip(
        ids,
        ["index", "index", "artifact", "artifact", "index"],
        ["miss", "hit", "miss", "hit", "miss"],
        strict=True,
    ):
        matching = [entry for entry in summaries if entry["request_id"] == request_id]
        assert len(matching) == 1
        summary = matching[0]
        assert summary["method"] == "GET"
        assert summary["outcome"] == "completed"
        assert summary["cache"] == cache
        assert summary["cache_result"] == result
        assert summary["index"] == "default"
        assert summary["duration_ms"] >= 0
    assert len(summaries) == len(ids) + 1  # Includes the readiness request.
    error = next(entry for entry in summaries if entry["request_id"] == ids[-1])
    assert error["status"] == 502
    assert error["error"] == "upstream_index"
