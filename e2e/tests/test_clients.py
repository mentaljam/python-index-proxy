import hashlib
import json
from html.parser import HTMLParser
from pathlib import Path
from typing import override
from urllib.error import HTTPError
from urllib.parse import unquote, urljoin, urlsplit

import pytest

from support import (
    Client,
    Proxy,
    ProxyFactory,
    Reply,
    Upstream,
    Wheel,
    isolated_env,
    make_wheel,
    require_success,
    run,
)

EXPECTED = {"app": "1.0.0", "dependency": "2.0.0", "value": "app:dependency"}


@pytest.mark.parametrize("unquoted", [False, True], ids=["quoted", "html5-unquoted"])
def test_fixture_installs_directly_from_upstream(
    client: Client,
    upstream: Upstream,
    wheels: tuple[Wheel, Wheel],
    *,
    unquoted: bool,
) -> None:
    app, _ = wheels
    upstream.publish(app, unquoted=unquoted)
    python = client.environment()
    result = client.install(python, upstream.url + "/simple/", "pipr-e2e-app==1.0.0")
    assert result.returncode == 0, result.stdout + result.stderr
    assert client.inspect(python) == EXPECTED
    assert len(upstream.artifact_requests()) == 2


def install_app(client: Client, proxy: Proxy) -> None:
    python = client.environment()
    result = client.install(python, proxy.index_url, "pipr-e2e-app==1.0.0")
    require_success(result, proxy)
    assert client.inspect(python) == EXPECTED


def test_cold_install_resolves_dependency(
    client: Client, proxy: Proxy, upstream: Upstream, wheels: tuple[Wheel, Wheel]
) -> None:
    install_app(client, proxy)
    assert sorted(upstream.artifact_requests()) == sorted(
        f"/files/{wheel.filename}" for wheel in wheels
    )


@pytest.mark.usefixtures("wheels")
def test_reinstall_uses_proxy_artifact_cache(
    client: Client,
    proxy: Proxy,
    upstream: Upstream,
) -> None:
    install_app(client, proxy)
    before = upstream.artifact_requests()
    assert len(before) == 2
    upstream.deny_artifacts()
    # A new venv and disabled client cache make a local client hit impossible.
    install_app(client, proxy)
    assert upstream.artifact_requests() == before


@pytest.mark.usefixtures("wheels")
def test_restart_keeps_artifacts_but_refetches_pages(
    client: Client,
    proxy: Proxy,
    upstream: Upstream,
) -> None:
    install_app(client, proxy)
    before_artifacts = upstream.artifact_requests()
    before_pages = upstream.journal().count("/simple/pipr-e2e-app/")
    assert len(before_artifacts) == 2
    upstream.deny_artifacts()
    proxy.restart()
    install_app(client, proxy)
    assert upstream.artifact_requests() == before_artifacts
    assert upstream.journal().count("/simple/pipr-e2e-app/") > before_pages


@pytest.mark.usefixtures("wheels")
def test_install_under_base_url(
    client: Client,
    proxy_factory: ProxyFactory,
    upstream: Upstream,
) -> None:
    proxy = proxy_factory("/proxy/")
    install_app(client, proxy)
    assert len(upstream.artifact_requests()) == 2


def test_pep658_metadata_is_fetched(
    client: Client, proxy: Proxy, upstream: Upstream, wheels: tuple[Wheel, Wheel]
) -> None:
    install_app(client, proxy)
    requests = upstream.journal()
    for wheel in wheels:
        metadata_path = f"/files/{wheel.filename}.metadata"
        wheel_path = f"/files/{wheel.filename}"
        assert metadata_path in requests, proxy.diagnostics()
        assert requests.index(metadata_path) < requests.index(wheel_path)


def test_install_without_pep658_metadata(
    client: Client, proxy: Proxy, upstream: Upstream, wheels: tuple[Wheel, Wheel]
) -> None:
    for wheel in wheels:
        upstream.publish(wheel, metadata=False)
    install_app(client, proxy)
    assert not any(
        urlsplit(path).path.endswith(".metadata") for path in upstream.journal()
    )


def test_relative_links_after_redirect_keep_query(
    client: Client, proxy: Proxy, upstream: Upstream, wheels: tuple[Wheel, Wheel]
) -> None:
    app, _ = wheels
    query = "?download=1&token=test"
    upstream.publish(app, href=f"../files/{app.filename}{query}#sha256={app.sha256}")
    page = upstream.routes[f"/simple/{app.name}/"]
    upstream.reply("/relocated/project/", page)
    upstream.reply(f"/simple/{app.name}/", Reply(302, location="/relocated/project/"))
    artifact_path = f"/relocated/files/{app.filename}"
    upstream.reply(
        f"{artifact_path}{query}", Reply(200, app.content, "application/octet-stream")
    )
    upstream.reply(
        f"{artifact_path}.metadata{query}",
        Reply(200, app.metadata, "application/octet-stream"),
    )
    # A wrong path or lost query must fail, rather than fall back to another valid URL.
    upstream.reply(f"/files/{app.filename}", Reply(404))
    upstream.reply(f"/files/{app.filename}.metadata", Reply(404))
    install_app(client, proxy)
    requests = upstream.journal()
    assert "/relocated/project/" in requests
    assert f"{artifact_path}{query}" in requests
    assert f"{artifact_path}.metadata{query}" in requests


def test_html5_unquoted_href(
    client: Client, proxy: Proxy, upstream: Upstream, wheels: tuple[Wheel, Wheel]
) -> None:
    app, _ = wheels
    upstream.publish(app, unquoted=True)
    install_app(client, proxy)


def test_wrong_checksum_rejects_installation(
    client: Client, proxy: Proxy, upstream: Upstream, wheels: tuple[Wheel, Wheel]
) -> None:
    app, _ = wheels
    wrong_digest = "0" * 64
    assert app.sha256 != wrong_digest
    upstream.publish(app, digest=wrong_digest)
    python = client.environment()
    result = client.install(python, proxy.index_url, "pipr-e2e-app==1.0.0")
    assert result.returncode != 0, proxy.diagnostics()
    assert f"/files/{app.filename}" in upstream.artifact_requests(), proxy.diagnostics()
    output = (result.stdout + result.stderr).lower()
    assert "hash" in output, output
    assert wrong_digest in output, output
    client.assert_absent(python, "pipr-e2e-app")


def test_missing_package_fails(
    client: Client, proxy: Proxy, upstream: Upstream
) -> None:
    with pytest.raises(HTTPError) as error:
        proxy.get(proxy.index_url + "pipr-e2e-missing/")
    assert error.value.code == 404
    python = client.environment()
    result = client.install(python, proxy.index_url, "pipr-e2e-missing==1.0.0")
    assert result.returncode != 0, proxy.diagnostics()
    assert "/simple/pipr-e2e-missing/" in upstream.journal(), proxy.diagnostics()
    assert not upstream.artifact_requests()
    assert "pipr-e2e-missing" in result.stdout + result.stderr
    client.assert_absent(python, "pipr-e2e-missing")


def test_large_wheel_stream_and_cache(
    client: Client, proxy: Proxy, upstream: Upstream, tmp_path: Path
) -> None:
    # Stored ZIP payload exceeds both the current 5 MiB stream threshold and read buffer.
    payload = bytes(range(256)) * (48 * 1024)
    wheel = make_wheel("pipr-e2e-large", "1.0.0", "VALUE = 'large'\n", payload=payload)
    assert len(wheel.content) > 12 * 1024 * 1024
    upstream.publish(wheel)
    for cached in (False, True):
        if cached:
            upstream.deny_artifacts()
        python = client.environment()
        result = client.install(python, proxy.index_url, "pipr-e2e-large==1.0.0")
        require_success(result, proxy)
        inspection = run(
            [
                str(python),
                "-I",
                "-c",
                (
                    "import hashlib, json; from importlib.resources import files; "
                    "from importlib.metadata import version; "
                    "from pipr_e2e_large import VALUE; "
                    "data = files('pipr_e2e_large').joinpath('payload.bin').read_bytes(); "
                    "print(json.dumps([version('pipr-e2e-large'), VALUE, len(data), "
                    "hashlib.sha256(data).hexdigest()]))"
                ),
            ],
            cwd=tmp_path,
            env=isolated_env(tmp_path / "inspect-home"),
        )
        require_success(inspection, proxy)
        assert json.loads(inspection.stdout) == [
            "1.0.0",
            "large",
            len(payload),
            hashlib.sha256(payload).hexdigest(),
        ]
    assert upstream.artifact_requests() == [f"/files/{wheel.filename}"]


class Links(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.hrefs: list[str] = []

    @override
    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "a":
            self.hrefs.extend(
                value for key, value in attrs if key == "href" and value is not None
            )


def test_cached_url_survives_restart_without_upstream(
    proxy: Proxy, upstream: Upstream, wheels: tuple[Wheel, Wheel]
) -> None:
    app, _ = wheels
    parser = Links()
    parser.feed(proxy.get(proxy.index_url + app.name + "/").decode())
    assert len(parser.hrefs) == 1
    artifact_url = urljoin(proxy.index_url, parser.hrefs[0])
    # Assert the public route, without duplicating the proxy's ID or storage-key algorithm.
    assert unquote(urlsplit(artifact_url).path).startswith(
        "/packages/default/pipr-e2e-app/"
    )
    assert proxy.get(artifact_url) == app.content
    assert upstream.artifact_requests() == [f"/files/{app.filename}"]
    before = upstream.journal()
    proxy.restart()
    upstream.deny_all()
    assert proxy.get(artifact_url) == app.content
    assert upstream.journal() == before
