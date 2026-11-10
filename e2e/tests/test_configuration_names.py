"""Configuration admission and canonical project contexts at process boundaries."""

import json
import socket
import subprocess
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import unquote, urljoin

import pytest

from support import Proxy, ProxyFactory, Reply, Upstream, isolated_env
from test_clients import Links


@pytest.mark.parametrize("invalid", ["prefix", "template", "public_url"])
def test_invalid_configuration_fails_before_binding_without_leaking_credentials(
    proxy_binary: Path, tmp_path: Path, *, invalid: str
) -> None:
    with socket.socket() as occupied:
        occupied.bind(("127.0.0.1", 0))
        occupied.listen()
        prefix = "/{capture}/" if invalid == "prefix" else "/"
        template = (
            "https://user:private-password@example.test/simple/"
            if invalid == "template"
            else "https://example.test/{package}/"
        )
        config = tmp_path / "invalid.toml"
        config.write_text(
            f'bind = "127.0.0.1:{occupied.getsockname()[1]}"\n'
            f"path_prefix = {json.dumps(prefix)}\n"
            + (
                'public_url = "https://user:private-password@example.test/external/"\n'
                if invalid == "public_url"
                else ""
            )
            + f"[indices]\ndefault = [{json.dumps(template)}]\n"
            f"[storage.fs]\nroot = {json.dumps(str(tmp_path / 'artifacts'))}\n"
        )
        result = subprocess.run(  # noqa: S603 - Known project binary and generated local configuration.
            [str(proxy_binary), str(config)],
            cwd=tmp_path,
            env=isolated_env(tmp_path / "home"),
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    output = result.stdout + result.stderr
    assert result.returncode == 1, output
    expected_field = {
        "prefix": "path_prefix",
        "template": "indices.default[0]",
        "public_url": "public_url",
    }[invalid]
    assert expected_field in output
    assert "private-password" not in output
    assert "Listening" not in output
    assert "panicked" not in output


def test_project_aliases_share_context_after_restart_and_keep_filenames(
    proxy_factory: ProxyFactory, upstream: Upstream
) -> None:
    upstream.reply(
        "/simple/foo-bar/?token=one",
        Reply(200, b"<a href='/files/Pkg_Name+build.whl?download=1'>wheel</a>"),
    )
    upstream.reply("/files/Pkg_Name+build.whl?download=1", Reply(200, b"wheel bytes"))
    upstream.reply(
        "/files/Pkg_Name+build.whl.metadata?download=1", Reply(200, b"metadata bytes")
    )
    proxy = proxy_factory(
        "/nested/a&b/%7Eroot/",
        index_templates=(upstream.url + "/simple/{package}?token=one",),
    )
    # Missing trailing slash in configured prefixes is accepted and normalized.
    proxy.stop()
    proxy.config.write_text(
        proxy.config.read_text().replace(
            'path_prefix = "/nested/a&b/%7Eroot/"',
            'path_prefix = "/nested/a&b/%7Eroot"',
        )
    )
    proxy.start()
    alias = proxy.index_url + "Foo__..Bar/"
    with proxy.opener.open(alias, timeout=5) as response:
        assert response.geturl() == alias, "project aliases must not redirect"
        body = response.read().decode()
    parser = Links()
    parser.feed(body)
    assert len(parser.hrefs) == 1
    saved = urljoin(proxy.index_url, parser.hrefs[0])
    assert "/default/foo-bar/" in unquote(saved)
    assert unquote(saved).endswith("/Pkg_Name+build.whl")
    assert "a&amp;b" in body
    assert proxy.get(proxy.index_url + "foo-bar/").decode() == body
    assert upstream.journal() == ["/simple/foo-bar/?token=one"]
    prefix, _, identifier, filename = saved.rsplit("/", 3)
    proxy.restart()
    assert proxy.get(f"{prefix}/FOO_bar/{identifier}/{filename}") == b"wheel bytes"
    assert proxy.get(saved + ".metadata") == b"metadata bytes"
    assert upstream.journal().count("/simple/foo-bar/?token=one") == 2
    requests = upstream.journal()
    upstream.reply("/files/Pkg_Name+build.whl?download=1", Reply(503))
    assert proxy.get(f"{prefix}/foo.bar/{identifier}/{filename}") == b"wheel bytes"
    assert upstream.journal() == requests


def test_case_sensitive_indices_and_invalid_names_do_not_fetch_upstream(
    proxy: Proxy, upstream: Upstream
) -> None:
    proxy.stop()
    proxy.config.write_text(
        proxy.config.read_text().replace("default =", '"Private_1.example" =')
    )
    proxy.start()
    upstream.reply("/simple/pkg/", Reply(200, b"<a href='/files/pkg.whl'>pkg</a>"))
    upstream.reply("/files/pkg.whl", Reply(200, b"cached wheel"))
    index_url = proxy.url + "Private_1.example/"
    parser = Links()
    parser.feed(proxy.get(index_url + "pkg/").decode())
    assert len(parser.hrefs) == 1
    saved = urljoin(proxy.url, parser.hrefs[0])
    assert proxy.get(saved) == b"cached wheel"
    baseline = upstream.journal()
    invalid_urls = [proxy.url + "private_1.example/pkg/"]
    for package in ["-bad", "bad_", "%CE%B1", "foo%2Fbar", "foo%5Cbar", "%00"]:
        invalid_urls.extend(
            [index_url + package + "/", saved.replace("/pkg/", f"/{package}/")]
        )
    prefix, _, package, identifier, filename = saved.rsplit("/", 4)
    invalid_urls.append(f"{prefix}/bad!index/{package}/{identifier}/{filename}")
    for url in invalid_urls:
        with pytest.raises(HTTPError) as error:
            proxy.get(url)
        assert error.value.code == 404, url
    assert upstream.journal() == baseline
