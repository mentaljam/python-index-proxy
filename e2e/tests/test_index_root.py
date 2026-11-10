"""Index roots provide local connection instructions without project enumeration."""

import re
import shlex
from html import unescape
from urllib.error import HTTPError
from urllib.parse import urljoin
from urllib.request import Request

import pytest

from support import ProxyFactory, Reply, Upstream
from test_clients import Links


@pytest.mark.parametrize("path_prefix", ["/", "/nested/a&b/o'connor/$HOME;/%7Eroot/"])
@pytest.mark.parametrize("index", ["Private_1.example", "packages"])
def test_index_root_instructions_are_local_and_respect_configured_paths(
    proxy_factory: ProxyFactory, upstream: Upstream, path_prefix: str, index: str
) -> None:
    source = upstream.url.replace("http://", "http://source-user:source-password@")
    proxy = proxy_factory(
        path_prefix,
        index_templates=(source + "/simple/{package}/?token=source-secret",),
    )
    proxy.stop()
    proxy.config.write_text(
        proxy.config.read_text().replace("default =", f'"{index}" =')
    )
    proxy.start()

    landing = Links()
    landing.feed(proxy.get(proxy.url).decode())
    assert len(landing.hrefs) == 1
    index_url = urljoin(proxy.url, landing.hrefs[0])
    assert index_url == proxy.url + index + "/"
    request = Request(  # noqa: S310 - URL was checked against the local HTTP proxy above.
        index_url,
        headers={"Host": "untrusted.example", "X-Forwarded-Host": "forwarded.example"},
    )
    with proxy.opener.open(request, timeout=5) as response:
        assert response.status == 200
        assert response.headers.get_content_type() == "text/html"
        assert response.headers.get_content_charset() == "utf-8"
        page = response.read().decode()
    assert page.startswith("<!DOCTYPE html>")
    assert f"Index {index}" in page
    assert "Project enumeration is not available" in page
    assert "Replace PACKAGE" in page
    assert "YOUR_HOST" not in page
    commands = re.search(r"<pre>(.*?)</pre>", page, re.DOTALL)
    assert commands is not None, "index instructions must contain pip and uv commands"
    expected_url = proxy.url + index + "/"
    assert [shlex.split(line) for line in unescape(commands[1]).splitlines()] == [
        ["python", "-m", "pip", "install", "--index-url", expected_url, "PACKAGE"],
        ["uv", "pip", "install", "--index-url", expected_url, "PACKAGE"],
    ]
    if "&" in path_prefix:
        assert "a&amp;b" in page
    for private in (
        "source-user",
        "source-password",
        "source-secret",
        upstream.url,
        "untrusted.example",
        "forwarded.example",
    ):
        assert private not in page

    for missing in ["unknown", "bad!index", "%CE%B1", "%00"] + (
        [index.lower()] if index != index.lower() else []
    ):
        with pytest.raises(HTTPError) as error:
            proxy.get(proxy.url + missing + "/")
        assert error.value.code == 404
    assert upstream.journal() == [], "index roots must not contact upstream sources"


def test_public_url_drives_links_independently_of_application_routes(
    proxy_factory: ProxyFactory, upstream: Upstream
) -> None:
    public_url = "https://packages.example.test/external/a&b/o'connor/$HOME;/%7Eroot"
    public_base = public_url + "/"
    proxy = proxy_factory("/internal/", public_url=public_url)
    landing = Links()
    landing.feed(proxy.get(proxy.url).decode())
    assert landing.hrefs == [public_base + "default/"]
    request = Request(  # noqa: S310 - Fixture-owned local proxy URL.
        proxy.index_url,
        headers={"Host": "untrusted.example", "X-Forwarded-Host": "forwarded.example"},
    )
    with proxy.opener.open(request, timeout=5) as response:
        page = response.read().decode()
    commands = re.search(r"<pre>(.*?)</pre>", page, re.DOTALL)
    assert commands is not None
    assert [shlex.split(line) for line in unescape(commands[1]).splitlines()] == [
        [
            "python",
            "-m",
            "pip",
            "install",
            "--index-url",
            public_base + "default/",
            "PACKAGE",
        ],
        ["uv", "pip", "install", "--index-url", public_base + "default/", "PACKAGE"],
    ]
    assert "YOUR_HOST" not in page
    assert "untrusted.example" not in page
    assert "forwarded.example" not in page
    assert "a&amp;b" in page
    assert upstream.journal() == []

    upstream.reply(
        "/simple/pkg/",
        Reply(200, b"<a href='/files/Pkg_Name.whl?download=1#sha256=abc'>wheel</a>"),
    )
    upstream.reply("/files/Pkg_Name.whl?download=1", Reply(200, b"wheel bytes"))
    upstream.reply(
        "/files/Pkg_Name.whl.metadata?download=1", Reply(200, b"metadata bytes")
    )
    project = Links()
    project.feed(proxy.get(proxy.index_url + "pkg/").decode())
    assert len(project.hrefs) == 1
    artifact = project.hrefs[0]
    assert artifact.startswith(public_base + "packages/default/pkg/")
    assert artifact.endswith("/Pkg%5FName%2Ewhl#sha256=abc")
    # Simulate an ingress rewriting the external base path to the local prefix.
    local_artifact = proxy.url + artifact.removeprefix(public_base).split("#", 1)[0]
    assert proxy.get(local_artifact) == b"wheel bytes"
    assert proxy.get(local_artifact + ".metadata") == b"metadata bytes"
    upstream.reply("/files/Pkg_Name.whl?download=1", Reply(503))
    assert proxy.get(local_artifact) == b"wheel bytes"
    with pytest.raises(HTTPError) as error:
        proxy.get(proxy.url.removesuffix("internal/") + "external/default/")
    assert error.value.code == 404
