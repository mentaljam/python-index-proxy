from __future__ import annotations

import shutil
from collections.abc import Generator, Iterator
from pathlib import Path
from typing import cast

import pytest

from support import Client, Proxy, ProxyFactory, Upstream, Wheel, make_wheel

PROXIES = pytest.StashKey[list[Proxy]]()


@pytest.hookimpl(wrapper=True)
def pytest_runtest_makereport(
    item: pytest.Item,
) -> Generator[None, pytest.TestReport, pytest.TestReport]:
    report = yield
    if report.failed:
        for proxy in item.stash.get(PROXIES, []):
            report.sections.append((f"Proxy {proxy.port}", proxy.diagnostics()))
    return report


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--proxy-binary",
        type=Path,
        default=Path(__file__).resolve().parents[2]
        / "target"
        / "debug"
        / "python-index-proxy",
        help="Path to a prebuilt proxy executable (default: target/debug/python-index-proxy)",
    )


@pytest.fixture(name="proxy_binary", scope="session")
def proxy_binary_fixture(request: pytest.FixtureRequest) -> Path:
    binary = cast("Path", request.config.getoption("--proxy-binary")).resolve()
    if not binary.is_file():
        pytest.fail(
            f"Proxy binary does not exist: {binary}. Run `just build-e2e` first."
        )
    return binary


@pytest.fixture(name="uv_binary", scope="session")
def uv_binary_fixture() -> str:
    binary = shutil.which("uv")
    if binary is None:
        pytest.fail("uv must already be installed; the tests never download tools")
    return binary


@pytest.fixture(name="client", params=["pip", "uv"])
def client_fixture(
    request: pytest.FixtureRequest, tmp_path: Path, uv_binary: str
) -> Client:
    return Client(request.param, tmp_path, uv_binary)


@pytest.fixture(name="upstream")
def upstream_fixture() -> Iterator[Upstream]:
    server = Upstream()
    try:
        yield server
    finally:
        server.close()


@pytest.fixture(name="wheels")
def wheels_fixture(upstream: Upstream) -> tuple[Wheel, Wheel]:
    dependency = make_wheel("pipr-e2e-dep", "2.0.0", "VALUE = 'dependency'\n")
    app = make_wheel(
        "pipr-e2e-app",
        "1.0.0",
        "from pipr_e2e_dep import VALUE as DEP\nVALUE = 'app:' + DEP\n",
        requires=("pipr-e2e-dep==2.0.0",),
    )
    upstream.publish(dependency)
    upstream.publish(app)
    return app, dependency


@pytest.fixture(name="proxy_factory")
def proxy_factory_fixture(
    proxy_binary: Path,
    tmp_path: Path,
    upstream: Upstream,
    request: pytest.FixtureRequest,
) -> Iterator[ProxyFactory]:
    proxies: list[Proxy] = []
    request.node.stash[PROXIES] = proxies

    def create(
        path_prefix: str = "/",
        *,
        storage_root: Path | None = None,
        index_templates: tuple[str, ...] | None = None,
        index_policy: str | None = None,
        public_url: str | None = None,
    ) -> Proxy:
        directory = tmp_path / f"proxy-{len(proxies)}"
        directory.mkdir()
        proxy = Proxy(
            proxy_binary,
            directory,
            upstream,
            path_prefix,
            storage_root=storage_root,
            index_templates=index_templates,
            index_policy=index_policy,
            public_url=public_url,
        )
        proxies.append(proxy)
        proxy.start()
        return proxy

    yield create
    for proxy in reversed(proxies):
        proxy.stop()


@pytest.fixture(name="proxy")
def proxy_fixture(proxy_factory: ProxyFactory) -> Proxy:
    return proxy_factory()
