from __future__ import annotations

import json
import shutil
import subprocess
from collections.abc import Generator, Iterator
from pathlib import Path
from typing import cast

import pytest
from xdist.workermanage import WorkerController

from support import Client, Proxy, ProxyFactory, Upstream, Wheel, make_wheel

PROXIES = pytest.StashKey[list[Proxy]]()
PROXY_BINARY = pytest.StashKey[Path]()


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
        help="Use an existing proxy executable instead of building with Cargo",
    )


@pytest.hookimpl(tryfirst=True)
def pytest_sessionstart(session: pytest.Session) -> None:
    config = session.config
    if not hasattr(config, "workerinput") and config.getoption("numprocesses"):
        # Finish the shared build before xdist starts any worker processes.
        config.stash[PROXY_BINARY] = resolve_proxy_binary(config)


def pytest_configure_node(node: WorkerController) -> None:
    node.workerinput["proxy_binary"] = str(node.config.stash[PROXY_BINARY])


@pytest.fixture(name="proxy_binary", scope="session")
def proxy_binary_fixture(request: pytest.FixtureRequest) -> Path:
    workerinput = cast(
        "dict[str, object] | None", getattr(request.config, "workerinput", None)
    )
    if workerinput is not None:
        return Path(cast("str", workerinput["proxy_binary"]))
    return resolve_proxy_binary(request.config)


def resolve_proxy_binary(config: pytest.Config) -> Path:
    supplied = cast("Path | None", config.getoption("--proxy-binary"))
    if supplied is not None:
        binary = supplied.resolve()
        if not binary.is_file():
            pytest.fail(f"Proxy binary does not exist: {binary}")
        return binary
    root = Path(__file__).resolve().parents[2]
    try:
        build = subprocess.run(
            [  # noqa: S607 - Resolve the installed Cargo through PATH.
                "cargo",
                "build",
                "--locked",
                "--no-default-features",
                "--features",
                "storage-fs",
                "--message-format=json",
            ],
            cwd=root,
            capture_output=True,
            text=True,
            timeout=300,
            check=False,
        )
    except subprocess.TimeoutExpired:
        pytest.fail("Cargo build timed out after 300 seconds")
    assert build.returncode == 0, build.stdout + build.stderr
    for line in build.stdout.splitlines():
        event = json.loads(line)
        if (
            event.get("reason") == "compiler-artifact"
            and event.get("executable")
            and event["target"]["name"] == "python-index-proxy"
        ):
            return Path(event["executable"])
    pytest.fail("Cargo did not report a python-index-proxy executable")


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
        base_url: str = "/",
        *,
        storage_root: Path | None = None,
        index_templates: tuple[str, ...] | None = None,
    ) -> Proxy:
        directory = tmp_path / f"proxy-{len(proxies)}"
        directory.mkdir()
        proxy = Proxy(
            proxy_binary,
            directory,
            upstream,
            base_url,
            storage_root=storage_root,
            index_templates=index_templates,
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
