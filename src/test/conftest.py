import pytest

def pytest_configure(config):
    config.addinivalue_line(
        "markers", "real: mark testes to run only if had a running ceph cluster"
    )


def pytest_addoption(parser):
    parser.addoption(
        "--real", action="store_true", default=False, help="run if had a running ceph cluster"
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--real"):
        # --real given in cli
        return
    skip = pytest.mark.skip(reason="need --real option to run integration test")
    for item in items:
        if "real" in item.keywords:
            item.add_marker(skip)
