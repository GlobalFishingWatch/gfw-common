import pytest


def pytest_collection_modifyitems(config, items):
    if config.getoption("-m") != "integration":
        skip_integration = pytest.mark.skip(reason="skipped integration test by default")
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip_integration)
