import pytest


def pytest_collection_modifyitems(items):
    """Auto-mark any test in test_integration.py with the 'integration' marker."""
    for item in items:
        if 'test_integration' in str(item.fspath):
            item.add_marker(pytest.mark.integration)
