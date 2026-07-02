from __future__ import annotations

import pytest

from tars.plugins.registry import PluginRegistry


@pytest.fixture
def registry() -> PluginRegistry:
    """A registry pre-loaded with tars' built-in plugins but isolated from
    whatever tests run before/after (so custom test-only registrations
    don't leak)."""
    return PluginRegistry(load_entry_points=True)
