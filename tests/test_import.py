"""Test sportsDb."""

import sportsdb


def test_import() -> None:
    """Test that the package can be imported."""
    assert isinstance(sportsdb.__name__, str)
