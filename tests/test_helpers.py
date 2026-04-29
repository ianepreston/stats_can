"""Test the helpers moduled of the stats_can package."""

import pytest

from stats_can import helpers

vs = ["v74804", "v41692457"]
v = "41692452"
t = "271-000-22-01"
ts = ["271-000-22-01", "18100204"]


def test_parse_tables_one_string():
    """Test parsing a table of one string."""
    assert helpers.parse_tables(t) == ["27100022"]


def test_parse_tables_string_list():
    """Test parsing a list of strings."""
    assert helpers.parse_tables(ts) == ["27100022", "18100204"]


def test_parse_parsed_table_string():
    """Test that an already parsed string still returns correctly."""
    assert helpers.parse_tables("10100132") == ["10100132"]


def test_parse_string_vector():
    """Test parsing single string vector."""
    assert helpers.parse_vectors(v) == [41692452]


def test_parse_list_of_strings_vector():
    """Test parsing a list of strings."""
    assert helpers.parse_vectors(vs) == [74804, 41692457]


def test_parse_parsed_vectors():
    """Double parsing vectors shouldn't change anything."""
    pv = helpers.parse_vectors
    assert pv(v) == pv(pv(v))
    assert pv(vs) == pv(pv(vs))


def test_parse_parsed_tables():
    """Double parsing tables shouldn't change anything."""
    pt = helpers.parse_tables
    assert pt(t) == pt(pt(t))
    assert pt(ts) == pt(pt(ts))


def test_chunks():
    """Long lists of vectors should be chunked up."""
    long_vectors = [v for _ in range(499)]
    chunks = helpers.chunk_vectors(long_vectors)
    assert len(chunks) == 2
    for chunk in chunks:
        assert len(chunk) <= 250


def test_pad_coordinate_short():
    """Short coordinates should be right-padded with .0 to 10 dimensions."""
    assert helpers.pad_coordinate("1.12") == "1.12.0.0.0.0.0.0.0.0"


def test_pad_coordinate_single_dimension():
    """A single dimension should still pad to 10 positions."""
    assert helpers.pad_coordinate("1") == "1.0.0.0.0.0.0.0.0.0"


def test_pad_coordinate_already_full():
    """A coordinate already at 10 dimensions should be returned unchanged."""
    full = "1.2.3.4.5.6.7.8.9.10"
    assert helpers.pad_coordinate(full) == full


def test_pad_coordinate_too_many_dimensions():
    """More than 10 dimensions should raise ValueError."""
    with pytest.raises(ValueError, match="more than 10 dimensions"):
        helpers.pad_coordinate("1.2.3.4.5.6.7.8.9.10.11")


def test_pad_coordinate_empty_string():
    """An empty string should raise ValueError rather than over-padding."""
    with pytest.raises(ValueError, match="non-empty"):
        helpers.pad_coordinate("")


def test_pad_coordinate_non_numeric():
    """Non-integer dimension members should raise ValueError."""
    with pytest.raises(ValueError, match="non-negative integers"):
        helpers.pad_coordinate("1.a.3")
