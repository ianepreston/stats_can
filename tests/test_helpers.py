"""Test the helpers moduled of the stats_can package."""

import pytest

import stats_can

vs = ["v74804", "v41692457"]
v = "41692452"
t = "271-000-22-01"
ts = ["271-000-22-01", "18100204"]


def test_check_one_status():
    """Kind of annoying one just to satisfy coverage."""
    bad_status = {"status": "error", "object": "bad stuff"}
    with pytest.raises(RuntimeError):
        stats_can.helpers._check_one_status(bad_status)


def test_check_status():
    """Not implemented, not sure if I will, it's only used internally.

    works on json returned from various API calls. I probably should figure
    out how to test it but at least I'll know if it breaks since it'll break
    most of my other methods
    """
    pass


def test_parse_tables_one_string():
    """Test parsing a table of one string."""
    assert stats_can.helpers.parse_tables(t) == ["27100022"]


def test_parse_tables_string_list():
    """Test parsing a list of strings."""
    assert stats_can.helpers.parse_tables(ts) == ["27100022", "18100204"]


def test_parse_parsed_table_string():
    """Test that an already parsed string still returns correctly."""
    assert stats_can.helpers.parse_tables("10100132") == ["10100132"]


def test_parse_string_vector():
    """Test parsing single string vector."""
    assert stats_can.helpers.parse_vectors(v) == [41692452]


def test_parse_list_of_strings_vector():
    """Test parsing a list of strings."""
    assert stats_can.helpers.parse_vectors(vs) == [74804, 41692457]


def test_parse_parsed_vectors():
    """Double parsing vectors shouldn't change anything."""
    pv = stats_can.helpers.parse_vectors
    assert pv(v) == pv(pv(v))
    assert pv(vs) == pv(pv(vs))


def test_parse_parsed_tables():
    """Double parsing tables shouldn't change anything."""
    pt = stats_can.helpers.parse_tables
    assert pt(t) == pt(pt(t))
    assert pt(ts) == pt(pt(ts))


def test_chunks():
    """Long lists of vectors should be chunked up."""
    long_vectors = [v for _ in range(499)]
    chunks = stats_can.helpers.chunk_vectors(long_vectors)
    assert len(chunks) == 2
    for chunk in chunks:
        assert len(chunk) <= 250
