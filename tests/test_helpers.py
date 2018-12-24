"""Test the helpers moduled of the stats_can package"""
import stats_can

vs = ['v74804', 'v41692457']
v = '41692452'
t = '271-000-22-01'
ts = ['271-000-22-01', '18100204']


def test_check_status():
    """not implemented, not sure if I will, it's only used internally

    works on json returned from various API calls. I probably should figure
    out how to test it but at least I'll know if it breaks since it'll break
    most of my other methods
    """
    pass


def test_parse_tables_one_string():
    """test parsing a table of one string"""
    assert stats_can.helpers.parse_tables(t) == ['27100022']


def test_parse_tables_string_list():
    """test parsing a list of strings"""
    assert stats_can.helpers.parse_tables(ts) == ['27100022', '18100204']


def test_parse_parsed_table_string():
    """Test that an already parsed string still returns correctly"""
    assert stats_can.helpers.parse_tables('10100132') == ['10100132']


def test_parse_string_vector():
    """test parsing single string vector"""
    assert stats_can.helpers.parse_vectors(v) == [41692452]


def test_parse_list_of_strings_vector():
    """test parsing a list of strings"""
    assert stats_can.helpers.parse_vectors(vs) == [74804, 41692457]


def test_parse_parsed_vectors():
    """double parsing vectors shouldn't change anything"""
    pv = stats_can.helpers.parse_vectors
    assert pv(v) == pv(pv(v))
    assert pv(vs) == pv(pv(vs))


def test_parse_parsed_tables():
    """double parsing tables shouldn't change anything"""
    pt = stats_can.helpers.parse_tables
    assert pt(t) == pt(pt(t))
    assert pt(ts) == pt(pt(ts))
