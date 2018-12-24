"""Test the helpers moduled of the stats_can package"""
import stats_can

vs = ['v74804', 'v41692457']
v = '41692452'
t = '271-000-22-01'
ts = ['271-000-22-01', '18100204']



def test_parse_tables():
    """test table string parsing"""
    t1 = stats_can.helpers.parse_tables(t)
    t2 = stats_can.helpers.parse_tables(ts)
    t3 = stats_can.helpers.parse_tables('10100132')
    assert t1 == ['27100022']
    assert t2 == ['27100022', '18100204']
    assert t3 == ['10100132']


def test_parse_vectors():
    """test vector string parsing"""
    v1 = stats_can.helpers.parse_vectors(v)
    v2 = stats_can.helpers.parse_vectors(vs)
    assert v1 == [41692452]
    assert v2 == [74804, 41692457]