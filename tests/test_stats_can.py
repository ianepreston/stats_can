# -*- coding: utf-8 -*-
"""
Tests for the Stats_Can package

@author: e975360
"""
import datetime as dt
import pandas as pd
import stats_can

vs = ['v74804', 'v41692457']
v = '41692452'
t = '271-000-22-01'
ts = ['271-000-22-01', '18100204']


def test_gcsl():
    """test get changed series list"""
    r = stats_can.get_changed_series_list()
    assert isinstance(r, list)
    if len(r) > 0:
        assert list(r[0].keys()) == [
                'responseStatusCode', 'vectorId', 'productId', 'coordinate',
                'releaseTime'
                ]


def test_gccl():
    """test get changed cube list"""
    r = stats_can.get_changed_cube_list()
    assert isinstance(r, list)
    if len(r) > 0:
        assert list(r[0].keys()) == [
                'responseStatusCode', 'productId', 'releaseTime'
                ]


def test_gcmd():
    """test get cube metadata"""
    r = stats_can.get_cube_metadata(t)
    assert isinstance(r, list)
    assert list(r[0].keys()) == [
            'responseStatusCode', 'productId', 'cansimId', 'cubeTitleEn',
            'cubeTitleFr', 'cubeStartDate', 'cubeEndDate', 'frequencyCode',
            'nbSeriesCube', 'nbDatapointsCube', 'releaseTime',
            'archiveStatusCode', 'archiveStatusEn', 'archiveStatusFr',
            'subjectCode', 'surveyCode', 'dimension', 'footnote', 'correction'
            ]


def test_gsifcpc():
    """test get series info from cube pid coord"""
    pass


def test_gsifv():
    """test get series info from vector"""
    r = stats_can.get_series_info_from_vector(v)
    assert isinstance(r, list)
    assert list(r[0].keys()) == [
        'responseStatusCode', 'productId', 'coordinate', 'vectorId',
        'frequencyCode', 'scalarFactorCode', 'decimals', 'terminated',
        'SeriesTitleEn', 'SeriesTitleFr', 'memberUomCode'
        ]


def test_gcsdfcpc():
    """test get changed series data from cube pid coord"""
    pass


def test_gcsdfv():
    """test get get changed series data from vector"""
    pass


def test_gdfcpcalnp():
    """test get data from cube pid coord and latest n periods"""
    pass


def test_gdfvalnp():
    """test get data from vectors and latest n periods"""
    r = stats_can.scwds.get_data_from_vectors_and_latest_n_periods(vs, 5)
    assert len(r) == len(vs)
    r0v = r[0]['vectorDataPoint']
    assert len(r0v) == 5
    assert list(r0v[0].keys()) == [
        'refPer', 'refPer2', 'value', 'decimals', 'scalarFactorCode',
        'symbolCode', 'statusCode', 'securityLevelCode', 'releaseTime',
        'frequencyCode'
        ]


def test_gbvdbr():
    """test get bulk vector data by range"""
    r = stats_can.scwds.get_bulk_vector_data_by_range(
        vs, dt.date(2018, 1, 1), dt.date(2018, 5, 1)
        )
    assert len(r) == len(vs)
    r0v = r[0]['vectorDataPoint']
    assert len(r0v) == 1
    assert list(r0v[0].keys()) == [
        'refPer', 'refPer2', 'value', 'decimals', 'scalarFactorCode',
        'symbolCode', 'statusCode', 'securityLevelCode', 'releaseTime',
        'frequencyCode'
        ]


def test_gftd():
    """test get full table download"""
    rc = stats_can.scwds.get_full_table_download(t, csv=True)
    assert rc == 'https://www150.statcan.gc.ca/n1/tbl/csv/27100022-eng.zip'
    rs = stats_can.scwds.get_full_table_download(t, csv=False)
    assert rs == 'https://www150.statcan.gc.ca/n1/tbl/sdmx/27100022-SDMX.zip'


def test_gcs():
    """test get code sets"""
    pass


def test_check_status():
    """not implemented, not sure if I will, it's only used internally

    works on json returned from various API calls. I probably should figure
    out how to test it but at least I'll know if it breaks since it'll break
    most of my other methods
    """
    pass


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


def test_get_tables_for_vectors():
    """test tables for vectors method"""
    tv1 = stats_can.get_tables_for_vectors(vs)
    assert tv1 == {
        41692457: '18100004',
        74804: '23100216',
        'all_tables': ['18100004', '23100216']
        }


def test_table_subsets_from_vectors():
    """test table subsets from vectors method"""
    tv1 = stats_can.table_subsets_from_vectors(vs)
    assert tv1 == {'23100216': [74804], '18100004': [41692457]}


def test_vectors_to_df_by_release():
    """test one vector to df method"""
    r = stats_can.vectors_to_df(
        vs,
        start_release_date=dt.date(2018, 1, 1),
        end_release_date=dt.date(2018, 5, 1)
        )
    assert r.shape == (13, 2)
    assert list(r.columns) == ['v41692457', 'v74804']
    assert isinstance(r.index, pd.DatetimeIndex)


def test_vectors_to_df_by_periods():
    """test the other vector to df method"""
    r = stats_can.vectors_to_df(vs, 5)
    for v in vs:
        assert v in r.columns
    for col in r.columns:
        assert r[col].count() == 5
    assert isinstance(r.index, pd.DatetimeIndex)
