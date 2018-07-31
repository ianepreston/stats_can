# -*- coding: utf-8 -*-
"""
Tests for the Stats_Can package

@author: e975360
"""
import stats_can

vs = ['v41692452', 'v41692457']
v = '41692452'
t = '27100022'
ts = ['27100022', '27100023']


def test_gcsl():
    r = stats_can.get_changed_series_list()
    assert isinstance(r, list)
    assert list(r[0].keys()) == [
        'responseStatusCode',
        'vectorId',
        'productId',
        'coordinate',
        'releaseTime'
        ]


def test_gccl():
    r = stats_can.get_changed_cube_list()
    assert isinstance(r, list)
    assert list(r[0].keys()) == [
        'responseStatusCode', 'productId', 'releaseTime'
        ]


def test_gcmd():
    r = stats_can.get_cube_metadata(t)
    assert isinstance(r, list)
    assert list(r[0].keys()) == [
        'responseStatusCode',
        'productId',
        'cansimId',
        'cubeTitleEn',
        'cubeTitleFr',
        'cubeStartDate',
        'cubeEndDate',
        'nbSeriesCube',
        'nbDatapointsCube',
        'archiveStatusCode',
        'archiveStatusEn',
        'archiveStatusFr',
        'subjectCode',
        'surveyCode',
        'dimension',
        'footnote',
        'correction'
        ]


def test_gsifcpc():
    pass


def test_gsifv():
    r = stats_can.get_series_info_from_vector(v)
    assert isinstance(r, list)
    assert list(r[0].keys()) == [
        'responseStatusCode',
        'productId',
        'coordinate',
        'vectorId',
        'frequencyCode',
        'scalarFactorCode',
        'decimals',
        'terminated',
        'SeriesTitleEn',
        'SeriesTitleFr',
        'memberUomCode'
        ]


def test_gcsdfcpc():
    pass


def test_gcsdfv():
    pass


def test_gdfcpcalnp():
    pass


def test_gdfvalnp():
    pass


def test_gbvdbr():
