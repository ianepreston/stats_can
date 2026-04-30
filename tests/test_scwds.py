"""Tests for the scwds module."""

import datetime as dt

import pytest

import stats_can

pytestmark = pytest.mark.integration

vs = ["v74804", "v41692457"]
v = "41692452"
t = "271-000-22-01"
ts = ["271-000-22-01", "18100204"]


def test_gcsl():
    """Test get changed series list."""
    r = stats_can.get_changed_series_list()
    assert isinstance(r, list)
    if len(r) > 0:
        assert list(r[0].keys()) == [
            "responseStatusCode",
            "vectorId",
            "productId",
            "coordinate",
            "releaseTime",
        ]


def test_gccl():
    """Test get changed cube list."""
    r = stats_can.get_changed_cube_list(dt.date(2023, 9, 5))
    assert isinstance(r, list)
    if len(r) > 0:
        assert list(r[0].keys()) == ["responseStatusCode", "productId", "releaseTime"]


def test_gcmd():
    """Test get cube metadata."""
    r = stats_can.get_cube_metadata(t)
    assert isinstance(r, list)
    assert list(r[0].keys()) == [
        "responseStatusCode",
        "productId",
        "cansimId",
        "cubeTitleEn",
        "cubeTitleFr",
        "cubeStartDate",
        "cubeEndDate",
        "frequencyCode",
        "nbSeriesCube",
        "nbDatapointsCube",
        "releaseTime",
        "archiveStatusCode",
        "archiveStatusEn",
        "archiveStatusFr",
        "subjectCode",
        "surveyCode",
        "dimension",
        "footnote",
        "correction",
        "correctionFootnote",
        "issueDate",
    ]


def test_gsifcpc():
    """Test get series info from cube pid coord."""
    pass


def test_gsifv():
    """Test get series info from vector."""
    r = stats_can.get_series_info_from_vector(v)
    assert isinstance(r, list)
    assert list(r[0].keys()) == [
        "responseStatusCode",
        "productId",
        "coordinate",
        "vectorId",
        "frequencyCode",
        "scalarFactorCode",
        "decimals",
        "terminated",
        "SeriesTitleEn",
        "SeriesTitleFr",
        "memberUomCode",
    ]


def test_gcsdfcpc():
    """Test get changed series data from cube pid coord."""
    r = stats_can.scwds.get_changed_series_data_from_cube_pid_coord(
        ("35100003", "1.12")
    )
    assert isinstance(r, list)
    assert len(r) == 1
    assert r[0]["productId"] == 35100003


def test_gcsdfv():
    """Test get get changed series data from vector."""
    r = stats_can.scwds.get_changed_series_data_from_vector(v)
    assert isinstance(r, list)
    assert len(r) == 1


def test_gdfcpcalnp():
    """Test get data from cube pid coord and latest n periods."""
    r = stats_can.scwds.get_data_from_cube_pid_coord_and_latest_n_periods(
        ("35100003", "1.12"), periods=3
    )
    assert isinstance(r, list)
    assert len(r) == 1
    assert len(r[0]["vectorDataPoint"]) == 3


def test_gdfvalnp():
    """Test get data from vectors and latest n periods."""
    r = stats_can.scwds.get_data_from_vectors_and_latest_n_periods(vs, 5)
    assert len(r) == len(vs)
    r0v = r[0]["vectorDataPoint"]
    assert len(r0v) == 5
    assert list(r0v[0].keys()) == [
        "refPer",
        "refPer2",
        "refPerRaw",
        "refPerRaw2",
        "value",
        "decimals",
        "scalarFactorCode",
        "symbolCode",
        "statusCode",
        "securityLevelCode",
        "releaseTime",
        "frequencyCode",
    ]


def test_gbvdbr():
    """Test get bulk vector data by range."""
    r = stats_can.scwds.get_bulk_vector_data_by_range(
        vs, dt.date(2023, 1, 1), dt.date(2023, 12, 1)
    )
    assert len(r) == len(vs)
    r0v = r[0]["vectorDataPoint"]
    assert len(r0v) == 13
    assert list(r0v[0].keys()) == [
        "refPer",
        "refPer2",
        "refPerRaw",
        "refPerRaw2",
        "value",
        "decimals",
        "scalarFactorCode",
        "symbolCode",
        "statusCode",
        "securityLevelCode",
        "releaseTime",
        "frequencyCode",
    ]


def test_gbvdbrpr():
    """Test get bulk vector data by reference period range."""
    r = stats_can.scwds.get_bulk_vector_data_by_reference_period_range(
        vs, dt.date(2023, 1, 1), dt.date(2023, 12, 1)
    )
    assert len(r) == len(vs)
    r0v = r[0]["vectorDataPoint"]
    assert len(r0v) == 12
    assert list(r0v[0].keys()) == [
        "refPer",
        "refPer2",
        "refPerRaw",
        "refPerRaw2",
        "value",
        "decimals",
        "scalarFactorCode",
        "symbolCode",
        "statusCode",
        "securityLevelCode",
        "releaseTime",
        "frequencyCode",
    ]


def test_gftd():
    """Test get full table download."""
    rc = stats_can.scwds.get_full_table_download(t, csv=True)
    assert rc == "https://www150.statcan.gc.ca/n1/tbl/csv/27100022-eng.zip"
    rs = stats_can.scwds.get_full_table_download(t, csv=False)
    assert rs == "https://www150.statcan.gc.ca/n1/tbl/sdmx/27100022-SDMX.zip"


def test_gcs():
    """Test get code list."""
    r = stats_can.scwds.get_code_sets()

    assert isinstance(r, dict)
    if len(r) > 0:
        print(r.keys())
        assert list(r.keys()) == [
            "scalar",
            "frequency",
            "symbol",
            "status",
            "uom",
            "survey",
            "subject",
            "classificationType",
            "securityLevel",
            "terminated",
            "wdsResponseStatus",
        ]
