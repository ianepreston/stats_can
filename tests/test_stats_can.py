"""Tests for the sc module.

Todo
----
Refactor a lot of this setup and teardown into its own setup functions
"""

import datetime as dt
import pathlib
import shutil

import pandas as pd

import stats_can

vs = ["v74804", "v41692457"]
v = "41692452"
t = "271-000-22-01"
ts = ["271-000-22-01", "18100204"]
TEST_FILES_PATH = pathlib.Path(__file__).parent / "test_files"


def test_get_tables_for_vectors():
    """Test tables for vectors function."""
    actual = stats_can.sc.get_tables_for_vectors(vs)
    assert actual[74804] == "23100216"
    assert actual[41692457] == "18100004"
    assert sorted(actual["all_tables"]) == sorted(["18100004", "23100216"])


def test_table_subsets_from_vectors():
    """Test tables subsets from vectors function."""
    actual = stats_can.sc.table_subsets_from_vectors(vs)
    assert actual == {"23100216": [74804], "18100004": [41692457]}


def test_vectors_to_df_by_release():
    """Test one vector to df method."""
    r = stats_can.sc.vectors_to_df(
        vs,
        start_release_date=dt.date(2023, 1, 1),
        end_release_date=dt.date(2023, 12, 1),
    )
    assert r.shape == (24, 2)
    assert list(r.columns) == ["v74804", "v41692457"]
    assert isinstance(r.index, pd.DatetimeIndex)


def test_empty_release():
    """Test requesting data from a period with no releases."""
    vec = "v41692457"
    r = stats_can.sc.vectors_to_df(
        vec,
        start_release_date=dt.date(2018, 1, 1),
        end_release_date=dt.date(2018, 5, 1),
    )
    assert len(r) == 0


def test_vectors_to_df_by_periods():
    """Test the other vector to df method."""
    r = stats_can.sc.vectors_to_df(vs, 5)
    for v in vs:
        assert v in r.columns
    for col in r.columns:
        assert r[col].count() == 5
    assert isinstance(r.index, pd.DatetimeIndex)


def test_download_table(tmpdir):
    """Test downloading a table.

    Parameters
    ----------
    tmpdir: Path
        Where to download the table
    """

    # convert tmpdir from LocalPath to pathlib.Path
    tmpdir = pathlib.Path(tmpdir)
    t = "18100204"
    t_json = tmpdir / (t + ".json")
    t_zip = tmpdir / (t + "-eng.zip")
    assert not t_json.exists()
    assert not t_zip.exists()
    tmpdir = pathlib.Path(tmpdir)
    stats_can.sc.download_tables(t, path=tmpdir)
    assert t_json.exists()
    assert t_zip.exists()


def test_zip_update_tables(tmpdir):
    """Test updating a table from a zip file using a different function signature.

    Parameters
    ----------
    tmpdir: Path
        Where to download the table
    update_func: function
        Function to test
    """
    tmpdir = pathlib.Path(tmpdir)
    src = TEST_FILES_PATH
    files = ["18100204.json", "18100204-eng.zip"]
    for f in files:
        src_file = src / f
        dest_file = tmpdir / f
        shutil.copyfile(src_file, dest_file)
        assert src_file.exists()
        assert dest_file.exists()
    updater = stats_can.sc.zip_update_tables(path=tmpdir)
    assert updater == ["18100204"]


def test_zip_table_to_dataframe(tmpdir):
    """Convert a zipped table to a pandas dataframe.

    Parameters
    ----------
    tmpdir: Path
        Where to download the table
    """

    tmpdir = pathlib.Path(tmpdir)
    src = TEST_FILES_PATH
    files = ["18100204.json", "18100204-eng.zip"]
    for f in files:
        src_file = src / f
        dest_file = tmpdir / f
        shutil.copyfile(src_file, dest_file)
        assert src_file.exists()
        assert dest_file.exists()
    df = stats_can.sc.zip_table_to_dataframe("18100204", path=tmpdir)
    assert df.shape == (11804, 15)
    assert df.columns[0] == "REF_DATE"


def test_list_tables(tmpdir):
    """Check which tables have been downloaded as zip files.

    Parameters
    ----------
    tmpdir: Path
        Where to download the table
    list_func: function
        Function under test
    """
    src = TEST_FILES_PATH
    files = ["18100204.json", "unrelated123.json", "23100216.json"]
    for file in files:
        shutil.copyfile(src / file, tmpdir / file)
    tbls = stats_can.sc.list_zipped_tables(path=tmpdir)
    assert len(tbls) == 2
    assert tbls[0]["productId"] in ["18100204", "23100216"]
    assert tbls[1]["productId"] in ["18100204", "23100216"]


def test_table_to_df_zip(tmpdir):
    """Load a table to a dataframe from a zip file.

    Parameters
    ----------
    tmpdir: Path
        Where to download the table
    """
    tmpdir = pathlib.Path(tmpdir)
    src = TEST_FILES_PATH
    files = ["18100204.json", "18100204-eng.zip"]
    for f in files:
        src_file = src / f
        dest_file = tmpdir / f
        shutil.copyfile(src_file, dest_file)
        assert src_file.exists()
        assert dest_file.exists()
    df = stats_can.sc.zip_table_to_dataframe("18100204", path=tmpdir)
    assert df.shape == (11804, 15)
    assert df.columns[0] == "REF_DATE"


def test_weird_dates(tmpdir):
    """Tables with weird (e.g quarterly) date formats used to break reading to df.

    calling parse_dates on a ref date column that pandas couldn't figure out would cause
    an error and kill the whole process

    Parameters
    ----------
    tmpdir: Path
        Where to download the table
    """
    tmpdir = pathlib.Path(tmpdir)
    src = TEST_FILES_PATH
    files = ["13100805.json", "13100805-eng.zip"]
    for f in files:
        src_file = src / f
        dest_file = tmpdir / f
        shutil.copyfile(src_file, dest_file)
        assert src_file.exists()
        assert dest_file.exists()
    # Will fail if I don't have correct date parsing
    df = stats_can.sc.zip_table_to_dataframe("13100805", path=tmpdir)
    assert len(df) > 0
