"""Tests for the sc module.

Todo
----
Refactor a lot of this setup and teardown into its own setup functions
"""
import datetime as dt
from functools import partial
import os
from pathlib import Path
import shutil

from freezegun import freeze_time
import pandas as pd
from pandas.testing import assert_frame_equal
import pytest

import stats_can


vs = ["v74804", "v41692457"]
v = "41692452"
t = "271-000-22-01"
ts = ["271-000-22-01", "18100204"]
TEST_FILES_PATH = Path(__file__).resolve().parent / "test_files"


@pytest.fixture(scope="module")
def class_folder(tmp_path_factory):
    """Make a folder to store class data.

    Parameters
    ----------
    tmp_path_factory: Path
        tell pytest to generate a temporary path

    Returns
    -------
    Path
        A folder called "classdata" in a temporary directory
    """
    return tmp_path_factory.mktemp("classdata")


@pytest.fixture(scope="module")
def class_fixture(class_folder):
    """Generate a reusable StatsCan class instance for testing.

    Parameters
    ----------
    class_folder: Path
        Where to store data for the class

    Returns
    -------
    stats_can.StatsCan
        Class instance for testing
    """
    return stats_can.StatsCan(data_folder=class_folder)


@pytest.mark.vcr()
def test_class_tables_for_vectors(class_fixture):
    """Make sure we find vectors in the right tables.

    Parameters
    ----------
    class_fixture: stats_can.StatsCan
        The statscan class we're testing against
    """
    tv1 = class_fixture.get_tables_for_vectors(vs)
    assert tv1 == {
        74804: "23100216",
        41692457: "18100004",
        "all_tables": ["23100216", "18100004"],
    }


@pytest.mark.vcr()
def test_class_update_tables(class_fixture):
    """Should always be empty since we're loading this data fresh.

    Parameters
    ----------
    class_fixture: stats_can.StatsCan
        The statscan class we're testing against
    """
    _ = class_fixture.table_to_df(ts[0])
    assert class_fixture.update_tables() == []


@pytest.mark.vcr()
@freeze_time("2020-09-06")
def test_class_static_methods(class_fixture):
    """Static methods are just wrappers, should always match.

    Parameters
    ----------
    class_fixture: stats_can.StatsCan
        The statscan class we're testing against
    """
    assert (
        class_fixture.vectors_updated_today()
        == stats_can.scwds.get_changed_series_list()
    )
    assert (
        class_fixture.tables_updated_today() == stats_can.scwds.get_changed_cube_list()
    )
    test_dt = dt.date(2018, 1, 1)
    assert class_fixture.tables_updated_on_date(
        test_dt
    ) == stats_can.scwds.get_changed_cube_list(test_dt)
    assert class_fixture.get_code_sets() == stats_can.scwds.get_code_sets()
    for v_input in [v, vs]:
        assert class_fixture.vector_metadata(
            v_input
        ) == stats_can.scwds.get_series_info_from_vector(v_input)
        assert_frame_equal(
            class_fixture.vectors_to_df_remote(v_input, periods=1),
            stats_can.vectors_to_df(v_input, periods=1),
        )
        assert_frame_equal(
            class_fixture.vectors_to_df(v_input), stats_can.vectors_to_df_local(v_input)
        )
    # cleanup
    candidates = ["18100004", "23100216"]
    to_delete = [tbl for tbl in candidates if tbl in class_fixture.downloaded_tables]
    for tbl in to_delete:
        class_fixture.delete_tables(tbl)


@pytest.mark.vcr()
def test_class_table_list_download_delete(class_fixture):
    """Test loading and deleting tables.

    Parameters
    ----------
    class_fixture: stats_can.StatsCan
        The statscan class we're testing against
    """
    _ = class_fixture.table_to_df(ts[0])
    assert class_fixture.downloaded_tables == ["27100022"]
    _ = class_fixture.table_to_df(ts[1])
    assert sorted(class_fixture.downloaded_tables) == sorted(["27100022", "18100204"])
    assert class_fixture.delete_tables("111111") == []
    assert class_fixture.delete_tables("18100204") == ["18100204"]


@pytest.mark.parametrize("mapping_func, expected",
                         [(stats_can.sc.get_tables_for_vectors,
                           {74804: "23100216", 41692457: "18100004",
                            "all_tables": ["23100216", "18100004"]}),
                          (stats_can.sc.table_subsets_from_vectors,
                           {"23100216": [74804], "18100004": [41692457]})])
@pytest.mark.vcr()
def test_vectors_mapping(mapping_func, expected):
    """Test mapping from vectors methods.

    Parameters
    ----------
    mapping_func: function
        The statscan mapping function to be tested
    expected: object
        The expected value
    """
    tv1 = mapping_func(vs)
    assert tv1 == expected, mapping_func.__name__


@pytest.mark.vcr()
def test_vectors_to_df_by_release():
    """Test one vector to df method."""
    r = stats_can.sc.vectors_to_df(
        vs, start_release_date=dt.date(2018, 1, 1), end_release_date=dt.date(2018, 5, 1)
    )
    assert r.shape == (13, 2)
    assert list(r.columns) == ["v74804", "v41692457"]
    assert isinstance(r.index, pd.DatetimeIndex)


@pytest.mark.vcr()
def test_vectors_to_df_by_periods():
    """Test the other vector to df method."""
    r = stats_can.sc.vectors_to_df(vs, 5)
    for v in vs:
        assert v in r.columns
    for col in r.columns:
        assert r[col].count() == 5
    assert isinstance(r.index, pd.DatetimeIndex)


@pytest.mark.vcr()
def test_download_table(tmpdir):
    """Test downloading a table.

    Parameters
    ----------
    tmpdir: Path
        Where to download the table
    """
    t = "18100204"
    t_json = tmpdir / t + ".json"
    t_zip = tmpdir / t + "-eng.zip"
    assert not t_json.exists()
    assert not t_zip.exists()
    stats_can.sc.download_tables(t, path=tmpdir)
    assert t_json.exists()
    assert t_zip.exists()


@pytest.mark.parametrize("update_func",
                         [stats_can.sc.zip_update_tables,
                          partial(stats_can.sc.update_tables, h5file=None)])
@pytest.mark.vcr()
def test_zip_update_tables(tmpdir, update_func):
    """Test updating a table from a zip file using a different function signature.

    Parameters
    ----------
    tmpdir: Path
        Where to download the table
    update_func: function
        Function to test
    """
    src = TEST_FILES_PATH
    files = ["18100204.json", "18100204-eng.zip"]
    for f in files:
        src_file = src / f
        dest_file = tmpdir / f
        shutil.copyfile(src_file, dest_file)
        assert src_file.exists()
        assert dest_file.exists()
    updater = update_func(path=tmpdir)
    assert updater == ["18100204"]


def test_zip_table_to_dataframe(tmpdir):
    """Convert a zipped table to a pandas dataframe.

    Parameters
    ----------
    tmpdir: Path
        Where to download the table
    """
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


def test_table_to_new_h5(tmpdir):
    """Convert a zipped table to a pandas dataframe and save in an h5 file.

    Parameters
    ----------
    tmpdir: Path
        Where to download the table
    """
    src = TEST_FILES_PATH
    files = ["18100204.json", "18100204-eng.zip"]
    for f in files:
        src_file = src / f
        assert src_file.exists()
        dest_file = tmpdir / f
        shutil.copyfile(src_file, dest_file)
        assert dest_file.exists()
    h5file = tmpdir / "stats_can.h5"
    assert not h5file.exists()
    stats_can.sc.tables_to_h5("18100204", path=tmpdir)
    assert h5file.exists()


def test_table_to_new_h5_no_path(tmpdir):
    """Convert a zipped table to a pandas dataframe and save in an h5 file.

    Parameters
    ----------
    tmpdir: Path
        Where to download the table
    """
    src = TEST_FILES_PATH
    files = ["18100204.json", "18100204-eng.zip"]
    for f in files:
        src_file = src / f
        assert src_file.exists()
        dest_file = tmpdir / f
        shutil.copyfile(src_file, dest_file)
        assert dest_file.exists()
    h5file = tmpdir / "stats_can.h5"
    assert not h5file.exists()
    oldpath = os.getcwd()
    os.chdir(tmpdir)
    stats_can.sc.tables_to_h5("18100204", path=tmpdir)
    os.chdir(oldpath)
    assert h5file.exists()


def test_table_from_h5(tmpdir):
    """Load a dataframe from a h5 file.

    Parameters
    ----------
    tmpdir: Path
        Where to download the table
    """
    src = TEST_FILES_PATH
    file = "stats_can.h5"
    src_file = src / file
    dest_file = tmpdir / file
    shutil.copyfile(src_file, dest_file)
    tbl = "18100204"
    df = stats_can.sc.table_from_h5(tbl, path=tmpdir)
    assert df.shape == (11804, 15)
    assert df.columns[0] == "REF_DATE"


def test_table_from_h5_no_path(tmpdir):
    """Load a dataframe from a h5 file.

    Parameters
    ----------
    tmpdir: Path
        Where to download the table
    """
    src = TEST_FILES_PATH
    file = "stats_can.h5"
    src_file = src / file
    dest_file = tmpdir / file
    shutil.copyfile(src_file, dest_file)
    tbl = "18100204"
    oldpath = os.getcwd()
    os.chdir(tmpdir)
    df = stats_can.sc.table_from_h5(tbl)
    os.chdir(oldpath)
    assert df.shape == (11804, 15)
    assert df.columns[0] == "REF_DATE"


def test_metadata_from_h5(tmpdir):
    """Load table metadata from a h5 file.

    Parameters
    ----------
    tmpdir: Path
        Where to download the table
    """
    src = TEST_FILES_PATH
    file = "stats_can.h5"
    src_file = src / file
    dest_file = tmpdir / file
    shutil.copyfile(src_file, dest_file)
    tbl = "18100204"
    meta = stats_can.sc.metadata_from_h5(tbl, path=tmpdir)
    assert meta[0]["cansimId"] == "329-0079"


def test_metadata_from_h5_no_path(tmpdir):
    """Load table metadata from a h5 file.

    Parameters
    ----------
    tmpdir: Path
        Where to download the table
    """
    src = TEST_FILES_PATH
    file = "stats_can.h5"
    src_file = src / file
    dest_file = tmpdir / file
    shutil.copyfile(src_file, dest_file)
    tbl = "18100204"
    oldpath = os.getcwd()
    os.chdir(tmpdir)
    meta = stats_can.sc.metadata_from_h5(tbl)
    os.chdir(oldpath)
    assert meta[0]["cansimId"] == "329-0079"


@pytest.mark.parametrize(["sc_h5_func", "table_name", "expected"],
                         [pytest.param(stats_can.sc.table_from_h5, "23100216",
                                       "Downloading and loading table_23100216\n"),
                          (stats_can.sc.metadata_from_h5, "badtable123",
                           "Couldn't find table json_123\n")])
@pytest.mark.vcr()
def test_missing_data_from_h5(tmpdir, capsys, sc_h5_func, table_name, expected):
    """Test loading missing data from a h5 file, make sure it fails.

    Parameters
    ----------
    tmpdir: Path
        Where to download the table
    capsys
        Capture standard output
    sc_h5_func: function
        Function under test
    table_name: str
        Name of table
    expected: str
        Error message
    """
    src = TEST_FILES_PATH
    file = "stats_can.h5"
    src_file = src / file
    dest_file = tmpdir / file
    shutil.copyfile(src_file, dest_file)
    tbl = table_name
    sc_h5_func(tbl, path=tmpdir)
    captured = capsys.readouterr()
    assert captured.out == expected, sc_h5_func.__name__


@pytest.mark.parametrize(["test_name", "sc_func", "expected"],
                         [("tables",
                           stats_can.sc.h5_update_tables,
                           ["18100204", "27100022"]),
                          ("table from update tables",
                           stats_can.sc.update_tables,
                           ["18100204", "27100022"]),
                          ("tables list",
                           partial(stats_can.sc.h5_update_tables, tables="18100204"),
                           ["18100204"]),
                          ("tables list from update tables",
                           partial(stats_can.sc.update_tables, tables="18100204"),
                           ["18100204"])])
@pytest.mark.vcr()
def test_h5_update(tmpdir, test_name, sc_func, expected):
    """Download data in a h5 file.

    Parameters
    ----------
    tmpdir: Path
        Where to download the table
    test_name: str
        Test name
    sc_func: Function
        Function under test
    expected: list
        result
    """
    src = TEST_FILES_PATH
    file = "stats_can.h5"
    src_file = src / file
    dest_file = tmpdir / file
    shutil.copyfile(src_file, dest_file)
    result = sc_func(path=tmpdir)
    assert result == expected, test_name


@pytest.mark.vcr()
def test_h5_update_tables_no_path(tmpdir):
    """Download updated versions of a subset of tables in a h5 file.

    Parameters
    ----------
    tmpdir: Path
        Where to download the table
    """
    src = TEST_FILES_PATH
    file = "stats_can.h5"
    src_file = src / file
    dest_file = tmpdir / file
    shutil.copyfile(src_file, dest_file)
    oldpath = os.getcwd()
    os.chdir(tmpdir)
    result = stats_can.sc.h5_update_tables(tables="18100204")
    os.chdir(oldpath)
    assert result == ["18100204"]


@pytest.mark.vcr()
def test_h5_update_tables_no_path_from_update_tables(tmpdir):
    """Download updated versions of a subset of tables in a h5 file.

    Parameters
    ----------
    tmpdir: Path
        Where to download the table
    """
    src = TEST_FILES_PATH
    file = "stats_can.h5"
    src_file = src / file
    dest_file = tmpdir / file
    shutil.copyfile(src_file, dest_file)
    oldpath = os.getcwd()
    os.chdir(tmpdir)
    result = stats_can.sc.update_tables(tables="18100204")
    os.chdir(oldpath)
    assert result == ["18100204"]


def test_h5_included_keys():
    """Test low level h5 function for stored data."""
    src = TEST_FILES_PATH
    keys = stats_can.sc.h5_included_keys(path=src)
    assert keys == [
        "json_18100204",
        "json_27100022",
        "table_18100204",
        "table_27100022",
    ]


def test_h5_included_keys_no_path():
    """Test low level h5 function for stored data."""
    oldpath = os.getcwd()
    os.chdir(TEST_FILES_PATH)
    keys = stats_can.sc.h5_included_keys()
    os.chdir(oldpath)
    assert keys == [
        "json_18100204",
        "json_27100022",
        "table_18100204",
        "table_27100022",
    ]


def test_vectors_to_df_local_defaults(tmpdir):
    """Load certain vectors to a dataframe.

    Parameters
    ----------
    tmpdir: Path
        Where to download the table
    """
    src = TEST_FILES_PATH
    files = ["18100204.json", "18100204-eng.zip", "23100216.json", "23100216-eng.zip"]
    for file in files:
        src_file = src / file
        dest_file = tmpdir / file
        shutil.copyfile(src_file, dest_file)
    df = stats_can.sc.vectors_to_df_local(vectors=["v107792885", "V74804"], path=tmpdir)
    assert df.shape == (454, 2)


@pytest.mark.vcr()
def test_vectors_to_df_local_missing_tables_no_h5(tmpdir):
    """Load certain vectors to a dataframe.

    Parameters
    ----------
    tmpdir: Path
        Where to download the table
    """
    df = stats_can.sc.vectors_to_df_local(vectors=["v107792885", "v74804"], path=tmpdir)
    assert df.shape[1] == 2
    assert df.shape[0] > 450
    assert list(df.columns) == ["v107792885", "v74804"]


def test_vectors_to_df_local_missing_tables_h5(tmpdir):
    """Load certain vectors to a dataframe.

    Parameters
    ----------
    tmpdir: Path
        Where to download the table
    """
    src = TEST_FILES_PATH
    h5 = "stats_can.h5"
    src_file = os.path.join(src, h5)
    dest_file = os.path.join(tmpdir, h5)
    shutil.copyfile(src_file, dest_file)
    df = stats_can.sc.vectors_to_df_local(
        vectors=["v107792885", "V74804"], path=tmpdir, h5file=h5
    )
    assert df.shape[1] == 2
    assert df.shape[0] > 450
    assert list(df.columns) == ["v107792885", "v74804"]


def test_list_zipped_tables(tmpdir):
    """Check which tables have been downloaded as zip files.

    Parameters
    ----------
    tmpdir: Path
        Where to download the table
    """
    src = TEST_FILES_PATH
    files = ["18100204.json", "unrelated123.json", "23100216.json"]
    for file in files:
        shutil.copyfile(src / file, tmpdir / file)
    tbls = stats_can.sc.list_zipped_tables(path=tmpdir)
    assert len(tbls) == 2
    assert tbls[0]["productId"] in ["18100204", "23100216"]
    assert tbls[1]["productId"] in ["18100204", "23100216"]


def test_list_h5_tables():
    """Check which tables have been loaded to a h5 file."""
    tbls = stats_can.sc.list_h5_tables(path=TEST_FILES_PATH)
    assert len(tbls) == 2
    assert tbls[0]["productId"] in ["18100204", "27100022"]
    assert tbls[1]["productId"] in ["18100204", "27100022"]


def test_list_downloaded_tables_zip(tmpdir):
    """Check which tables have been downloaded as zip files.

    Parameters
    ----------
    tmpdir: Path
        Where to download the table
    """
    src = TEST_FILES_PATH
    files = ["18100204.json", "unrelated123.json", "23100216.json"]
    for file in files:
        shutil.copyfile(src / file, tmpdir / file)
    tbls = stats_can.sc.list_downloaded_tables(path=tmpdir, h5file=None)
    assert len(tbls) == 2
    assert tbls[0]["productId"] in ["18100204", "23100216"]
    assert tbls[1]["productId"] in ["18100204", "23100216"]


def test_list_downloaded_tables_h5(tmpdir):
    """Check which tables have been loaded to a h5 file.

    Parameters
    ----------
    tmpdir: Path
        Where to download the table
    """
    tbls = stats_can.sc.list_downloaded_tables(path=TEST_FILES_PATH)
    assert len(tbls) == 2
    assert tbls[0]["productId"] in ["18100204", "27100022"]
    assert tbls[1]["productId"] in ["18100204", "27100022"]


def test_delete_table_zip(tmpdir):
    """Delete a downloaded zip file.

    Parameters
    ----------
    tmpdir: Path
        Where to download the table
    """
    src = TEST_FILES_PATH
    files = os.listdir(src)
    for file in files:
        shutil.copyfile(src / file, tmpdir / file)
    for file in files:
        assert os.path.exists(tmpdir / file)
    deleted = stats_can.sc.delete_tables("18100204", path=tmpdir, h5file=None)
    assert deleted == ["18100204"]
    assert not os.path.exists(os.path.join(tmpdir, "18100204-eng.zip"))
    assert not os.path.exists(os.path.join(tmpdir, "18100204.json"))


def test_delete_table_h5(tmpdir):
    """Delete a table from a h5 file.

    Parameters
    ----------
    tmpdir: Path
        Where to download the table
    """
    src = TEST_FILES_PATH
    files = os.listdir(src)
    for file in files:
        shutil.copyfile(src / file, tmpdir / file)
    for file in files:
        assert os.path.exists(tmpdir / file)
    deleted = stats_can.sc.delete_tables("27100022", path=tmpdir)
    assert deleted == ["27100022"]
    tbls = stats_can.sc.list_downloaded_tables(path=tmpdir)
    assert len(tbls) == 1
    assert tbls[0]["productId"] == "18100204"


def test_delete_table_bad_tables(tmpdir):
    """Delete a nonexistant table from a h5 file.

    Parameters
    ----------
    tmpdir: Path
        Where to download the table
    """
    src = TEST_FILES_PATH
    files = os.listdir(src)
    for file in files:
        shutil.copyfile(src / file, tmpdir / file)
    for file in files:
        assert os.path.exists(tmpdir / file)
    bad_tables = ["12345", "nothing", "4444444", "23100216"]
    deleted = stats_can.sc.delete_tables(bad_tables, path=tmpdir)
    assert deleted == []


def test_table_to_df_h5(tmpdir):
    """Load a table to a dataframe from a h5 file.

    Parameters
    ----------
    tmpdir: Path
        Where to download the table
    """
    src = TEST_FILES_PATH
    file = "stats_can.h5"
    src_file = src / file
    dest_file = tmpdir / file
    shutil.copyfile(src_file, dest_file)
    tbl = "18100204"
    df = stats_can.sc.table_to_df(tbl, path=tmpdir)
    assert df.shape == (11804, 15)
    assert df.columns[0] == "REF_DATE"


def test_table_to_df_zip(tmpdir):
    """Load a table to a dataframe from a zip file.

    Parameters
    ----------
    tmpdir: Path
        Where to download the table
    """
    src = TEST_FILES_PATH
    files = ["18100204.json", "18100204-eng.zip"]
    for f in files:
        src_file = src / f
        dest_file = tmpdir / f
        shutil.copyfile(src_file, dest_file)
        assert src_file.exists()
        assert dest_file.exists()
    df = stats_can.sc.table_to_df("18100204", path=tmpdir, h5file=None)
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


def test_code_sets_to_df_dict(class_fixture):
    """Load all the code sets into a dictionary of dataframes.

    Parameters
    ----------
    class_fixture: stats_can.StatsCan
        The class instance to call
    """
    codes = stats_can.sc.code_sets_to_df_dict()

    assert isinstance(codes, dict)
    assert all(isinstance(group, pd.DataFrame) for group in codes.values())
