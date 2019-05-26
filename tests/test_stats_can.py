"""Tests for the sc module


TODO
----
Refactor a lot of this setup and teardown into its own setup functions
"""
import os
import shutil
import pytest
import datetime as dt
import pandas as pd
import stats_can

vs = ['v74804', 'v41692457']
v = '41692452'
t = '271-000-22-01'
ts = ['271-000-22-01', '18100204']


@pytest.mark.slow
def test_get_tables_for_vectors():
    """test tables for vectors method"""
    tv1 = stats_can.sc.get_tables_for_vectors(vs)
    assert tv1 == {
        41692457: '18100004',
        74804: '23100216',
        'all_tables': ['18100004', '23100216']
        }


@pytest.mark.slow
def test_table_subsets_from_vectors():
    """test table subsets from vectors method"""
    tv1 = stats_can.sc.table_subsets_from_vectors(vs)
    assert tv1 == {'23100216': [74804], '18100004': [41692457]}


@pytest.mark.slow
def test_vectors_to_df_by_release():
    """test one vector to df method"""
    r = stats_can.sc.vectors_to_df(
        vs,
        start_release_date=dt.date(2018, 1, 1),
        end_release_date=dt.date(2018, 5, 1)
        )
    assert r.shape == (13, 2)
    assert list(r.columns) == ['v41692457', 'v74804']
    assert isinstance(r.index, pd.DatetimeIndex)


@pytest.mark.slow
def test_vectors_to_df_by_periods():
    """test the other vector to df method"""
    r = stats_can.sc.vectors_to_df(vs, 5)
    for v in vs:
        assert v in r.columns
    for col in r.columns:
        assert r[col].count() == 5
    assert isinstance(r.index, pd.DatetimeIndex)


@pytest.mark.slow
def test_download_table(tmpdir):
    t = '18100204'
    t_json = os.path.join(tmpdir, t + '.json')
    t_zip = os.path.join(tmpdir, t + '-eng.zip')
    assert not os.path.isfile(t_json)
    assert not os.path.isfile(t_zip)
    stats_can.sc.download_tables(t, path=tmpdir)
    assert os.path.isfile(t_json)
    assert os.path.isfile(t_zip)


def test_zip_update_tables(tmpdir):
    src = 'test_files'
    files = ['18100204.json', '18100204-eng.zip']
    for f in files:
        src_file = os.path.join(src, f)
        dest_file = os.path.join(tmpdir, f)
        shutil.copyfile(src_file, dest_file)
        assert os.path.isfile(src_file)
        assert os.path.isfile(dest_file)
    updater = stats_can.sc.zip_update_tables(path=tmpdir)
    assert updater == ['18100204']


def test_zip_update_tables_from_update_tables(tmpdir):
    src = 'test_files'
    files = ['18100204.json', '18100204-eng.zip']
    for f in files:
        src_file = os.path.join(src, f)
        dest_file = os.path.join(tmpdir, f)
        shutil.copyfile(src_file, dest_file)
        assert os.path.isfile(src_file)
        assert os.path.isfile(dest_file)
    updater = stats_can.sc.update_tables(path=tmpdir, h5file=None)
    assert updater == ['18100204']


def test_zip_table_to_dataframe(tmpdir):
    src = 'test_files'
    files = ['18100204.json', '18100204-eng.zip']
    for f in files:
        src_file = os.path.join(src, f)
        dest_file = os.path.join(tmpdir, f)
        shutil.copyfile(src_file, dest_file)
        assert os.path.isfile(src_file)
        assert os.path.isfile(dest_file)
    df = stats_can.sc.zip_table_to_dataframe('18100204', path=tmpdir)
    assert df.shape == (11804, 15)
    assert df.columns[0] == 'REF_DATE'


def test_table_to_new_h5(tmpdir):
    src = 'test_files'
    files = ['18100204.json', '18100204-eng.zip']
    for f in files:
        src_file = os.path.join(src, f)
        assert os.path.isfile(src_file)
        dest_file = os.path.join(tmpdir, f)
        shutil.copyfile(src_file, dest_file)
        assert os.path.isfile(dest_file)
    h5file = os.path.join(tmpdir, 'stats_can.h5')
    assert not os.path.isfile(h5file)
    stats_can.sc.tables_to_h5('18100204', path=tmpdir)
    assert os.path.isfile(h5file)


def test_table_to_new_h5_no_path(tmpdir):
    src = 'test_files'
    files = ['18100204.json', '18100204-eng.zip']
    for f in files:
        src_file = os.path.join(src, f)
        assert os.path.isfile(src_file)
        dest_file = os.path.join(tmpdir, f)
        shutil.copyfile(src_file, dest_file)
        assert os.path.isfile(dest_file)
    h5file = os.path.join(tmpdir, 'stats_can.h5')
    assert not os.path.isfile(h5file)
    oldpath = os.getcwd()
    os.chdir(tmpdir)
    stats_can.sc.tables_to_h5('18100204')
    os.chdir(oldpath)
    assert os.path.isfile(h5file)


def test_table_from_h5(tmpdir):
    src = 'test_files'
    file = 'stats_can.h5'
    src_file = os.path.join(src, file)
    dest_file = os.path.join(tmpdir, file)
    shutil.copyfile(src_file, dest_file)
    tbl = '18100204'
    df = stats_can.sc.table_from_h5(tbl, path=tmpdir)
    assert df.shape == (11804, 15)
    assert df.columns[0] == 'REF_DATE'


def test_table_from_h5_no_path(tmpdir):
    src = 'test_files'
    file = 'stats_can.h5'
    src_file = os.path.join(src, file)
    dest_file = os.path.join(tmpdir, file)
    shutil.copyfile(src_file, dest_file)
    tbl = '18100204'
    oldpath = os.getcwd()
    os.chdir(tmpdir)
    df = stats_can.sc.table_from_h5(tbl)
    os.chdir(oldpath)
    assert df.shape == (11804, 15)
    assert df.columns[0] == 'REF_DATE'


@pytest.mark.slow
def test_missing_table_from_h5(tmpdir, capsys):
    src = 'test_files'
    file = 'stats_can.h5'
    src_file = os.path.join(src, file)
    dest_file = os.path.join(tmpdir, file)
    shutil.copyfile(src_file, dest_file)
    tbl = '23100216'
    stats_can.sc.table_from_h5(tbl, path=tmpdir)
    captured = capsys.readouterr()
    assert captured.out == "Downloading and loading table_23100216\n"



def test_metadata_from_h5(tmpdir):
    src = 'test_files'
    file = 'stats_can.h5'
    src_file = os.path.join(src, file)
    dest_file = os.path.join(tmpdir, file)
    shutil.copyfile(src_file, dest_file)
    tbl = '18100204'
    meta = stats_can.sc.metadata_from_h5(tbl, path=tmpdir)
    assert meta[0]['cansimId'] == '329-0079'


def test_metadata_from_h5_no_path(tmpdir):
    src = 'test_files'
    file = 'stats_can.h5'
    src_file = os.path.join(src, file)
    dest_file = os.path.join(tmpdir, file)
    shutil.copyfile(src_file, dest_file)
    tbl = '18100204'
    oldpath = os.getcwd()
    os.chdir(tmpdir)
    meta = stats_can.sc.metadata_from_h5(tbl)
    os.chdir(oldpath)
    assert meta[0]['cansimId'] == '329-0079'


def test_missing_h5_metadata(tmpdir, capsys):
    src = 'test_files'
    file = 'stats_can.h5'
    src_file = os.path.join(src, file)
    dest_file = os.path.join(tmpdir, file)
    shutil.copyfile(src_file, dest_file)
    tbl = 'badtable123'
    stats_can.sc.metadata_from_h5(tbl, path=tmpdir)
    captured = capsys.readouterr()
    assert captured.out == "Couldn't find table json_123\n"


def test_h5_update_tables(tmpdir):
    src = 'test_files'
    file = 'stats_can.h5'
    src_file = os.path.join(src, file)
    dest_file = os.path.join(tmpdir, file)
    shutil.copyfile(src_file, dest_file)
    result = stats_can.sc.h5_update_tables(path=tmpdir)
    assert result == ['18100204', '27100022']


def test_h5_update_tables_from_update_tables(tmpdir):
    src = 'test_files'
    file = 'stats_can.h5'
    src_file = os.path.join(src, file)
    dest_file = os.path.join(tmpdir, file)
    shutil.copyfile(src_file, dest_file)
    result = stats_can.sc.update_tables(path=tmpdir)
    assert result == ['18100204', '27100022']


def test_h5_update_tables_list(tmpdir):
    src = 'test_files'
    file = 'stats_can.h5'
    src_file = os.path.join(src, file)
    dest_file = os.path.join(tmpdir, file)
    shutil.copyfile(src_file, dest_file)
    result = stats_can.sc.h5_update_tables(path=tmpdir, tables='18100204')
    assert result == ['18100204']


def test_h5_update_tables_list_from_update_tables(tmpdir):
    src = 'test_files'
    file = 'stats_can.h5'
    src_file = os.path.join(src, file)
    dest_file = os.path.join(tmpdir, file)
    shutil.copyfile(src_file, dest_file)
    result = stats_can.sc.update_tables(path=tmpdir, tables='18100204')
    assert result == ['18100204']


def test_h5_update_tables_no_path(tmpdir):
    src = 'test_files'
    file = 'stats_can.h5'
    src_file = os.path.join(src, file)
    dest_file = os.path.join(tmpdir, file)
    shutil.copyfile(src_file, dest_file)
    oldpath = os.getcwd()
    os.chdir(tmpdir)
    result = stats_can.sc.h5_update_tables(tables='18100204')
    os.chdir(oldpath)
    assert result == ['18100204']


def test_h5_update_tables_no_path_from_update_tables(tmpdir):
    src = 'test_files'
    file = 'stats_can.h5'
    src_file = os.path.join(src, file)
    dest_file = os.path.join(tmpdir, file)
    shutil.copyfile(src_file, dest_file)
    oldpath = os.getcwd()
    os.chdir(tmpdir)
    result = stats_can.sc.update_tables(tables='18100204')
    os.chdir(oldpath)
    assert result == ['18100204']


def test_h5_included_keys():
    src = 'test_files'
    keys = stats_can.sc.h5_included_keys(path=src)
    assert keys == [
        'json_18100204', 'json_27100022', 'table_18100204', 'table_27100022'
        ]


def test_h5_included_keys_no_path():
    oldpath = os.getcwd()
    os.chdir('test_files')
    keys = stats_can.sc.h5_included_keys()
    os.chdir(oldpath)
    assert keys == [
        'json_18100204', 'json_27100022', 'table_18100204', 'table_27100022'
        ]


def test_vectors_to_df_local_defaults(tmpdir):
    src = 'test_files'
    files = [
        '18100204.json', '18100204-eng.zip',
        '23100216.json', '23100216-eng.zip'
        ]
    for file in files:
        src_file = os.path.join(src, file)
        dest_file = os.path.join(tmpdir, file)
        shutil.copyfile(src_file, dest_file)
    df = stats_can.sc.vectors_to_df_local(
        vectors=['v107792885', 'V74804'],
        path=tmpdir
    )
    assert df.shape == (454, 2)

@pytest.mark.slow
def test_vectors_to_df_local_missing_tables_no_h5(tmpdir):
    df = stats_can.sc.vectors_to_df_local(
        vectors=['v107792885', 'v74804'],
        path=tmpdir
    )
    assert df.shape[1] == 2
    assert df.shape[0] > 450
    assert list(df.columns) == ['v107792885', 'v74804']

def test_vectors_to_df_local_missing_tables_h5(tmpdir):
    src = 'test_files'
    h5 = 'stats_can.h5'
    src_file = os.path.join(src, h5)
    dest_file = os.path.join(tmpdir, h5)
    shutil.copyfile(src_file, dest_file)
    df = stats_can.sc.vectors_to_df_local(
        vectors=['v107792885', 'V74804'],
        path=tmpdir,
        h5file=h5
    )
    assert df.shape[1] == 2
    assert df.shape[0] > 450
    assert list(df.columns) == ['v107792885', 'v74804']


def test_list_zipped_tables(tmpdir):
    src = 'test_files'
    files = ['18100204.json', 'unrelated123.json', '23100216.json']
    for file in files:
        shutil.copyfile(os.path.join(src, file), os.path.join(tmpdir, file))
    tbls = stats_can.sc.list_zipped_tables(path=tmpdir)
    assert len(tbls) == 2
    assert tbls[0]['productId'] in ['18100204', '23100216']
    assert tbls[1]['productId'] in ['18100204', '23100216']


def test_list_h5_tables():
    tbls = stats_can.sc.list_h5_tables(path='test_files')
    assert len(tbls) == 2
    assert tbls[0]['productId'] in ['18100204', '27100022']
    assert tbls[1]['productId'] in ['18100204', '27100022']


def test_list_downloaded_tables_zip(tmpdir):
    src = 'test_files'
    files = ['18100204.json', 'unrelated123.json', '23100216.json']
    for file in files:
        shutil.copyfile(os.path.join(src, file), os.path.join(tmpdir, file))
    tbls = stats_can.sc.list_downloaded_tables(path=tmpdir, h5file=None)
    assert len(tbls) == 2
    assert tbls[0]['productId'] in ['18100204', '23100216']
    assert tbls[1]['productId'] in ['18100204', '23100216']


def test_list_downloaded_tables_h5(tmpdir):
    tbls = stats_can.sc.list_downloaded_tables(path='test_files')
    assert len(tbls) == 2
    assert tbls[0]['productId'] in ['18100204', '27100022']
    assert tbls[1]['productId'] in ['18100204', '27100022']


def test_delete_table_zip(tmpdir):
    src = 'test_files'
    files = os.listdir(src)
    for file in files:
        shutil.copyfile(os.path.join(src, file), os.path.join(tmpdir, file))
    for file in files:
        assert os.path.exists(os.path.join(tmpdir, file))
    deleted = stats_can.sc.delete_tables('18100204', path=tmpdir, h5file=None)
    assert deleted == ['18100204']
    assert not os.path.exists(os.path.join(tmpdir, '18100204-eng.zip'))
    assert not os.path.exists(os.path.join(tmpdir, '18100204.json'))


def test_delete_table_h5(tmpdir):
    src = 'test_files'
    files = os.listdir(src)
    for file in files:
        shutil.copyfile(os.path.join(src, file), os.path.join(tmpdir, file))
    for file in files:
        assert os.path.exists(os.path.join(tmpdir, file))
    deleted = stats_can.sc.delete_tables('27100022', path=tmpdir)
    assert deleted == ['27100022']
    tbls = stats_can.sc.list_downloaded_tables(path=tmpdir)
    assert len(tbls) == 1
    assert tbls[0]['productId'] == '18100204'

def test_delete_table_bad_tables(tmpdir):
    src = 'test_files'
    files = os.listdir(src)
    for file in files:
        shutil.copyfile(os.path.join(src, file), os.path.join(tmpdir, file))
    for file in files:
        assert os.path.exists(os.path.join(tmpdir, file))
    bad_tables = ['12345', 'nothing', '4444444', '23100216']
    deleted = stats_can.sc.delete_tables(bad_tables, path=tmpdir)
    assert deleted == []


def test_table_to_df_h5(tmpdir):
    src = 'test_files'
    file = 'stats_can.h5'
    src_file = os.path.join(src, file)
    dest_file = os.path.join(tmpdir, file)
    shutil.copyfile(src_file, dest_file)
    tbl = '18100204'
    df = stats_can.sc.table_to_df(tbl, path=tmpdir)
    assert df.shape == (11804, 15)
    assert df.columns[0] == 'REF_DATE'


def test_table_to_df_zip(tmpdir):
    src = 'test_files'
    files = ['18100204.json', '18100204-eng.zip']
    for f in files:
        src_file = os.path.join(src, f)
        dest_file = os.path.join(tmpdir, f)
        shutil.copyfile(src_file, dest_file)
        assert os.path.isfile(src_file)
        assert os.path.isfile(dest_file)
    df = stats_can.sc.table_to_df('18100204', path=tmpdir, h5file=None)
    assert df.shape == (11804, 15)
    assert df.columns[0] == 'REF_DATE'
