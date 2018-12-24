"""Tests for the sc module"""
import os
import shutil
import datetime as dt
import pandas as pd
import stats_can

vs = ['v74804', 'v41692457']
v = '41692452'
t = '271-000-22-01'
ts = ['271-000-22-01', '18100204']


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