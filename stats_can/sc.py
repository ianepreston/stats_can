# -*- coding: utf-8 -*-
"""Use the api to get useful things

TODO
----
HDF5 implementation

all the os.chdir stuff should be handled as appending paths instead I think

Extend getChangedCubeList with a function that returns all tables updated
within a date range

@author Ian Preston
"""
import os
import json
import zipfile
import pandas as pd
import numpy as np
import requests
from .scwds import get_series_info_from_vector
from .scwds import get_data_from_vectors_and_latest_n_periods
from .scwds import get_bulk_vector_data_by_range
from .scwds import get_cube_metadata
from .scwds import get_full_table_download
from .scwds import parse_tables
from .scwds import parse_vectors


def get_tables_for_vectors(vectors):
    """ get a list of dicts mapping vectors to tables

    Parameters
    ----------
    vectors : list of str or str
        Vectors to find tables for

    Returns
    -------
    list of dict
        keys for each vector number return the table, plus a key for
        'all_tables' that has a list of unique tables used by vectors
    """
    vectors = parse_vectors(vectors)
    v_json = get_series_info_from_vector(vectors)
    tables_list = {j['vectorId']: str(j['productId']) for j in v_json}
    tables_list['all_tables'] = []
    for vector in vectors:
        if tables_list[vector] not in tables_list['all_tables']:
            tables_list['all_tables'].append(tables_list[vector])
    return tables_list


def table_subsets_from_vectors(vectors):
    """get a list of dicts mapping tables to vectors

    Parameters
    ----------
    vectors : list of str or str
        Vectors to find tables for

    Returns
    -------
    list of dict
        keys for each table used by the vectors, matched to a list of vectors
    """
    start_tables_dict = get_tables_for_vectors(vectors)
    tables_dict = {t: [] for t in start_tables_dict['all_tables']}
    vecs = list(start_tables_dict.keys())[:-1]  # all but the all_tables key
    for vec in vecs:
        tables_dict[start_tables_dict[vec]].append(vec)
    return tables_dict


def vectors_to_df(
    vectors, periods=1, start_release_date=None, end_release_date=None
):
    """data frame of vectors with n periods data or over range of release dates

    Wrapper on get_bulk_vector_data_by_range and
    get_data_from_vectors_and_latest_n_periods function to turn the resulting
    list of JSONs into a DataFrame

    Parameters
    ----------
    vectors: str or list of str
        vector numbers to get info for
    periods: int
        number of periods to retrieve data for
    start_release_date: datetime.date
        start release date for the data
    end_release_date: datetime.date
        end release date for the data

    Returns
    -------
    DataFrame with vectors as columns and ref_date as the index (not release)
    """
    df = pd.DataFrame()
    if ((end_release_date is None) | (start_release_date is None)):
        start_list = get_data_from_vectors_and_latest_n_periods(
            vectors, periods
            )
    else:
        start_list = get_bulk_vector_data_by_range(
            vectors, start_release_date, end_release_date
            )
    for vec in start_list:
        name = "v" + str(vec['vectorId'])
        ser = pd.DataFrame(vec['vectorDataPoint'])
        ser.set_index('refPer', inplace=True)
        ser.index = pd.to_datetime(ser.index)
        ser.rename(columns={'value': name}, inplace=True)
        ser = ser[name]
        df = pd.concat([df, ser], axis=1, sort=True)
    return df


def download_tables(tables, path=os.getcwd()):
    """
    Download a json file and zip of CSVs for a list of tables to path
    Input: a list of tables
    Output: Null, but it saves json and CSV files to path for each table
    """
    oldpath = os.getcwd()
    os.chdir(path)
    metas = get_cube_metadata(tables)
    for meta in metas:
        product_id = meta['productId']
        csv_url = get_full_table_download(product_id)
        csv_file = product_id + '-eng.zip'
        # Thanks http://evanhahn.com/python-requests-library-useragent/
        response = requests.get(
            csv_url,
            stream=True,
            headers={'user-agent': None}
            )
        # Thanks https://bit.ly/2sPYPYw
        with open(csv_file, 'wb') as handle:
            for chunk in response.iter_content(chunk_size=512):
                if chunk:  # filter out keep-alive new chunks
                    handle.write(chunk)
        json_file = product_id + '.json'
        with open(json_file, 'w') as outfile:
            json.dump(meta, outfile)
    os.chdir(oldpath)


def zip_update_tables(path=os.getcwd()):
    """
    Grabs the json files in path, checks them against the metadata on
    StatsCan and grabs updated tables where there have been changes
    There isn't actually a "last modified date" part to the metadata
    What I'm doing is comparing the latest reference period. Almost all
    data changes will at least include incremental releases, so this should
    capture what I want
    Returns a list of the tables that were updated
    """
    oldpath = os.getcwd()
    os.chdir(path)
    local_jsons = []
    for file in os.listdir():
        if file.endswith('.json'):
            with open(file) as f_name:
                local_jsons.append(json.load(f_name))
    tables = [j['productId'] for j in local_jsons]
    remote_jsons = get_cube_metadata(tables)
    update_table_list = []
    for local, remote in zip(local_jsons, remote_jsons):
        if local['cubeEndDate'] != remote['cubeEndDate']:
            update_table_list.append(local['productId'])
    download_tables(update_table_list, path)
    os.chdir(oldpath)
    return update_table_list


def zip_table_to_dataframe(table, path=os.getcwd()):
    """
    Reads a StatsCan table into a pandas DataFrame
    If a zip file of the table does not exist in path, downloads it
    returns
    """
    oldpath = os.getcwd()
    os.chdir(path)
    # Parse tables returns a list, can only do one table at a time here though
    table = parse_tables(table)[0]
    table_zip = table + '-eng.zip'
    if not os.path.isfile(table_zip):
        download_tables([table], path)
    csv_file = table + '.csv'
    with zipfile.ZipFile(table_zip) as myzip:
        with myzip.open(csv_file) as myfile:
            col_names = pd.read_csv(myfile, nrows=0).columns
        # reopen the file or it misses the first row
        with myzip.open(csv_file) as myfile:
            types_dict = {'VALUE': float}
            types_dict.update(
                {col: str for col in col_names if col not in types_dict}
                )
            df = pd.read_csv(
                myfile,
                dtype=types_dict
                )

    possible_cats = [
        'GEO', 'DGUID', 'STATUS', 'SYMBOL', 'TERMINATED', 'DECIMALS',
        'UOM', 'UOM_ID', 'SCALAR_FACTOR', 'SCALAR_ID', 'VECTOR', 'COORDINATE',
        'Wages', 'National Occupational Classification for Statistics (NOC-S)',
        'Supplementary unemployment rates', 'Sex', 'Age group',
        'Labour force characteristics', 'Statistics', 'Data type'
        ]
    actual_cats = [col for col in possible_cats if col in col_names]
    df[actual_cats] = df[actual_cats].astype('category')
    try:
        df['REF_DATE'] = pd.to_datetime(df['REF_DATE'], format='%Y-%m')
    except TypeError:
        df['REF_DATE'] = pd.to_datetime(df['REF_DATE'])
    os.chdir(oldpath)
    return df


def get_classic_vector_format_df(vectors, path, start_date=None):
    """
    Like oldschool CANSIM, this will return a single dataframe with V numbers
    as columns, indexed on date
    Inputs:
        vectors: list of vectors to be read in
        path: path to zipped StatsCan tables
        start_date: optional earliest reference date to include
    TODO
    ----
    Either refactor significantly or maybe get rid of. I think the retrieve
    vectors for n periods satisfies this use case for the most part
    If I do keep it add in HDF5 support
    Returns: A DataFrame as described above
    """
    # Preserve an initial copy of the list for ordering, parsed and then
    # converted to string for consistency in naming
    vectors_ordered = parse_vectors(vectors)
    vectors_ordered = ['v' + str(v) for v in vectors_ordered]
    table_vec_dict = table_subsets_from_vectors(vectors)
    tables = list(table_vec_dict.keys())
    tables_dfs = {}
    columns = ['REF_DATE', 'VECTOR', 'VALUE']
    for table in tables:
        tables_dfs[table] = zip_table_to_dataframe(table, path)[columns]
        df = tables_dfs[table]  # save me some typing
        vec_list = ['v' + str(v) for v in table_vec_dict[table]]
        df = df[df['VECTOR'].isin(vec_list)]
        if start_date is not None:
            start_date = np.datetime64(start_date)
            df = df[df['REF_DATE'] >= start_date]
        df = df.pivot(index='REF_DATE', columns='VECTOR', values='VALUE')
        df.columns = list(df.columns)  # remove categorical index
        tables_dfs[table] = df
    final_df = tables_dfs[tables[0]]
    for table in tables[1:]:
        final_df = pd.merge(
            final_df, tables_dfs[table],
            how='outer',
            left_index=True, right_index=True
            )
    final_df = final_df[vectors_ordered]
    return final_df
