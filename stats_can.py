# -*- coding: utf-8 -*-
"""
Provides a python implementation of the Statistics Canada Web Data Service
https://www.statcan.gc.ca/eng/developers/wds/user-guide

Note: StatsCan uses cube/table interchangeably. I'm going to keep cube in my
function names where it maps to their api but otherwise I will use table.
Hence functions with cube in the function name will take tables as an argument
I'm not sure which is less confusing, it's annoying they weren't just
consistent.

ToDo: Oh man, so much. Some of the api is implemented, but lots is missing
Still have to figure out some conventions, and how much to put in each api
function in terms of cleanup
@author: Ian Preston
"""
import re
import os
import json
import warnings
import zipfile
import datetime as dt
import pandas as pd
import requests
SC_URL = 'https://www150.statcan.gc.ca/t1/wds/rest/'


def parse_tables(tables):
    """
    Strip out hyphens or other non-numeric characters from a list of tables
    or a single table
    Table names in StatsCan often have a trailing -01 which isn't necessary
    So also take just the first 8 characters.
    This function by no means guarantees you have a clean list of valid tables,
    but it's a good start.
    Returns a list of cleaned up table names or a string with cleaned name
    """
    def parse_table(table):
        """Clean up one table string"""
        return re.sub(r'\D', '', table)[:8]

    if isinstance(tables, str):
        return parse_table(tables)
    return [parse_table(t) for t in tables]


def get_cube_metadata(tables):
    """
    https://www.statcan.gc.ca/eng/developers/wds/user-guide#a11-1
    Take a list of tables and return a list of dictionaries with their
    metadata
    """
    tables = parse_tables(tables)
    tables = [{'productId': t} for t in tables]
    url = SC_URL + 'getCubeMetadata'
    result = requests.post(url, json=tables)
    result.raise_for_status()
    return result.json()


def get_full_table_download(table):
    """
    https://www.statcan.gc.ca/eng/developers/wds/user-guide#a12-6
    Take a table name and return a url to a zipped CSV of that table
    """
    table = parse_tables(table)
    url = SC_URL + 'getFullTableDownloadCSV/' + table + '/en'
    result = requests.get(url)
    result = result.json()
    if result['status'] != 'SUCCESS':
        warnings.warn(str(result['object']))
    return result['object']


def get_changed_series_list():
    """
    https://www.statcan.gc.ca/eng/developers/wds/user-guide#a10-1
    """
    url = SC_URL + 'getChangedSeriesList'
    result = requests.get(url)
    result.raise_for_status()
    return result.json()


def get_changed_cube_list(date=dt.date.today()):
    """
    https://www.statcan.gc.ca/eng/developers/wds/user-guide#a10-2
    """
    url = SC_URL + 'getChangedCubeList' + '/' + str(date)
    result = requests.get(url)
    result.raise_for_status()
    return result


def get_bulk_vector_data_by_range(
        vector_ids, start_release_date, end_release_date
):
    """
    https://www.statcan.gc.ca/eng/developers/wds/user-guide#a12-5
    """
    url = SC_URL + 'getBulkVectorDataByRange'
    result = requests.post(
        url,
        json={
            "vectorIds": vector_ids,
            "startDataPointReleaseDate": start_release_date,
            "endDataPointReleaseDate": end_release_date
            }
        )
    return result.json()


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
        if meta['status'] != 'SUCCESS':
            warnings.warn(str(meta['object']))
            return
        obj = meta['object']
        product_id = obj['productId']
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
            json.dump(obj, outfile)
    os.chdir(oldpath)


def update_tables(path=os.getcwd()):
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
    remote_jsons = [j['object'] for j in remote_jsons]
    update_table_list = []
    for local, remote in zip(local_jsons, remote_jsons):
        if local['cubeEndDate'] != remote['cubeEndDate']:
            update_table_list.append(local['productId'])
    download_tables(update_table_list, path)
    os.chdir(oldpath)
    return update_table_list


def table_to_dataframe(table, path):
    """
    Reads a StatsCan table into a pandas DataFrame
    If a zip file of the table does not exist in path, downloads it
    returns
    """
    oldpath = os.getcwd()
    os.chdir(path)
    table = parse_tables(table)
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
                dtype=types_dict,
                parse_dates=['REF_DATE']
                )
    possible_cats = [
        'GEO', 'DGUID', 'Supplementary unemployment rates', 'Sex', 'Age group',
        'UOM', 'UOM_ID', 'SCALAR_FACTOR', 'SCALAR_ID', 'VECTOR', 'COORDINATE',
        'STATUS', 'SYMBOL', 'TERMINATED', 'DECIMALS'
        ]
    actual_cats = [col for col in possible_cats if col in col_names]
    df[actual_cats] = df[actual_cats].astype('category')
    os.chdir(oldpath)
    return df
