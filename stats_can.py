# -*- coding: utf-8 -*-
"""
Created on Mon Jun  4 07:08:04 2018
# Can't just download a big list of vectors from the StatsCan website
any more. I guess I'll build a scraper
@author: Ian Preston
"""
import re
import os
import json
import datetime as dt
from lxml import html
import warnings
import urllib
import requests
import pandas as pd
SC_URL = 'https://www150.statcan.gc.ca/t1/wds/rest/'


def get_cube_metadata(cubes):
    # Do some basic cleanup and make a dictionary
    cubes = [re.sub(r'\D', '', c) for c in cubes]
    cubes = [s[:8] for s in cubes]
    cubes = [{'productId': c} for c in cubes]
    url = SC_URL + 'getCubeMetadata'
    r = requests.post(url, json=cubes)
    r.raise_for_status()
    return r.json()


def get_full_table_download(table):
    table = re.sub(r'\D', '', table)[:8]
    url = SC_URL + 'getFullTableDownloadCSV/' + table + '/en'
    r = requests.get(url)
    r = r.json()
    if r['status'] != 'SUCCESS':
        warnings.warn(str(r['object']))
    return r['object']


def download_tables(tables, path=os.getcwd()):
    oldpath = os.getcwd()
    os.chdir(path)
    metas = get_cube_metadata(tables)
    for meta in metas:
        if meta['status'] != 'SUCCESS':
            warnings.warn(str(meta['object']))
            return
        obj = meta['object']
        product_id = obj['productId']
        csv_url_base = get_full_table_download(product_id)
        r = requests.get(csv_url_base)
        web = html.fromstring(r.content)
        href_list = web.xpath('//a/@href')
        for i in href_list:
            if '?st=' in i:
                href = i
                break
        st = href[href.rfind('.zip')+4:]
        csv_url = csv_url_base + st
        csv_file = csv_url_base[csv_url_base.rfind('/')+1:]
        urllib.request.urlretrieve(csv_url, csv_file)
        json_file = product_id + '.json'
        with open(json_file, 'w') as outfile:
            json.dump(obj, outfile)
    os.chdir(oldpath)


def get_changed_series_list():
    """
    https://www.statcan.gc.ca/eng/developers/wds/user-guide#a10-1
    """
    url = SC_URL + 'getChangedSeriesList'
    r = requests.get(url)
    r.raise_for_status()
    return r.json()


def get_changed_cube_list(date=dt.date.today()):
    """
    https://www.statcan.gc.ca/eng/developers/wds/user-guide#a10-2
    """
    url = SC_URL + 'getChangedCubeList' + '/' + str(date)
    r = requests.get(url)
    r.raise_for_status()
    return r


def get_bulk_vector_data_by_range(
        vector_ids, start_release_date, end_release_date
):
    """
    https://www.statcan.gc.ca/eng/developers/wds/user-guide#a12-5
    """
    url = SC_URL + 'getBulkVectorDataByRange'
    r = requests.post(
        url,
        json={
            "vectorIds": vector_ids,
            "startDataPointReleaseDate": start_release_date,
            "endDataPointReleaseDate": end_release_date
            }
        )
    return r.json()


if __name__ == '__main__':
    full_table = get_full_table_download('14-10-0034')
