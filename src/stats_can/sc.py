"""Functionality that extends on what the base StatsCan api returns in some way.

Todo
----
Function to delete tables

Extend getChangedCubeList with a function that returns all tables updated
within a date range
"""

import json
import pathlib
import zipfile

import pandas as pd
import requests
from tqdm import tqdm

from stats_can.helpers import parse_tables
from stats_can.scwds import (
    get_bulk_vector_data_by_range,
    get_code_sets,
    get_cube_metadata,
    get_data_from_vectors_and_latest_n_periods,
    get_full_table_download,
    get_series_info_from_vector,
)


def get_tables_for_vectors(vectors):
    """Get a list of dicts mapping vectors to tables.

    Parameters
    ----------
    vectors : list of str or str
        Vectors to find tables for

    Returns
    -------
    tables_list: list of dict
        keys for each vector number return the table, plus a key for
        'all_tables' that has a list of unique tables used by vectors
    """
    v_json = get_series_info_from_vector(vectors)
    vectors = [j["vectorId"] for j in v_json]
    tables_list = {j["vectorId"]: str(j["productId"]) for j in v_json}
    tables_list["all_tables"] = []
    for vector in vectors:
        if tables_list[vector] not in tables_list["all_tables"]:
            tables_list["all_tables"].append(tables_list[vector])
    return tables_list


def table_subsets_from_vectors(vectors):
    """Get a list of dicts mapping tables to vectors.

    Parameters
    ----------
    vectors : list of str or str
        Vectors to find tables for

    Returns
    -------
    tables_dict: list of dict
        keys for each table used by the vectors, matched to a list of vectors
    """
    start_tables_dict = get_tables_for_vectors(vectors)
    tables_dict = {t: [] for t in start_tables_dict["all_tables"]}
    vecs = list(start_tables_dict.keys())[:-1]  # all but the all_tables key
    for vec in vecs:
        tables_dict[start_tables_dict[vec]].append(vec)
    return tables_dict


def download_tables(tables, path=None, csv=True):
    """Download a json file and zip of data for a list of tables to path.

    Parameters
    ----------
    tables: list of str
        tables to be downloaded
    path: str or path object, default: None (will do current directory)
        Where to download the table and json
    csv: boolean, default True
        download in CSV format, if not download SDMX

    Returns
    -------
    downloaded: list
        list of tables that were downloaded
    """
    path = pathlib.Path(path) if path else pathlib.Path()
    metas = get_cube_metadata(tables)
    for meta in metas:
        product_id = meta["productId"]
        zip_url = get_full_table_download(product_id, csv=csv)
        zip_file_name = product_id + ("-eng.zip" if csv else ".zip")
        json_file_name = product_id + ".json"
        zip_file = path / zip_file_name
        json_file = path / json_file_name

        # Thanks http://evanhahn.com/python-requests-library-useragent/
        response = requests.get(zip_url, stream=True, headers={"user-agent": None})

        progress_bar = tqdm(
            desc=zip_file_name,
            total=int(response.headers.get("content-length", 0)),
            unit="B",
            unit_scale=True,
        )

        # Thanks https://bit.ly/2sPYPYw
        with open(json_file, "w") as outfile:
            json.dump(meta, outfile)
        with open(zip_file, "wb") as handle:
            for chunk in response.iter_content(chunk_size=512):
                if chunk:  # filter out keep-alive new chunks
                    handle.write(chunk)
                    progress_bar.update(len(chunk))
        progress_bar.close()
    return [meta["productId"] for meta in metas]


def zip_update_tables(path=None, csv=True):
    """Check local json, update zips of outdated tables.

    Grabs the json files in path, checks them against the metadata on
    StatsCan and grabs updated tables where there have been changes
    There isn't actually a "last modified date" part to the metadata
    What I'm doing is comparing the latest reference period. Almost all
    data changes will at least include incremental releases, so this should
    capture what I want

    Parameters
    ----------
    path: str or pathlib.Path, default: None
        where to look for tables to update
    csv: boolean, default: True
        Downloads updates in CSV form by default, SDMX if false

    Returns
    -------
    update_table_list: list
        list of the tables that were updated

    """
    local_jsons = list_zipped_tables(path=path)
    tables = [j["productId"] for j in local_jsons]
    remote_jsons = get_cube_metadata(tables)
    update_table_list = [
        local["productId"]
        for local, remote in zip(local_jsons, remote_jsons)
        if local["cubeEndDate"] != remote["cubeEndDate"]
    ]

    download_tables(update_table_list, path, csv=csv)
    return update_table_list


def zip_table_to_dataframe(table, path=None):
    """Read a StatsCan table into a pandas DataFrame.

    If a zip file of the table does not exist in path, downloads it

    Parameters
    ----------
    table: str
        the table to load to dataframe from zipped csv
    path: str or pathlib.Path, default: current working directory when module is loaded
        where to download the tables or load them

    Returns
    -------
    df: pandas.DataFrame
        the table as a dataframe
    """
    path = pathlib.Path(path) if path else pathlib.Path()
    # Parse tables returns a list, can only do one table at a time here though
    table = parse_tables(table)[0]
    table_zip = table + "-eng.zip"
    table_zip = path / table_zip
    if not table_zip.is_file():
        download_tables([table], path)
    csv_file = table + ".csv"
    with zipfile.ZipFile(table_zip) as myzip:
        with myzip.open(csv_file) as myfile:
            col_names = pd.read_csv(myfile, nrows=0).columns
        # reopen the file or it misses the first row
        with myzip.open(csv_file) as myfile:
            types_dict = {"VALUE": float}
            types_dict.update({col: str for col in col_names if col not in types_dict})
            df = pd.read_csv(myfile, dtype=types_dict)

    possible_cats = [
        "GEO",
        "DGUID",
        "STATUS",
        "SYMBOL",
        "TERMINATED",
        "DECIMALS",
        "UOM",
        "UOM_ID",
        "SCALAR_FACTOR",
        "SCALAR_ID",
        "VECTOR",
        "COORDINATE",
        "Wages",
        "National Occupational Classification for Statistics (NOC-S)",
        "Supplementary unemployment rates",
        "Sex",
        "Age group",
        "Labour force characteristics",
        "Statistics",
        "Data type",
        "Job permanency",
        "Union coverage",
        "Educational attainment",
    ]
    actual_cats = [col for col in possible_cats if col in col_names]
    df[actual_cats] = df[actual_cats].astype("category")
    df["REF_DATE"] = pd.to_datetime(df["REF_DATE"], errors="ignore")
    return df


def list_zipped_tables(path=None):
    """List StatsCan tables available.

    defaults to looking in the current working directory and for zipped CSVs

    Parameters
    ----------
    path: string or path, default None
        Where to look for zipped tables

    Returns
    -------
    tables: list
        list of available tables json data
    """
    # Find json files
    path = pathlib.Path(path) if path else pathlib.Path.cwd()
    jsons = path.glob("*.json")
    tables = []
    for j in jsons:
        try:
            with open(j) as json_file:
                result = json.load(json_file)
                if "productId" in result:
                    tables.append(result)
        except ValueError as e:
            print("failed to read json file" + j)
            print(e)
    return tables


def vectors_to_df(vectors, periods=1, start_release_date=None, end_release_date=None):
    """Get DataFrame of vectors with n periods data or over range of release dates.

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
    df: DataFrame
        vectors as columns and ref_date as the index (not release date)
    """
    df = pd.DataFrame()
    if (end_release_date is None) | (start_release_date is None):
        start_list = get_data_from_vectors_and_latest_n_periods(vectors, periods)
    else:
        start_list = get_bulk_vector_data_by_range(
            vectors, start_release_date, end_release_date
        )
    for vec in start_list:
        name = "v" + str(vec["vectorId"])
        # If there's no data for the series just skip it
        if not vec["vectorDataPoint"]:
            continue
        ser = (
            pd.DataFrame(vec["vectorDataPoint"])
            .assign(refPer=lambda x: pd.to_datetime(x["refPer"], errors="ignore"))
            .set_index("refPer")
            .rename(columns={"value": name})
            .filter([name])
        )
        df = pd.concat([df, ser], axis=1, sort=True)
    return df


def code_sets_to_df_dict():
    """Get all code sets.

    Code sets provide additional metadata to describe
    information. Code sets are grouped into scales, frequencies, symbols etc.
    and returned as dictionary of dataframes.

    Returns
    -------
    pandas.Dataframe: list
        dictionary of dataframes
    """
    codes = get_code_sets()
    # Packs each code group in a dataframe for better lookup via dictionary
    codes_df_lookup = {key: pd.DataFrame(codes[key]) for key in codes.keys()}
    return codes_df_lookup
