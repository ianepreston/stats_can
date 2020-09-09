"""Functions that allow the package to return exactly what the api gives.

https://www.statcan.gc.ca/eng/developers/wds/user-guide

Note: StatsCan uses cube/table interchangeably. I'm going to keep cube in my
function names where it maps to their api but otherwise I will use table.
Hence functions with cube in the function name will take tables as an argument
I'm not sure which is less confusing, it's annoying they weren't just
consistent.

Attributes
----------
SC_URL : str
    URL for the Statistics Canada REST api

TODO
----
Missing api implementations:
    GetSeriesInfoFromCubePidCoord
    GetChangedSeriesDataFromCubePidCoord
    GetChangedSeriesDataFromVector
    GetDataFromCubePidCoordAndLatestNPeriods
    GetFullTableDownloadSDMX
"""
import datetime as dt

import requests

from stats_can.helpers import check_status, chunk_vectors, parse_tables


SC_URL = "https://www150.statcan.gc.ca/t1/wds/rest/"


def get_changed_series_list():
    """https://www.statcan.gc.ca/eng/developers/wds/user-guide#a10-1

    Gets all series that were updated today.

    Returns
    -------
    list of dicts
        one for each vector and when it was released
    """
    url = SC_URL + "getChangedSeriesList"
    result = requests.get(url)
    result = check_status(result)
    return result["object"]


def get_changed_cube_list(date=None):
    """https://www.statcan.gc.ca/eng/developers/wds/user-guide#a10-2

    Parameters
    ----------
    date : datetime.date
        Date to check for table changes, defaults to current date

    Returns
    -------
    list of dicts
        one for each table and when it was updated
    """
    if not date:
        date = dt.date.today()
    url = SC_URL + "getChangedCubeList" + "/" + str(date)
    result = requests.get(url)
    result = check_status(result)
    return result["object"]


def get_cube_metadata(tables):
    """https://www.statcan.gc.ca/eng/developers/wds/user-guide#a11-1

    Take a list of tables and return a list of dictionaries with their
    metadata

    Parameters
    ----------
    tables : str or list of str
        IDs of tables to get metadata for

    Returns
    -------
    list of dicts
        one for each table with its metadata
    """
    tables = parse_tables(tables)
    tables = [{"productId": t} for t in tables]
    url = SC_URL + "getCubeMetadata"
    result = requests.post(url, json=tables)
    result = check_status(result)
    return [r["object"] for r in result]


def get_series_info_from_cube_pid_coord():
    """Not implemented yet

    https://www.statcan.gc.ca/eng/developers/wds/user-guide#a11-2
    """
    pass


def get_series_info_from_vector(vectors):
    """https://www.statcan.gc.ca/eng/developers/wds/user-guide#a11-3

    Parameters
    ----------
    vectors: str or list of str
        vector numbers to get info for

    Returns
    -------
    List of dicts containing metadata for each v#
    """
    url = SC_URL + "getSeriesInfoFromVector"
    chunks = chunk_vectors(vectors)
    final_list = []
    for chunk in chunks:
        vectors = [{"vectorId": v} for v in chunk]
        result = requests.post(url, json=vectors)
        result = check_status(result)
        final_list += result
    return [r["object"] for r in final_list]


def get_changed_series_data_from_cube_pid_coord():
    """Not implemented yet

    https://www.statcan.gc.ca/eng/developers/wds/user-guide#a12-1
    """
    pass


def get_changed_series_data_from_vector():
    """Not implemented yet

    https://www.statcan.gc.ca/eng/developers/wds/user-guide#a12-2
    """
    pass


def get_data_from_cube_pid_coord_and_latest_n_periods():
    """Not implemented yet

    https://www.statcan.gc.ca/eng/developers/wds/user-guide#a12-3
    """
    pass


def get_data_from_vectors_and_latest_n_periods(vectors, periods):
    """https://www.statcan.gc.ca/eng/developers/wds/user-guide#a12-4

    Parameters
    ----------
    vectors: str or list of str
        vector numbers to get info for
    periods: int
        number of periods (starting at latest) to retrieve data for

    Returns
    -------
    List of dicts containing data for each vector
    """
    url = SC_URL + "getDataFromVectorsAndLatestNPeriods"
    chunks = chunk_vectors(vectors)
    final_list = []
    for chunk in chunks:
        periods_l = [periods for i in range(len(chunk))]
        json = [{"vectorId": v, "latestN": n} for v, n in zip(chunk, periods_l)]
        result = requests.post(url, json=json)
        result = check_status(result)
        final_list += [r["object"] for r in result]
    return final_list


def get_bulk_vector_data_by_range(vectors, start_release_date, end_release_date):
    """https://www.statcan.gc.ca/eng/developers/wds/user-guide#a12-5

    Parameters
    ----------
    vectors: str or list of str
        vector numbers to get info for
    start_release_date: datetime.date
        start release date for the data
    end_release_date: datetime.date
        end release date for the data

    Returns
    -------
    List of dicts containing data for each vector
    """
    url = SC_URL + "getBulkVectorDataByRange"
    start_release_date = str(start_release_date) + "T13:00"
    end_release_date = str(end_release_date) + "T13:00"
    chunks = chunk_vectors(vectors)
    final_list = []
    for vector_ids in chunks:
        result = requests.post(
            url,
            json={
                "vectorIds": vector_ids,
                "startDataPointReleaseDate": start_release_date,
                "endDataPointReleaseDate": end_release_date,
            },
        )
        result = check_status(result)
        final_list += [r["object"] for r in result]
    return final_list


def get_full_table_download(table, csv=True):
    """Take a table name and return a url to a zipped file of that table.

    https://www.statcan.gc.ca/eng/developers/wds/user-guide#a12-6
    https://www.statcan.gc.ca/eng/developers/wds/user-guide#a12-7


    Parameters
    ----------
    table: str
        table name to download
    csv: boolean, default True
        download in CSV format, if not download SDMX

    Returns
    -------
    str:
        path to the file download
    """
    table = parse_tables(table)[0]
    if csv:
        url = SC_URL + "getFullTableDownloadCSV/" + table + "/en"
    else:
        url = SC_URL + "getFullTableDownloadSDMX/" + table
    result = requests.get(url)
    result = check_status(result)
    return result["object"]


def get_code_sets():
    """https://www.statcan.gc.ca/eng/developers/wds/user-guide#a13-1

    Gets all code sets which provide additional information to describe
    information and are grouped into scales, frequencies, symbols etc.

    Returns
    -------
    list of dicts
        one dictionary for each group of information
    """
    url = SC_URL + "getCodeSets"
    result = requests.get(url)
    result = check_status(result)

    return result["object"]
