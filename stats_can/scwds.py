# -*- coding: utf-8 -*-
"""Functions that allow the package to return exactly what the api gives
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
    GetCodeSets
@author: Ian Preston
"""
import re
import datetime as dt
import requests
SC_URL = 'https://www150.statcan.gc.ca/t1/wds/rest/'


def check_status(results):
    """Make sure list of results succeeded

    Parameters
    ----------
    results : list of dicts, or dict
        JSON from an API call parsed as a dictionary
    """
    results.raise_for_status()
    results = results.json()

    def check_one_status(result):
        """Do the check on an individual result"""
        if result['status'] != 'SUCCESS':
            raise RuntimeError(str(result['object']))
    if isinstance(results, list):
        for result in results:
            check_one_status(result)
    else:
        check_one_status(results)
    return results


def parse_tables(tables):
    """ Basic cleanup of table or tables to numeric


    Strip out hyphens or other non-numeric characters from a list of tables
    or a single table
    Table names in StatsCan often have a trailing -01 which isn't necessary
    So also take just the first 8 characters.
    This function by no means guarantees you have a clean list of valid tables,
    but it's a good start.

    Parameters
    ----------
    tables : list of str or str
        A string or list of strings of table names to be parsed

    Returns
    -------
    list of str
        tables with unnecessary characters removed
    """
    def parse_table(table):
        """Clean up one table string"""
        return re.sub(r'\D', '', table)[:8]

    if isinstance(tables, str):
        return [parse_table(tables)]
    return [parse_table(t) for t in tables]


def parse_vectors(vectors):
    """ Basic cleanup of vector or vectors

    Strip out V from V#s. Similar to parse tables, this by no means guarantees
    a valid entry, just helps with some standard input formats

    Parameters
    ----------
    vectors : list of str or str
        A string or list of strings of vector names to be parsed

    Returns
    -------
    list of str
        vectors with unnecessary characters removed
    """
    def parse_vector(vector):
        """Strip string to numeric elements only"""
        if isinstance(vector, int):  # Already parsed earlier
            return vector
        return int(re.sub(r'\D', '', vector))

    if isinstance(vectors, str):
        return [parse_vector(vectors)]
    elif isinstance(vectors, int):
        return [parse_vector(vectors)]
    return [parse_vector(v) for v in vectors]


def chunk_vectors(vectors):
    """api calls max out at 300 vectors so break list into chunks"""
    MAX_CHUNK = 250
    vectors = parse_vectors(vectors)
    chunks = [
        vectors[i:i + MAX_CHUNK] for i in range(0, len(vectors), MAX_CHUNK)
        ]
    return chunks


def get_changed_series_list():
    """https://www.statcan.gc.ca/eng/developers/wds/user-guide#a10-1

    Gets all series that were updated today.

    Returns
    -------
    list of dicts
        one for each vector and when it was released
    """
    url = SC_URL + 'getChangedSeriesList'
    result = requests.get(url)
    result = check_status(result)
    return result['object']


def get_changed_cube_list(date=dt.date.today()):
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
    url = SC_URL + 'getChangedCubeList' + '/' + str(date)
    result = requests.get(url)
    result = check_status(result)
    return result['object']


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
    tables = [{'productId': t} for t in tables]
    url = SC_URL + 'getCubeMetadata'
    result = requests.post(url, json=tables)
    result.raise_for_status()
    result = check_status(result)
    return [r['object'] for r in result]


def get_series_info_from_cube_pid_coord():
    """ Not implemented yet

    https://www.statcan.gc.ca/eng/developers/wds/user-guide#a11-2"""
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
    url = SC_URL + 'getSeriesInfoFromVector'
    chunks = chunk_vectors(vectors)
    final_list = []
    for chunk in chunks:
        vectors = [{'vectorId': v} for v in chunk]
        result = requests.post(url, json=vectors)
        result = check_status(result)
        final_list += result
    return [r['object'] for r in final_list]


def get_changed_series_data_from_cube_pid_coord():
    """ Not implemented yet

    https://www.statcan.gc.ca/eng/developers/wds/user-guide#a12-1"""
    pass


def get_changed_series_data_from_vector():
    """ Not implemented yet

    https://www.statcan.gc.ca/eng/developers/wds/user-guide#a12-2"""
    pass


def get_data_from_cube_pid_coord_and_latest_n_periods():
    """ Not implemented yet

    https://www.statcan.gc.ca/eng/developers/wds/user-guide#a12-3
    """
    pass


def get_data_from_vectors_and_latest_n_periods(vectors, periods):
    """ https://www.statcan.gc.ca/eng/developers/wds/user-guide#a12-4

    Parameters
    ----------
    vectors: str or list of str
        vector numbers to get info for
    periods: int
        number of periods (starting at latest) to retrieve data for

    Returns
    -------
    List of dicts containing data for each vector

    ToDo
    ----
    Add chunking to handle over 300 vectors
    """
    url = SC_URL + 'getDataFromVectorsAndLatestNPeriods'
    chunks = chunk_vectors(vectors)
    final_list = []
    for chunk in chunks:
        periods_l = [periods for i in range(len(chunk))]
        json = [
            {'vectorId': v, 'latestN': n} for v, n in zip(chunk, periods_l)
            ]
        result = requests.post(url, json=json)
        result = check_status(result)
        final_list += [r['object'] for r in result]
    return final_list


def get_bulk_vector_data_by_range(
        vectors, start_release_date, end_release_date
):
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
    url = SC_URL + 'getBulkVectorDataByRange'
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
                "endDataPointReleaseDate": end_release_date
                }
            )
        result = check_status(result)
        final_list += [r['object'] for r in result]
    return final_list


def get_full_table_download(table, csv=True):
    """https://www.statcan.gc.ca/eng/developers/wds/user-guide#a12-6
    https://www.statcan.gc.ca/eng/developers/wds/user-guide#a12-7

    Take a table name and return a url to a zipped file of that table

    Parameters
    ----------
    table: str
        table name to download
    csv: boolean, default True
        download in CSV format, if not download SDMX
    """
    table = parse_tables(table)[0]
    if csv:
        url = SC_URL + 'getFullTableDownloadCSV/' + table + '/en'
    else:
        url = SC_URL + 'getFullTableDownloadSDMX/' + table
    result = requests.get(url)
    result = check_status(result)
    return result['object']


def get_code_sets():
    """ Not implemented yet

    https://www.statcan.gc.ca/eng/developers/wds/user-guide#a13-1
    """
    pass