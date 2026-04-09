"""Functions that allow the package to return exactly what the api gives.

[api reference](https://www.statcan.gc.ca/eng/developers/wds/user-guide)

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
import time
from importlib.metadata import version
from typing import TypeVar
from pydantic import TypeAdapter

import requests
from requests import Response
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from stats_can.helpers import (
    chunk_vectors,
    parse_tables,
)
from stats_can.schemas import (
    ChangedSeries,
    ChangedCube,
    CubeMetadata,
    SeriesInfo,
    VectorData,
    CodeSet,
)

SC_URL = "https://www150.statcan.gc.ca/t1/wds/rest/"
DEFAULT_TIMEOUT = 30
_CHUNK_DELAY = 0.1
_USER_AGENT = f"stats_can/{version('stats_can')}"

T = TypeVar("T")

_retry = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST"],
)
_adapter = HTTPAdapter(max_retries=_retry)

_session = requests.Session()
_session.headers["User-Agent"] = _USER_AGENT
_session.mount("https://", _adapter)
_session.mount("http://", _adapter)


def _fetch_and_validate(
    url: str, schema: type[T], method: str = "GET", **kwargs
) -> T | list[T]:
    """Fetch from the StatsCan API, check status, and validate with Pydantic.

    Returns a single ``T`` when the API responds with a dict wrapper, or
    ``list[T]`` when the API responds with a list of wrappers (bulk endpoints).
    """
    kwargs.setdefault("timeout", DEFAULT_TIMEOUT)
    response: Response = _session.request(method, url, **kwargs)
    response.raise_for_status()
    data = response.json()

    # Handle StatsCan sometimes returning lists and sometimes just a single object
    items = data if isinstance(data, list) else [data]
    for item in items:
        if item.get("status") != "SUCCESS":
            raise RuntimeError(str(item.get("object")))
    adapter = TypeAdapter(schema)
    if isinstance(data, dict):
        payload = data.get("object")
        return adapter.validate_python(payload)
    elif isinstance(data, list):
        payload = [d.get("object") for d in data]
        return [adapter.validate_python(p) for p in payload]
    else:
        raise RuntimeError(f"data came back weird. We should never get here: {data}")


def get_changed_series_list() -> list[ChangedSeries]:
    """[api reference](https://www.statcan.gc.ca/eng/developers/wds/user-guide#a10-1)

    Gets all series that were updated today.

    Returns
    -------
    :
        list of changed series, one for each vector and when it was released
    """
    return _fetch_and_validate(
        url=f"{SC_URL}getChangedSeriesList",
        schema=list[ChangedSeries],
    )


def get_changed_cube_list(date: dt.date | None = None) -> list[ChangedCube]:
    """[api reference](https://www.statcan.gc.ca/eng/developers/wds/user-guide#a10-2)

    Parameters
    ----------
    date
        Date to check for table changes, defaults to current date

    Returns
    -------
    :
        list of changed cubes, one for each table and when it was updated
    """
    if date is None:
        date = dt.date.today()
    return _fetch_and_validate(
        url=f"{SC_URL}getChangedCubeList/{date}", schema=list[ChangedCube]
    )


def get_cube_metadata(tables: str | list[str]) -> list[CubeMetadata]:
    """[api reference](https://www.statcan.gc.ca/eng/developers/wds/user-guide#a11-1)

    Take a list of tables and return a list of dictionaries with their
    metadata

    Parameters
    ----------
    tables
        IDs of tables to get metadata for

    Returns
    -------
    :
        one for each table with its metadata
    """
    tables = parse_tables(tables)
    tables_json = [{"productId": t} for t in tables]
    url = f"{SC_URL}getCubeMetadata"
    return _fetch_and_validate(
        url, schema=CubeMetadata, method="POST", json=tables_json
    )


def get_series_info_from_cube_pid_coord():
    """Not implemented yet

    [api reference](https://www.statcan.gc.ca/eng/developers/wds/user-guide#a11-2)
    """
    pass


def get_series_info_from_vector(vectors: str | list[str]) -> list[SeriesInfo]:
    """[api reference](https://www.statcan.gc.ca/eng/developers/wds/user-guide#a11-3)

    Parameters
    ----------
    vectors
        vector numbers to get info for

    Returns
    -------
    :
        List of dicts containing metadata for each v#
    """
    url = f"{SC_URL}getSeriesInfoFromVector"
    chunks = chunk_vectors(vectors)
    final_list = []
    for i, chunk in enumerate(chunks):
        if i > 0:
            time.sleep(_CHUNK_DELAY)
        vector_dict = [{"vectorId": v} for v in chunk]
        result = _fetch_and_validate(
            url, schema=SeriesInfo, method="POST", json=vector_dict
        )
        final_list += result
    return final_list


def get_changed_series_data_from_cube_pid_coord():
    """Not implemented yet

    [api reference](https://www.statcan.gc.ca/eng/developers/wds/user-guide#a12-1)
    """
    pass


def get_changed_series_data_from_vector():
    """Not implemented yet

    [api reference](https://www.statcan.gc.ca/eng/developers/wds/user-guide#a12-2)
    """
    pass


def get_data_from_cube_pid_coord_and_latest_n_periods():
    """Not implemented yet

    [api reference](https://www.statcan.gc.ca/eng/developers/wds/user-guide#a12-3)
    """
    pass


def get_data_from_vectors_and_latest_n_periods(
    vectors: str | list[str], periods: int
) -> list[VectorData]:
    """[api reference](https://www.statcan.gc.ca/eng/developers/wds/user-guide#a12-4)

    Parameters
    ----------
    vectors
        vector numbers to get info for
    periods
        number of periods (starting at latest) to retrieve data for

    Returns
    -------
    :
        List of dicts containing data for each vector
    """
    url = f"{SC_URL}getDataFromVectorsAndLatestNPeriods"
    chunks = chunk_vectors(vectors)
    final_list = []
    for i, chunk in enumerate(chunks):
        if i > 0:
            time.sleep(_CHUNK_DELAY)
        periods_l = [periods for i in range(len(chunk))]
        json = [{"vectorId": v, "latestN": n} for v, n in zip(chunk, periods_l)]
        result = _fetch_and_validate(url, schema=VectorData, method="POST", json=json)
        final_list += result
    return final_list


def get_bulk_vector_data_by_range(
    vectors: str | list[str], start_release_date: dt.date, end_release_date: dt.date
) -> list[VectorData]:
    """[api reference](https://www.statcan.gc.ca/eng/developers/wds/user-guide#a12-5)

    Parameters
    ----------
    vectors
        vector numbers to get info for
    start_release_date
        start release date for the data
    end_release_date
        end release date for the data

    Returns
    -------
    :
        List of dicts containing data for each vector
    """
    url = f"{SC_URL}getBulkVectorDataByRange"
    start_release_date_str = str(start_release_date) + "T13:00"
    end_release_date_str = str(end_release_date) + "T13:00"
    chunks = chunk_vectors(vectors)
    final_list = []
    for i, vector_ids in enumerate(chunks):
        if i > 0:
            time.sleep(_CHUNK_DELAY)
        result = _fetch_and_validate(
            url,
            schema=VectorData,
            method="POST",
            json={
                "vectorIds": vector_ids,
                "startDataPointReleaseDate": start_release_date_str,
                "endDataPointReleaseDate": end_release_date_str,
            },
        )
        final_list += result
    return final_list


def get_bulk_vector_data_by_reference_period_range(
    vectors: str | list[str], start_ref_date: dt.date, end_ref_date: dt.date
) -> list[VectorData]:
    """[api reference](https://www.statcan.gc.ca/eng/developers/wds/user-guide#a12-5a)

    Parameters
    ----------
    vectors
        vector numbers to get info for
    start_ref_date
        start reference period date for the data
    end_ref_date
        end reference period date for the data

    Returns
    -------
    :
        List of dicts containing data for each vector
    """
    url = f"{SC_URL}getDataFromVectorByReferencePeriodRange"
    chunks = chunk_vectors(vectors)
    final_list = []
    for i, vector_ids in enumerate(chunks):
        if i > 0:
            time.sleep(_CHUNK_DELAY)
        # I know the rest are .post, they changed it just for this one
        v_string = ",".join(f"{v}" for v in vector_ids)
        vector_param = f"vectorIds={v_string}"
        full_url = f"{url}?{vector_param}&startRefPeriod={start_ref_date}&endReferencePeriod={end_ref_date}"
        result = _fetch_and_validate(url=full_url, schema=VectorData)
        final_list += result
    return final_list


def get_full_table_download(table: str, csv: bool = True) -> str:
    """Take a table name and return a url to a zipped file of that table.

    [api reference](https://www.statcan.gc.ca/eng/developers/wds/user-guide#a12-6)
    [api reference](https://www.statcan.gc.ca/eng/developers/wds/user-guide#a12-7)


    Parameters
    ----------
    table
        table name to download
    csv
        download in CSV format, if not download SDMX

    Returns
    -------
    :
        path to the file download
    """
    parsed_table = parse_tables(table)[0]
    if csv:
        url = f"{SC_URL}getFullTableDownloadCSV/{parsed_table}/en"
    else:
        url = f"{SC_URL}getFullTableDownloadSDMX/{parsed_table}"
    return _fetch_and_validate(url, schema=str)


def get_code_sets() -> CodeSet:
    """[api reference](https://www.statcan.gc.ca/eng/developers/wds/user-guide#a13-1)

    Gets all code sets which provide additional information to describe
    information and are grouped into scales, frequencies, symbols etc.

    Returns
    -------
    :
        one dictionary for each group of information
    """
    url = f"{SC_URL}getCodeSets"
    return _fetch_and_validate(url, schema=CodeSet)
