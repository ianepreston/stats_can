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

"""

import datetime as dt
import functools
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
    parse_vectors,
    pad_coordinate
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
_MAX_CHUNK = 250
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


def _post_in_chunks(
    url: str, body: list[dict], schema: type[T]
) -> list[T]:
    """POST ``body`` to ``url`` in chunks of ``_MAX_CHUNK`` items.

    Validates each chunk's response against ``schema`` and concatenates the
    results. Sleeps ``_CHUNK_DELAY`` seconds between chunks.
    """
    chunks = [body[i : i + _MAX_CHUNK] for i in range(0, len(body), _MAX_CHUNK)]
    final_list: list[T] = []
    for i, chunk in enumerate(chunks):
        if i > 0:
            time.sleep(_CHUNK_DELAY)
        result = _fetch_and_validate(
            url, schema=schema, method="POST", json=chunk
        )
        final_list += result
    return final_list


def get_changed_series_list() -> list[ChangedSeries]:
    """[api reference](https://www.statcan.gc.ca/eng/developers/wds/user-guide#a10-1)

    Gets all series that were updated today.

    Returns
    -------
    :
        list of changed series, one for each vector and when it was released.
        Returns an empty list if no series have been released yet today.
    """
    try:
        return _fetch_and_validate(
            url=f"{SC_URL}getChangedSeriesList",
            schema=list[ChangedSeries],
        )
    except requests.HTTPError as exc:
        # The API returns 409 when no series have been released yet today,
        # which is a normal condition, not an error.
        if exc.response is not None and exc.response.status_code == 409:
            return []
        raise


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



def get_series_info_from_cube_pid_coord(
    pairs: tuple[str | int, str] | list[tuple[str | int, str]],
) -> list[SeriesInfo]:
    """[api reference](https://www.statcan.gc.ca/eng/developers/wds/user-guide#a11-2)

    Get series metadata for one or more (productId, coordinate) pairs.

    Parameters
    ----------
    pairs
        One pair, or a list of pairs, where each pair is
        ``(productId, coordinate)``. ``productId`` accepts the same forms
        as :func:`stats_can.helpers.parse_tables`
        (e.g. ``"25-10-0015-01"``, ``"25100015"``, ``25100015``).
        ``coordinate`` is the dot-delimited dimension member id string;
        it is right-padded with ``.0`` to the 10 positions the API requires.

    Returns
    -------
    :
        One :class:`SeriesInfo` per requested pair, in the order returned
        by the API.
    """
    if isinstance(pairs, tuple):
        pairs = [pairs]

    body = [
        {
            "productId": parse_tables(product_id)[0],
            "coordinate": pad_coordinate(coord),
        }
        for product_id, coord in pairs
    ]
    return _post_in_chunks(
        f"{SC_URL}getSeriesInfoFromCubePidCoord", body, SeriesInfo
    )


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
    body = [{"vectorId": v} for v in parse_vectors(vectors)]
    return _post_in_chunks(
        f"{SC_URL}getSeriesInfoFromVector", body, SeriesInfo
    )


def get_changed_series_data_from_cube_pid_coord(
    pairs: tuple[str | int, str] | list[tuple[str | int, str]],
) -> list[VectorData]:
    """[api reference](https://www.statcan.gc.ca/eng/developers/wds/user-guide#a12-1)

    Get changed-series data for one or more (productId, coordinate) pairs.

    Parameters
    ----------
    pairs
        One pair, or a list of pairs, where each pair is
        ``(productId, coordinate)``. ``productId`` accepts the same forms
        as :func:`stats_can.helpers.parse_tables`. ``coordinate`` is the
        dot-delimited dimension member id string; it is right-padded with
        ``.0`` to the 10 positions the API requires.

    Returns
    -------
    :
        One :class:`VectorData` per requested pair, in the order returned
        by the API.
    """
    if isinstance(pairs, tuple):
        pairs = [pairs]

    body = [
        {
            "productId": parse_tables(product_id)[0],
            "coordinate": pad_coordinate(coord),
        }
        for product_id, coord in pairs
    ]
    return _post_in_chunks(
        f"{SC_URL}getChangedSeriesDataFromCubePidCoord", body, VectorData
    )


def get_changed_series_data_from_vector(
    vectors: str | list[str],
) -> list[VectorData]:
    """[api reference](https://www.statcan.gc.ca/eng/developers/wds/user-guide#a12-2)

    Parameters
    ----------
    vectors
        vector numbers to get changed data for

    Returns
    -------
    :
        List of dicts containing changed data for each vector
    """
    body = [{"vectorId": v} for v in parse_vectors(vectors)]
    return _post_in_chunks(
        f"{SC_URL}getChangedSeriesDataFromVector", body, VectorData
    )


def get_data_from_cube_pid_coord_and_latest_n_periods(
    pairs: tuple[str | int, str] | list[tuple[str | int, str]],
    periods: int,
) -> list[VectorData]:
    """[api reference](https://www.statcan.gc.ca/eng/developers/wds/user-guide#a12-3)

    Get the latest ``periods`` reference periods of data for one or more
    (productId, coordinate) pairs.

    Parameters
    ----------
    pairs
        One pair, or a list of pairs, where each pair is
        ``(productId, coordinate)``. ``productId`` accepts the same forms
        as :func:`stats_can.helpers.parse_tables`. ``coordinate`` is the
        dot-delimited dimension member id string; it is right-padded with
        ``.0`` to the 10 positions the API requires.
    periods
        number of periods (starting at latest) to retrieve data for; applied
        to every pair.

    Returns
    -------
    :
        One :class:`VectorData` per requested pair, in the order returned
        by the API.
    """
    if isinstance(pairs, tuple):
        pairs = [pairs]

    body = [
        {
            "productId": parse_tables(product_id)[0],
            "coordinate": pad_coordinate(coord),
            "latestN": periods,
        }
        for product_id, coord in pairs
    ]
    return _post_in_chunks(
        f"{SC_URL}getDataFromCubePidCoordAndLatestNPeriods", body, VectorData
    )


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
    body = [
        {"vectorId": v, "latestN": periods} for v in parse_vectors(vectors)
    ]
    return _post_in_chunks(
        f"{SC_URL}getDataFromVectorsAndLatestNPeriods", body, VectorData
    )


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


@functools.lru_cache(maxsize=1)
def get_code_sets() -> CodeSet:
    """[api reference](https://www.statcan.gc.ca/eng/developers/wds/user-guide#a13-1)

    Gets all code sets which provide additional information to describe
    information and are grouped into scales, frequencies, symbols etc.

    Results are cached after the first call. Call
    ``get_code_sets.cache_clear()`` to force a refresh.

    Returns
    -------
    :
        one dictionary for each group of information
    """
    url = f"{SC_URL}getCodeSets"
    return _fetch_and_validate(url, schema=CodeSet)
