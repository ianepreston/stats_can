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
from typing import TypeVar, TypedDict, Any
from pydantic import TypeAdapter

import requests
from requests import Response

from stats_can.helpers import (
    chunk_vectors,
    parse_tables,
)

SC_URL = "https://www150.statcan.gc.ca/t1/wds/rest/"

T = TypeVar("T")


def _fetch_and_validate(url: str, schema: type[T], method: str = "GET", **kwargs) -> T:
    """
    Centralized handler for:
    1. HTTP Request
    2. Status Checking
    3. JSON Extraction
    4. Pydantic Runtime Validation
    """
    response: Response = requests.request(method, url, **kwargs)
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


class ChangedSeries(TypedDict):
    responseStatusCode: int
    vectorId: int
    productId: int
    coordinate: str
    releaseTime: str


def get_changed_series_list() -> list[ChangedSeries]:
    """https://www.statcan.gc.ca/eng/developers/wds/user-guide#a10-1

    Gets all series that were updated today.

    Returns
    -------
    list of changed series
        one for each vector and when it was released
    """
    return _fetch_and_validate(
        url=f"{SC_URL}getChangedSeriesList",
        schema=list[ChangedSeries],
    )


class ChangedCube(TypedDict):
    responseStatusCode: int
    productId: int
    releaseTime: str


def get_changed_cube_list(date: dt.date | None = None) -> list[ChangedCube]:
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
    if date is None:
        date = dt.date.today()
    return _fetch_and_validate(
        url=f"{SC_URL}getChangedCubeList/{date}", schema=list[ChangedCube]
    )


class Member(TypedDict):
    memberId: int
    parentMemberId: int | None
    memberNameEn: str
    memberNameFr: str
    classificationCode: str | None
    classificationTypeCode: str | None
    geoLevel: int | None
    vintage: int | None
    terminated: int
    memberUomCode: int | None


class Dimension(TypedDict):
    dimensionPositionId: int
    dimensionNameEn: str
    dimensionNameFr: str
    hasUom: bool
    member: list[Member]


class FootNoteLink(TypedDict):
    footnoteId: int
    dimensionPositionId: int
    memberId: int


class Footnote(TypedDict):
    footnoteId: int
    footnotesEn: str
    footnotesFr: str
    link: FootNoteLink


class CubeMetadata(TypedDict):
    responseStatusCode: int
    productId: int
    cansimId: str
    cubeTitleEn: str
    cubeTitleFr: str
    cubeStartDate: str
    cubeEndDate: str
    frequencyCode: int
    nbSeriesCube: int
    nbDatapointsCube: int
    releaseTime: str
    archiveStatusCode: str
    archiveStatusEn: str
    archiveStatusFr: str
    subjectCode: list[str]
    surveyCode: list[str]
    dimension: list[Dimension]
    footnote: list[Footnote]
    correction: list[Any]
    correctionFootnote: list[Any]
    issueDate: str


def get_cube_metadata(tables: str | list[str]) -> list[CubeMetadata] | CubeMetadata:
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
    tables_json = [{"productId": t} for t in tables]
    url = f"{SC_URL}getCubeMetadata"
    return _fetch_and_validate(
        url, schema=CubeMetadata, method="POST", json=tables_json
    )


def get_series_info_from_cube_pid_coord():
    """Not implemented yet

    https://www.statcan.gc.ca/eng/developers/wds/user-guide#a11-2
    """
    pass


class SeriesInfo(TypedDict):
    responseStatusCode: int
    productId: int
    coordinate: str
    vectorId: int
    frequencyCode: int
    scalarFactorCode: int
    decimals: int
    terminated: int
    SeriesTitleEn: str
    SeriesTitleFr: str
    memberUomCode: int


def get_series_info_from_vector(vectors: str | list[str]) -> list[SeriesInfo]:
    """https://www.statcan.gc.ca/eng/developers/wds/user-guide#a11-3

    Parameters
    ----------
    vectors: str or list of str
        vector numbers to get info for

    Returns
    -------
    List of dicts containing metadata for each v#
    """
    url = f"{SC_URL}getSeriesInfoFromVector"
    chunks = chunk_vectors(vectors)
    final_list = []
    for chunk in chunks:
        vector_dict = [{"vectorId": v} for v in chunk]
        result = _fetch_and_validate(
            url, schema=SeriesInfo, method="POST", json=vector_dict
        )
        final_list += result
    return final_list


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


class VectorDataPoint(TypedDict):
    refPer: str
    refPer2: str
    refPerRaw: str
    refPerRaw2: str
    value: str | int | float
    decimals: int
    scalarFactorCode: int
    symbolCode: int
    statusCode: int
    securityLevelCode: int
    releaseTime: str
    frequencyCode: int


class VectorData(TypedDict):
    responseStatusCode: int
    productId: int
    coordinate: str
    vectorId: int
    vectorDataPoint: list[VectorDataPoint]


def get_data_from_vectors_and_latest_n_periods(
    vectors: str | list[str], periods: int
) -> list[VectorData]:
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
    url = f"{SC_URL}getDataFromVectorsAndLatestNPeriods"
    chunks = chunk_vectors(vectors)
    final_list = []
    for chunk in chunks:
        periods_l = [periods for i in range(len(chunk))]
        json = [{"vectorId": v, "latestN": n} for v, n in zip(chunk, periods_l)]
        result = _fetch_and_validate(url, schema=VectorData, method="POST", json=json)
        final_list += result
    return final_list


def get_bulk_vector_data_by_range(
    vectors: str | list[str], start_release_date: dt.date, end_release_date: dt.date
) -> list[VectorData]:
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
    url = f"{SC_URL}getBulkVectorDataByRange"
    start_release_date_str = str(start_release_date) + "T13:00"
    end_release_date_str = str(end_release_date) + "T13:00"
    chunks = chunk_vectors(vectors)
    final_list = []
    for vector_ids in chunks:
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
    """https://www.statcan.gc.ca/eng/developers/wds/user-guide#a12-5a

    Parameters
    ----------
    vectors: str or list of str
        vector numbers to get info for
    start_ref_date: datetime.date
        start reference period date for the data
    end_ref_date: datetime.date
        end reference period date for the data

    Returns
    -------
    List of dicts containing data for each vector
    """
    url = f"{SC_URL}getDataFromVectorByReferencePeriodRange"
    chunks = chunk_vectors(vectors)
    final_list = []
    for vector_ids in chunks:
        # I know the rest are .post, they changed it just for this one
        v_string = ",".join(f"{v}" for v in vector_ids)
        vector_param = f"vectorIds={v_string}"
        full_url = f"{url}?{vector_param}&startRefPeriod={start_ref_date}&endReferencePeriod={end_ref_date}"
        result = _fetch_and_validate(url=full_url, schema=VectorData)
        final_list += result
    return final_list


def get_full_table_download(table: str, csv: bool = True) -> str:
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
    parsed_table = parse_tables(table)[0]
    if csv:
        url = f"{SC_URL}getFullTableDownloadCSV/{parsed_table}/en"
    else:
        url = f"{SC_URL}getFullTableDownloadSDMX/{parsed_table}"
    return _fetch_and_validate(url, schema=str)


class ScalarFactor(TypedDict):
    scalarFactorCode: int
    scalarFactorDescEn: str
    scalarFactorDescFr: str


class FrequencyCode(TypedDict):
    frequencyCode: int
    frequencyDescEn: str
    frequencyDescFr: str


class SymbolCode(TypedDict):
    symbolCode: int
    symbolDescEn: str
    symbolDescFr: str


class StatusCode(TypedDict):
    statusCode: int
    statusDescEn: str
    statusDescFr: str


class UomCode(TypedDict):
    memberUomCode: int
    memberUomEn: str | None
    memberUomFr: str | None


class SurveyCode(TypedDict):
    surveyCode: int
    surveyEn: str | None
    surveyFr: str | None


class SubjectCode(TypedDict):
    subjectCode: int
    subjectEn: str | None
    subjectFr: str | None


class ClassificationTypeCode(TypedDict):
    classificationTypeCode: int
    classificationTypeEn: str | None
    classificationTypeFr: str | None


class SecurityLevelCode(TypedDict):
    securityLevelCode: int
    securityLevelRepresentationEn: str | None
    securityLevelRepresentationFr: str | None
    securityLevelDescEn: str
    securityLevelDescFr: str


class CodeCode(TypedDict):
    codeId: int
    codeTextEn: str
    codeTextFr: str


class CodeSet(TypedDict):
    scalar: list[ScalarFactor]
    frequency: list[FrequencyCode]
    symbol: list[SymbolCode]
    status: list[StatusCode]
    uom: list[UomCode]
    survey: list[SurveyCode]
    subject: list[SubjectCode]
    classificationType: list[ClassificationTypeCode]
    securityLevel: list[SecurityLevelCode]
    terminated: list[CodeCode]
    wdsResponseStatus: list[CodeCode]


def get_code_sets() -> CodeSet:
    """https://www.statcan.gc.ca/eng/developers/wds/user-guide#a13-1

    Gets all code sets which provide additional information to describe
    information and are grouped into scales, frequencies, symbols etc.

    Returns
    -------
    list of dicts
        one dictionary for each group of information
    """
    url = f"{SC_URL}getCodeSets"
    return _fetch_and_validate(url, schema=CodeSet)
