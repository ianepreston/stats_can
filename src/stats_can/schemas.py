"""Objects representing what's returned from the Statistics Canada Web Data Service.

Having these created allows pydantic validation without doing crazy long
type signatures. It should also help your editor pick up on what
attributes are available, and its useful for referencing things in the docs.
"""

from typing import Any
from typing_extensions import TypedDict


class ChangedSeries(TypedDict):
    responseStatusCode: int
    vectorId: int
    productId: int
    coordinate: str
    releaseTime: str


class ChangedCube(TypedDict):
    responseStatusCode: int
    productId: int
    releaseTime: str


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
