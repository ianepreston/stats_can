"""Tests for error handling paths that don't require the live API."""

from unittest.mock import patch, MagicMock

import pytest
import requests

import stats_can
from stats_can import scwds


def _mock_response(status_code=200, json_data=None, raise_for_status=None):
    """Build a mock requests.Response."""
    mock = MagicMock(spec=requests.Response)
    mock.status_code = status_code
    mock.json.return_value = json_data
    if raise_for_status:
        mock.raise_for_status.side_effect = raise_for_status
    else:
        mock.raise_for_status.return_value = None
    return mock


class TestFetchAndValidateErrors:
    """Tests for _fetch_and_validate error paths."""

    def test_http_error_raises(self):
        """HTTP 500 should raise HTTPError."""
        mock_resp = _mock_response(
            status_code=500,
            raise_for_status=requests.exceptions.HTTPError("500 Server Error"),
        )
        with patch.object(scwds._session, "request", return_value=mock_resp):
            with pytest.raises(requests.exceptions.HTTPError):
                scwds._fetch_and_validate(
                    "https://example.com", schema=str, method="GET"
                )

    def test_api_failure_status_raises_runtime_error(self):
        """Non-SUCCESS status in response body should raise RuntimeError."""
        mock_resp = _mock_response(
            json_data={"status": "FAILED", "object": "Vector not found"}
        )
        with patch.object(scwds._session, "request", return_value=mock_resp):
            with pytest.raises(RuntimeError, match="Vector not found"):
                scwds._fetch_and_validate(
                    "https://example.com", schema=str, method="GET"
                )

    def test_api_failure_in_list_raises_runtime_error(self):
        """Non-SUCCESS in a list response should raise RuntimeError."""
        mock_resp = _mock_response(
            json_data=[{"status": "FAILED", "object": "Bad request"}]
        )
        with patch.object(scwds._session, "request", return_value=mock_resp):
            with pytest.raises(RuntimeError, match="Bad request"):
                scwds._fetch_and_validate(
                    "https://example.com", schema=str, method="GET"
                )

    def test_timeout_raises(self):
        """Request timeout should propagate as ReadTimeout."""
        with patch.object(
            scwds._session,
            "request",
            side_effect=requests.exceptions.ReadTimeout("timed out"),
        ):
            with pytest.raises(requests.exceptions.ReadTimeout):
                scwds._fetch_and_validate(
                    "https://example.com", schema=str, method="GET"
                )

    def test_connection_error_raises(self):
        """Network failure should propagate as ConnectionError."""
        with patch.object(
            scwds._session,
            "request",
            side_effect=requests.exceptions.ConnectionError("DNS failed"),
        ):
            with pytest.raises(requests.exceptions.ConnectionError):
                scwds._fetch_and_validate(
                    "https://example.com", schema=str, method="GET"
                )


class TestDownloadTableErrors:
    """Tests for download_tables error paths."""

    def test_download_bad_status_raises(self, tmpdir):
        """A 404 from the download URL should raise, not write garbage to disk."""
        mock_meta = {
            "productId": 99999999,
            "cubeEndDate": "2024-01-01",
        }
        mock_url_resp = _mock_response(
            json_data={"status": "SUCCESS", "object": "https://example.com/fake.zip"}
        )
        mock_download_resp = _mock_response(
            status_code=404,
            raise_for_status=requests.exceptions.HTTPError("404 Not Found"),
        )

        with (
            patch.object(scwds._session, "request", return_value=mock_url_resp),
            patch("stats_can.sc.get_cube_metadata", return_value=[mock_meta]),
            patch(
                "stats_can.sc.get_full_table_download",
                return_value="https://example.com/fake.zip",
            ),
            patch.object(scwds._session, "get", return_value=mock_download_resp),
        ):
            with pytest.raises(requests.exceptions.HTTPError):
                stats_can.sc.download_tables("99999999", path=tmpdir)


class TestCubeMetadataValidation:
    """Tests for CubeMetadata schema validation."""

    _CUBE_METADATA_BASE = {
        "responseStatusCode": 0,
        "productId": 34100292,
        "cubeTitleEn": "Building permits",
        "cubeTitleFr": "Permis de bâtir",
        "cubeStartDate": "2018-01-01",
        "cubeEndDate": "2026-02-01",
        "frequencyCode": 6,
        "nbSeriesCube": 375864,
        "nbDatapointsCube": 36834672,
        "releaseTime": "2026-04-13T08:30",
        "archiveStatusCode": "2",
        "archiveStatusEn": "CURRENT",
        "archiveStatusFr": "ACTIF",
        "subjectCode": ["3409"],
        "surveyCode": ["2802"],
        "dimension": [],
        "footnote": [],
        "correction": [],
        "correctionFootnote": [],
        "issueDate": "2026-04-13",
    }

    def test_null_cansim_id_validates(self):
        """Tables without a CANSIM ID return cansimId=null from the API."""
        mock_resp = _mock_response(
            json_data=[
                {
                    "status": "SUCCESS",
                    "object": {**self._CUBE_METADATA_BASE, "cansimId": None},
                }
            ]
        )
        with patch.object(scwds._session, "request", return_value=mock_resp):
            result = scwds.get_cube_metadata("34100292")
        assert result[0]["cansimId"] is None

    def test_string_cansim_id_validates(self):
        """Tables with a CANSIM ID should still validate normally."""
        mock_resp = _mock_response(
            json_data=[
                {
                    "status": "SUCCESS",
                    "object": {**self._CUBE_METADATA_BASE, "cansimId": "271-0022"},
                }
            ]
        )
        with patch.object(scwds._session, "request", return_value=mock_resp):
            result = scwds.get_cube_metadata("27100022")
        assert result[0]["cansimId"] == "271-0022"

    def test_null_issue_date_validates(self):
        """Tables like 36100402 return issueDate=null from the API."""
        mock_resp = _mock_response(
            json_data=[
                {
                    "status": "SUCCESS",
                    "object": {
                        **self._CUBE_METADATA_BASE,
                        "cansimId": None,
                        "issueDate": None,
                    },
                }
            ]
        )
        with patch.object(scwds._session, "request", return_value=mock_resp):
            result = scwds.get_cube_metadata("36100402")
        assert result[0]["issueDate"] is None

    def test_null_cube_start_date_validates(self):
        """cubeStartDate can be null for tables with no data yet."""
        mock_resp = _mock_response(
            json_data=[
                {
                    "status": "SUCCESS",
                    "object": {
                        **self._CUBE_METADATA_BASE,
                        "cansimId": None,
                        "cubeStartDate": None,
                    },
                }
            ]
        )
        with patch.object(scwds._session, "request", return_value=mock_resp):
            result = scwds.get_cube_metadata("34100292")
        assert result[0]["cubeStartDate"] is None

    def test_null_cube_end_date_validates(self):
        """cubeEndDate can be null for tables with no data yet."""
        mock_resp = _mock_response(
            json_data=[
                {
                    "status": "SUCCESS",
                    "object": {
                        **self._CUBE_METADATA_BASE,
                        "cansimId": None,
                        "cubeEndDate": None,
                    },
                }
            ]
        )
        with patch.object(scwds._session, "request", return_value=mock_resp):
            result = scwds.get_cube_metadata("34100292")
        assert result[0]["cubeEndDate"] is None

    def test_null_release_time_validates(self):
        """releaseTime can be null for unreleased tables."""
        mock_resp = _mock_response(
            json_data=[
                {
                    "status": "SUCCESS",
                    "object": {
                        **self._CUBE_METADATA_BASE,
                        "cansimId": None,
                        "releaseTime": None,
                    },
                }
            ]
        )
        with patch.object(scwds._session, "request", return_value=mock_resp):
            result = scwds.get_cube_metadata("34100292")
        assert result[0]["releaseTime"] is None

    def test_multiple_null_fields_validate(self):
        """Multiple nullable fields can be null simultaneously."""
        metadata = {
            **self._CUBE_METADATA_BASE,
            "cansimId": None,
            "cubeStartDate": None,
            "cubeEndDate": None,
            "releaseTime": None,
            "issueDate": None,
        }
        mock_resp = _mock_response(
            json_data=[{"status": "SUCCESS", "object": metadata}]
        )
        with patch.object(scwds._session, "request", return_value=mock_resp):
            result = scwds.get_cube_metadata("36100402")
        assert result[0]["cansimId"] is None
        assert result[0]["cubeStartDate"] is None
        assert result[0]["cubeEndDate"] is None
        assert result[0]["releaseTime"] is None
        assert result[0]["issueDate"] is None


class TestVectorsToDfEdgeCases:
    """Tests for vectors_to_df edge cases."""

    def test_empty_vector_data_returns_empty_df(self):
        """Vectors with no data points should produce an empty DataFrame."""
        mock_data = [
            {
                "responseStatusCode": 0,
                "productId": 123,
                "coordinate": "1.1",
                "vectorId": 74804,
                "vectorDataPoint": [],
            }
        ]
        with patch(
            "stats_can.sc.get_data_from_vectors_and_latest_n_periods",
            return_value=mock_data,
        ):
            df = stats_can.sc.vectors_to_df("v74804", periods=5)
            assert len(df) == 0


class TestGetSeriesInfoFromCubePidCoord:
    """Tests for get_series_info_from_cube_pid_coord."""

    _SERIES_INFO = {
        "responseStatusCode": 0,
        "productId": 25100015,
        "coordinate": "1.12.0.0.0.0.0.0.0.0",
        "vectorId": 1234567,
        "frequencyCode": 6,
        "scalarFactorCode": 0,
        "decimals": 2,
        "terminated": 0,
        "SeriesTitleEn": "Series",
        "SeriesTitleFr": "Serie",
        "memberUomCode": 0,
    }

    def test_single_pair_accepted(self):
        """A single (productId, coordinate) tuple should be wrapped into a list."""
        mock_resp = _mock_response(
            json_data=[{"status": "SUCCESS", "object": self._SERIES_INFO}]
        )
        with patch.object(scwds._session, "request", return_value=mock_resp) as mocked:
            result = scwds.get_series_info_from_cube_pid_coord(
                ("25-10-0015-01", "1.12")
            )
        assert len(result) == 1
        # Verify the body StatsCan received was parsed and padded.
        sent_json = mocked.call_args.kwargs["json"]
        assert sent_json == [
            {"productId": "25100015", "coordinate": "1.12.0.0.0.0.0.0.0.0"}
        ]

    def test_list_of_pairs_accepted(self):
        """A list of pairs should produce one body entry per pair."""
        mock_resp = _mock_response(
            json_data=[
                {"status": "SUCCESS", "object": self._SERIES_INFO},
                {"status": "SUCCESS", "object": self._SERIES_INFO},
            ]
        )
        with patch.object(scwds._session, "request", return_value=mock_resp) as mocked:
            result = scwds.get_series_info_from_cube_pid_coord(
                [("25100015", "1.12"), (25100015, "2")]
            )
        assert len(result) == 2
        sent_json = mocked.call_args.kwargs["json"]
        assert sent_json == [
            {"productId": "25100015", "coordinate": "1.12.0.0.0.0.0.0.0.0"},
            {"productId": "25100015", "coordinate": "2.0.0.0.0.0.0.0.0.0"},
        ]

    def test_chunks_large_input(self):
        """Inputs over 250 pairs should be split across multiple POSTs."""
        pairs = [("25100015", "1")] * 251
        mock_resp = _mock_response(
            json_data=[{"status": "SUCCESS", "object": self._SERIES_INFO}]
        )
        with (
            patch.object(scwds._session, "request", return_value=mock_resp) as mocked,
            patch("stats_can.scwds.time.sleep"),
        ):
            scwds.get_series_info_from_cube_pid_coord(pairs)
        assert mocked.call_count == 2


_VECTOR_DATA = {
    "responseStatusCode": 0,
    "productId": 35100003,
    "coordinate": "1.12.0.0.0.0.0.0.0.0",
    "vectorId": 32164132,
    "vectorDataPoint": [],
}


class TestGetChangedSeriesDataFromCubePidCoord:
    """Tests for get_changed_series_data_from_cube_pid_coord."""

    def test_single_pair_accepted(self):
        """A single (productId, coordinate) tuple should be wrapped into a list."""
        mock_resp = _mock_response(
            json_data=[{"status": "SUCCESS", "object": _VECTOR_DATA}]
        )
        with patch.object(scwds._session, "request", return_value=mock_resp) as mocked:
            result = scwds.get_changed_series_data_from_cube_pid_coord(
                ("35-10-0003-01", "1.12")
            )
        assert len(result) == 1
        assert mocked.call_args.args[0] == "POST"
        assert mocked.call_args.args[1].endswith("getChangedSeriesDataFromCubePidCoord")
        assert mocked.call_args.kwargs["json"] == [
            {"productId": "35100003", "coordinate": "1.12.0.0.0.0.0.0.0.0"}
        ]

    def test_list_of_pairs_accepted(self):
        """A list of pairs should produce one body entry per pair."""
        mock_resp = _mock_response(
            json_data=[
                {"status": "SUCCESS", "object": _VECTOR_DATA},
                {"status": "SUCCESS", "object": _VECTOR_DATA},
            ]
        )
        with patch.object(scwds._session, "request", return_value=mock_resp) as mocked:
            result = scwds.get_changed_series_data_from_cube_pid_coord(
                [("35100003", "1.12"), (35100003, "2")]
            )
        assert len(result) == 2
        assert mocked.call_args.kwargs["json"] == [
            {"productId": "35100003", "coordinate": "1.12.0.0.0.0.0.0.0.0"},
            {"productId": "35100003", "coordinate": "2.0.0.0.0.0.0.0.0.0"},
        ]

    def test_chunks_large_input(self):
        """Inputs over 250 pairs should be split across multiple POSTs."""
        pairs = [("35100003", "1")] * 251
        mock_resp = _mock_response(
            json_data=[{"status": "SUCCESS", "object": _VECTOR_DATA}]
        )
        with (
            patch.object(scwds._session, "request", return_value=mock_resp) as mocked,
            patch("stats_can.scwds.time.sleep"),
        ):
            scwds.get_changed_series_data_from_cube_pid_coord(pairs)
        assert mocked.call_count == 2


class TestGetChangedSeriesDataFromVector:
    """Tests for get_changed_series_data_from_vector."""

    def test_single_vector(self):
        """A single vector should produce a single-entry body."""
        mock_resp = _mock_response(
            json_data=[{"status": "SUCCESS", "object": _VECTOR_DATA}]
        )
        with patch.object(scwds._session, "request", return_value=mock_resp) as mocked:
            result = scwds.get_changed_series_data_from_vector("v32164132")
        assert len(result) == 1
        assert mocked.call_args.args[1].endswith("getChangedSeriesDataFromVector")
        assert mocked.call_args.kwargs["json"] == [{"vectorId": 32164132}]

    def test_list_of_vectors(self):
        """A list of vectors should produce one body entry per vector."""
        mock_resp = _mock_response(
            json_data=[
                {"status": "SUCCESS", "object": _VECTOR_DATA},
                {"status": "SUCCESS", "object": _VECTOR_DATA},
            ]
        )
        with patch.object(scwds._session, "request", return_value=mock_resp) as mocked:
            result = scwds.get_changed_series_data_from_vector(["v32164132", "74804"])
        assert len(result) == 2
        assert mocked.call_args.kwargs["json"] == [
            {"vectorId": 32164132},
            {"vectorId": 74804},
        ]

    def test_chunks_large_input(self):
        """Inputs over 250 vectors should be split across multiple POSTs."""
        vectors = ["v32164132"] * 251
        mock_resp = _mock_response(
            json_data=[{"status": "SUCCESS", "object": _VECTOR_DATA}]
        )
        with (
            patch.object(scwds._session, "request", return_value=mock_resp) as mocked,
            patch("stats_can.scwds.time.sleep"),
        ):
            scwds.get_changed_series_data_from_vector(vectors)
        assert mocked.call_count == 2


class TestGetDataFromCubePidCoordAndLatestNPeriods:
    """Tests for get_data_from_cube_pid_coord_and_latest_n_periods."""

    def test_single_pair_includes_latest_n(self):
        """The latestN value should be propagated into every body entry."""
        mock_resp = _mock_response(
            json_data=[{"status": "SUCCESS", "object": _VECTOR_DATA}]
        )
        with patch.object(scwds._session, "request", return_value=mock_resp) as mocked:
            result = scwds.get_data_from_cube_pid_coord_and_latest_n_periods(
                ("35-10-0003-01", "1.12"), periods=3
            )
        assert len(result) == 1
        assert mocked.call_args.args[1].endswith(
            "getDataFromCubePidCoordAndLatestNPeriods"
        )
        assert mocked.call_args.kwargs["json"] == [
            {
                "productId": "35100003",
                "coordinate": "1.12.0.0.0.0.0.0.0.0",
                "latestN": 3,
            }
        ]

    def test_list_of_pairs_shares_latest_n(self):
        """A shared latestN should appear on every body entry."""
        mock_resp = _mock_response(
            json_data=[
                {"status": "SUCCESS", "object": _VECTOR_DATA},
                {"status": "SUCCESS", "object": _VECTOR_DATA},
            ]
        )
        with patch.object(scwds._session, "request", return_value=mock_resp) as mocked:
            result = scwds.get_data_from_cube_pid_coord_and_latest_n_periods(
                [("35100003", "1.12"), (35100003, "2")], periods=5
            )
        assert len(result) == 2
        assert mocked.call_args.kwargs["json"] == [
            {
                "productId": "35100003",
                "coordinate": "1.12.0.0.0.0.0.0.0.0",
                "latestN": 5,
            },
            {
                "productId": "35100003",
                "coordinate": "2.0.0.0.0.0.0.0.0.0",
                "latestN": 5,
            },
        ]

    def test_chunks_large_input(self):
        """Inputs over 250 pairs should be split across multiple POSTs."""
        pairs = [("35100003", "1")] * 251
        mock_resp = _mock_response(
            json_data=[{"status": "SUCCESS", "object": _VECTOR_DATA}]
        )
        with (
            patch.object(scwds._session, "request", return_value=mock_resp) as mocked,
            patch("stats_can.scwds.time.sleep"),
        ):
            scwds.get_data_from_cube_pid_coord_and_latest_n_periods(pairs, periods=3)
        assert mocked.call_count == 2
