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
