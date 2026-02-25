"""Tests for the dedup module."""

from unittest.mock import patch, MagicMock

from data_publishing.hf.dedup import get_existing_ids_for_date


def _mock_response(rows, status_code=200):
    """Create a mock requests.Response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = {"rows": [{"row": {"unique_id": uid}} for uid in rows]}
    resp.text = ""
    return resp


@patch("data_publishing.hf.dedup.requests.get")
def test_get_existing_ids_returns_set(mock_get):
    mock_get.return_value = _mock_response(["id-1", "id-2", "id-3"])
    result = get_existing_ids_for_date("user/dataset", "2025-01-15")
    assert result == {"id-1", "id-2", "id-3"}


@patch("data_publishing.hf.dedup.requests.get")
def test_get_existing_ids_empty_dataset(mock_get):
    mock_get.return_value = _mock_response([])
    result = get_existing_ids_for_date("user/dataset", "2025-01-15")
    assert result == set()


@patch("data_publishing.hf.dedup.requests.get")
def test_get_existing_ids_handles_api_error(mock_get):
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = {"error": "loading"}
    mock_get.return_value = resp
    result = get_existing_ids_for_date("user/dataset", "2025-01-15")
    assert result == set()


@patch("data_publishing.hf.dedup.requests.get")
def test_get_existing_ids_handles_http_error(mock_get):
    mock_get.return_value = _mock_response([], status_code=500)
    result = get_existing_ids_for_date("user/dataset", "2025-01-15")
    assert result == set()


@patch("data_publishing.hf.dedup.requests.get")
def test_get_existing_ids_paginates(mock_get):
    # First call returns 100 results, second call returns 50
    page1 = [f"id-{i}" for i in range(100)]
    page2 = [f"id-{i}" for i in range(100, 150)]
    mock_get.side_effect = [
        _mock_response(page1),
        _mock_response(page2),
    ]
    result = get_existing_ids_for_date("user/dataset", "2025-01-15")
    assert len(result) == 150
