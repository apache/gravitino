"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""
import pytest
import requests
from unittest.mock import MagicMock
from gravitino_client.core import GravitinoClient


@pytest.fixture
def mock_get(monkeypatch):
    mock = MagicMock()
    monkeypatch.setattr("requests.get", mock)
    return mock


def test_get_version_success(mock_get):
    mock_get.return_value.json.return_value = {
        "code": 0,
        "version": {
            "version": "0.3.2-SNAPSHOT",
            "compileDate": "25/01/2024 00:04:59",
            "gitCommit": "cb7a604bf19b6f992f00529e938cdd1d37af0187"
        }
    }

    client = GravitinoClient(base_url="http://localhost:8090")
    version_data = client.getVersion()

    assert version_data == {
        "version": "0.3.2-SNAPSHOT",
        "compileDate": "25/01/2024 00:04:59",
        "gitCommit": "cb7a604bf19b6f992f00529e938cdd1d37af0187"
    }


def test_get_version_http_error(mock_get):
    mock_get.side_effect = requests.exceptions.HTTPError

    client = GravitinoClient(base_url="http://localhost:8090")

    with pytest.raises(requests.exceptions.HTTPError):
        client.getVersion()