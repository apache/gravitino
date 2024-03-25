"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

from requests import request
from requests.exceptions import HTTPError

from gravitino.constants import TIMEOUT


class HTTPClient:
    def __init__(self, timeout=TIMEOUT):
        pass

    def _request(
        self, method, endpoint, params=None, json=None, headers=None, timeout=None
    ):
        try:
            response = request(method, endpoint)
            response.raise_for_status()
            return response.json()
        except HTTPError as e:
            raise HTTPError(f"Failed to retrieve version information: {e}")

    def get(self, endpoint, params=None, **kwargs):
        return self._request("get", endpoint, params=params, **kwargs)

    def delete(self, endpoint, **kwargs):
        return self._request("delete", endpoint, **kwargs)

    def post(self, endpoint, json=None, **kwargs):
        return self._request("post", endpoint, json=json, **kwargs)

    def put(self, endpoint, json=None, **kwargs):
        return self._request("put", endpoint, json=json, **kwargs)
