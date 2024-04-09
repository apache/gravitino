"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""
import logging
from urllib.request import Request, build_opener
from urllib.parse import urlencode
from urllib.error import HTTPError
import json as _json

from gravitino.typing import JSON_ro
from gravitino.utils.exceptions import handle_error
from gravitino.constants import TIMEOUT

logger = logging.getLogger(__name__)

class Response:
    def __init__(self, response):
        self._status_code = response.getcode()
        self._body = response.read()
        self._headers = response.info()
        self._url = response.url

        logging.basicConfig(level=logging.DEBUG)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        logger.addHandler(console_handler)

    @property
    def status_code(self):
        return self._status_code

    @property
    def url(self):
        return self._url

    @property
    def body(self):
        return self._body

    @property
    def headers(self):
        return self._headers

    def json(self):
        if self.body:
            return _json.loads(self.body.decode("utf-8"))
        else:
            return None


class HTTPClient:
    def __init__(
        self,
        host,
        *,
        request_headers=None,
        timeout=TIMEOUT,
        is_debug=False,
    ) -> None:
        self.host = host
        self.request_headers = request_headers or {}
        self.timeout = timeout
        self.is_debug = is_debug

    def _build_url(self, endpoint=None, params=None):
        url = self.host

        if endpoint:
            url = "{}/{}".format(url.rstrip("/"), endpoint.lstrip("/"))

        if params:
            params = {k: v for k, v in params.items() if v is not None}
            url_values = urlencode(sorted(params.items()), True)
            url = "{}?{}".format(url, url_values)

        return url

    def _update_headers(self, request_headers):
        self.request_headers.update(request_headers)

    def _mask_auth_headers(self, headers):
        if self.is_debug:
            return headers

        _headers = {}
        for key, value in headers.items():
            if key.lower() == "authorization":
                _headers[key] = "******"
            else:
                _headers[key] = value
        return _headers

    def _make_request(self, opener, request, timeout=None):
        timeout = timeout or self.timeout
        try:
            return opener.open(request, timeout=timeout)
        except HTTPError as err:
            exc = handle_error(err)
            exc.__cause__ = None
            raise exc

    def _request(
        self, method, endpoint, params=None, json=None, headers=None, timeout=None
    ):
        method = method.upper()
        request_data = None

        if headers:
            self._update_headers(headers)
        else:
            headers = {'Content-Type': 'application/json', 'Accept': 'application/vnd.gravitino.v1+json'}
            self._update_headers(headers)

        if json:
            request_data = json.to_json().encode("utf-8")

        opener = build_opener()
        request = Request(self._build_url(endpoint, params), data=request_data)
        if self.request_headers:
            for key, value in self.request_headers.items():
                request.add_header(key, value)
        if request_data and ("Content-Type" not in self.request_headers):
            request.add_header("Content-Type", "application/json")

        request.get_method = lambda: method
        return Response(self._make_request(opener, request, timeout=timeout))

    def get(self, endpoint, params=None, **kwargs):
        return self._request("get", endpoint, params=params, **kwargs)

    def delete(self, endpoint, **kwargs):
        return self._request("delete", endpoint, **kwargs)

    def post(self, endpoint, json=None, **kwargs):
        return self._request("post", endpoint, json=json, **kwargs)

    def put(self, endpoint, json=None, **kwargs):
        return self._request("put", endpoint, json=json, **kwargs)

    def close(self):
        self._request("close")


def unpack(path: str):
    def decorator(func):
        def wrapper(*args, **kwargs) -> JSON_ro:
            resp = func(*args, **kwargs)
            rv = resp.json()
            for p in path.split("."):
                if p not in rv:
                    raise KeyError(f"The path '{path}' can't find in dict")
                rv = rv.get(p)
            return rv

        return wrapper

    return decorator
