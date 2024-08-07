"""
MIT License

Copyright (c) 2016 Dhamu

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import logging
from typing import Tuple
from urllib.request import Request, build_opener
from urllib.parse import urlencode

from urllib.error import HTTPError
import json as _json

from gravitino.auth.auth_constants import AuthConstants
from gravitino.auth.auth_data_provider import AuthDataProvider
from gravitino.typing import JSONType

from gravitino.constants.timeout import TIMEOUT

from gravitino.dto.responses.error_response import ErrorResponse
from gravitino.dto.responses.oauth2_error_response import OAuth2ErrorResponse
from gravitino.exceptions.base import RESTException, UnknownError
from gravitino.exceptions.handlers.error_handler import ErrorHandler

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
        return None


class HTTPClient:

    FORMDATA_HEADER = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/vnd.gravitino.v1+json",
    }

    JSON_HEADER = {
        "Content-Type": "application/json",
        "Accept": "application/vnd.gravitino.v1+json",
    }

    def __init__(
        self,
        host,
        *,
        request_headers=None,
        timeout=TIMEOUT,
        is_debug=False,
        auth_data_provider: AuthDataProvider = None,
    ) -> None:
        self.host = host
        self.request_headers = request_headers or {}
        self.timeout = timeout
        self.is_debug = is_debug
        self.auth_data_provider = auth_data_provider

    def _build_url(self, endpoint=None, params=None):
        url = self.host

        if endpoint:
            url = f"{url.rstrip('/')}/{endpoint.lstrip('/')}"

        if params:
            params = {k: v for k, v in params.items() if v is not None}
            url_values = urlencode(sorted(params.items()), True)
            url = f"{url}?{url_values}"

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

    def _make_request(self, opener, request, timeout=None) -> Tuple[bool, Response]:
        timeout = timeout or self.timeout
        try:
            return (True, Response(opener.open(request, timeout=timeout)))
        except HTTPError as err:
            err_body = err.read()

            if err_body is None:
                return (
                    False,
                    ErrorResponse.generate_error_response(RESTException, err.reason),
                )

            err_resp = self._parse_error_response(err_body)
            err_resp.validate()

            return (False, err_resp)

    def _parse_error_response(self, err_body: bytes) -> ErrorResponse:
        json_err_body = _json.loads(err_body)

        if "code" in json_err_body:
            return ErrorResponse.from_json(err_body, infer_missing=True)

        return OAuth2ErrorResponse.from_json(err_body, infer_missing=True)

    # pylint: disable=too-many-locals
    def _request(
        self,
        method,
        endpoint,
        params=None,
        json=None,
        data=None,
        headers=None,
        timeout=None,
        error_handler: ErrorHandler = None,
    ):
        method = method.upper()
        request_data = None

        if data:
            request_data = urlencode(data.to_dict()).encode()
            self._update_headers(self.FORMDATA_HEADER)
        else:
            if json:
                request_data = json.to_json().encode("utf-8")

            self._update_headers(self.JSON_HEADER)

        if headers:
            self._update_headers(headers)

        opener = build_opener()
        request = Request(self._build_url(endpoint, params), data=request_data)
        if self.request_headers:
            for key, value in self.request_headers.items():
                request.add_header(key, value)
        if request_data and ("Content-Type" not in self.request_headers):
            request.add_header("Content-Type", "application/json")
        if self.auth_data_provider is not None:
            request.add_header(
                AuthConstants.HTTP_HEADER_AUTHORIZATION,
                self.auth_data_provider.get_token_data().decode("utf-8"),
            )
        request.get_method = lambda: method
        is_success, resp = self._make_request(opener, request, timeout=timeout)

        if is_success:
            return resp

        if not isinstance(error_handler, ErrorHandler):
            raise UnknownError(
                f"Unknown error handler {type(error_handler).__name__}, error response body: {resp}"
            ) from None

        error_handler.handle(resp)

        # This code generally will not be run because the error handler should define the default behavior,
        # and raise appropriate
        raise UnknownError(
            f"Error handler {type(error_handler).__name__} can't handle this response, error response body: {resp}"
        ) from None

    def get(self, endpoint, params=None, error_handler=None, **kwargs):
        return self._request(
            "get", endpoint, params=params, error_handler=error_handler, **kwargs
        )

    def delete(self, endpoint, error_handler=None, **kwargs):
        return self._request("delete", endpoint, error_handler=error_handler, **kwargs)

    def post(self, endpoint, json=None, error_handler=None, **kwargs):
        return self._request(
            "post", endpoint, json=json, error_handler=error_handler, **kwargs
        )

    def put(self, endpoint, json=None, error_handler=None, **kwargs):
        return self._request(
            "put", endpoint, json=json, error_handler=error_handler, **kwargs
        )

    def post_form(self, endpoint, data=None, error_handler=None, **kwargs):
        return self._request(
            "post", endpoint, data=data, error_handler=error_handler, **kwargs
        )

    def close(self):
        self._request("close", "/")
        if self.auth_data_provider is not None:
            self.auth_data_provider.close()


def unpack(path: str):
    def decorator(func):
        def wrapper(*args, **kwargs) -> JSONType:
            resp = func(*args, **kwargs)
            rv = resp.json()
            for p in path.split("."):
                if p not in rv:
                    raise KeyError(f"The path '{path}' can't find in dict")
                rv = rv.get(p)
            return rv

        return wrapper

    return decorator
