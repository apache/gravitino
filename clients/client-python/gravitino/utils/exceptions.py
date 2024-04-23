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

import json


class HTTPError(Exception):
    """Base of all other errors"""

    def __init__(self, error):
        self.status_code = error.code
        self.reason = error.reason
        self.body = error.read()
        self.headers = error.hdrs

    def json(self):
        """
        :Returns: object of response error from the API
        """
        try:
            return json.loads(self.body.decode("utf-8"))
        except json.decoder.JSONDecodeError:
            return {"exception": self.body.decode("utf-8")}

    def __str__(self):
        return self.json().get("exception")


class BadRequestsError(HTTPError):
    pass


class UnauthorizedError(HTTPError):
    pass


class ForbiddenError(HTTPError):
    pass


class NotFoundError(HTTPError):
    pass


class MethodNotAllowedError(HTTPError):
    pass


class PayloadTooLargeError(HTTPError):
    pass


class UnsupportedMediaTypeError(HTTPError):
    pass


class TooManyRequestsError(HTTPError):
    pass


class InternalServerError(HTTPError):
    pass


class ServiceUnavailableError(HTTPError):
    pass


class GatewayTimeoutError(HTTPError):
    pass


err_dict = {
    400: BadRequestsError,
    401: UnauthorizedError,
    403: ForbiddenError,
    404: NotFoundError,
    405: MethodNotAllowedError,
    413: PayloadTooLargeError,
    415: UnsupportedMediaTypeError,
    429: TooManyRequestsError,
    500: InternalServerError,
    503: ServiceUnavailableError,
    504: GatewayTimeoutError,
}


def handle_error(error):
    try:
        exc = err_dict[error.code](error)
    except KeyError:
        return HTTPError(error)
    return exc
