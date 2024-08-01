"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""

from gravitino.exceptions.base import UnauthorizedException, BadRequestException
from gravitino.dto.responses.oauth2_error_response import OAuth2ErrorResponse
from gravitino.exceptions.handlers.rest_error_handler import RestErrorHandler

INVALID_CLIENT_ERROR = "invalid_client"
INVALID_REQUEST_ERROR = "invalid_request"
INVALID_GRANT_ERROR = "invalid_grant"
UNAUTHORIZED_CLIENT_ERROR = "unauthorized_client"
UNSUPPORTED_GRANT_TYPE_ERROR = "unsupported_grant_type"
INVALID_SCOPE_ERROR = "invalid_scope"


class OAuthErrorHandler(RestErrorHandler):

    def handle(self, error_response: OAuth2ErrorResponse):

        error_message = error_response.message()
        exception_type = error_response.type()

        if exception_type == INVALID_CLIENT_ERROR:
            raise UnauthorizedException(
                f"Not authorized: {exception_type}: {error_message}"
            )

        if exception_type in [
            INVALID_REQUEST_ERROR,
            INVALID_GRANT_ERROR,
            UNAUTHORIZED_CLIENT_ERROR,
            UNSUPPORTED_GRANT_TYPE_ERROR,
            INVALID_SCOPE_ERROR,
        ]:
            raise BadRequestException(
                f"Malformed request: {exception_type}: {error_message}"
            )

        super().handle(error_response)


OAUTH_ERROR_HANDLER = OAuthErrorHandler()
