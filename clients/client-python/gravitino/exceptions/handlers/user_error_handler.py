# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


from gravitino.constants.error import ErrorConstants
from gravitino.dto.responses.error_response import ErrorResponse
from gravitino.exceptions.base import (
    IllegalArgumentException,
    MetalakeNotInUseException,
    NoSuchMetalakeException,
    NoSuchRoleException,
    NoSuchUserException,
    NotFoundException,
    UserAlreadyExistsException,
)
from gravitino.exceptions.handlers.rest_error_handler import RestErrorHandler


class UserErrorHandler(RestErrorHandler):
    """Error handler specific to User operations."""

    def handle(self, error_response: ErrorResponse):
        error_message = error_response.format_error_message()
        code = error_response.code()
        exception_type = error_response.type()

        if code == ErrorConstants.ILLEGAL_ARGUMENTS_CODE:
            raise IllegalArgumentException(error_message)

        if code == ErrorConstants.NOT_FOUND_CODE:
            if exception_type == NoSuchMetalakeException.__name__:
                raise NoSuchMetalakeException(error_message)

            if exception_type == NoSuchUserException.__name__:
                raise NoSuchUserException(error_message)

            if exception_type == NoSuchRoleException.__name__:
                raise NoSuchRoleException(error_message)

            raise NotFoundException(error_message)

        if code == ErrorConstants.ALREADY_EXISTS_CODE:
            raise UserAlreadyExistsException(error_message)

        if code == ErrorConstants.NOT_IN_USE_CODE:
            raise MetalakeNotInUseException(error_message)

        if code == ErrorConstants.INTERNAL_ERROR_CODE:
            raise RuntimeError(error_message)

        super().handle(error_response)


USER_ERROR_HANDLER = UserErrorHandler()
