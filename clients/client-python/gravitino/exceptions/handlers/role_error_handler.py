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
    ForbiddenException,
    IllegalArgumentException,
    IllegalMetadataObjectException,
    IllegalPrivilegeException,
    MetalakeNotInUseException,
    NoSuchMetadataObjectException,
    NoSuchMetalakeException,
    NoSuchRoleException,
    NotFoundException,
    RoleAlreadyExistsException,
    UnsupportedOperationException,
)
from gravitino.exceptions.handlers.rest_error_handler import RestErrorHandler


class RoleErrorHandler(RestErrorHandler):
    """Error handler specific to Role operations."""

    def handle(self, error_response: ErrorResponse):
        error_message = error_response.format_error_message()
        code = error_response.code()
        exception_type = error_response.type()

        if code == ErrorConstants.ILLEGAL_ARGUMENTS_CODE:
            if exception_type == IllegalPrivilegeException.__name__:
                raise IllegalPrivilegeException(error_message)
            if exception_type == IllegalMetadataObjectException.__name__:
                raise IllegalMetadataObjectException(error_message)

            raise IllegalArgumentException(error_message)

        if code == ErrorConstants.NOT_FOUND_CODE:
            if exception_type == NoSuchMetalakeException.__name__:
                raise NoSuchMetalakeException(error_message)

            if exception_type == NoSuchRoleException.__name__:
                raise NoSuchRoleException(error_message)

            if exception_type == NoSuchMetadataObjectException.__name__:
                raise NoSuchMetadataObjectException(error_message)

            raise NotFoundException(error_message)

        if code == ErrorConstants.ALREADY_EXISTS_CODE:
            raise RoleAlreadyExistsException(error_message)

        if code == ErrorConstants.UNSUPPORTED_OPERATION_CODE:
            raise UnsupportedOperationException(error_message)

        if code == ErrorConstants.FORBIDDEN_CODE:
            raise ForbiddenException(error_message)

        if code == ErrorConstants.NOT_IN_USE_CODE:
            raise MetalakeNotInUseException(error_message)

        if code == ErrorConstants.INTERNAL_ERROR_CODE:
            raise RuntimeError(error_message)

        super().handle(error_response)


ROLE_ERROR_HANDLER = RoleErrorHandler()
