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
    AlreadyExistsException,
    CatalogNotInUseException,
    ForbiddenException,
    IllegalArgumentException,
    IllegalSemanticModelException,
    MetalakeNotInUseException,
    NoSuchCatalogException,
    NoSuchMetalakeException,
    NoSuchSchemaException,
    NoSuchSemanticModelException,
    NotFoundException,
    NotInUseException,
    SemanticModelAlreadyExistsException,
    UnsupportedOperationException,
)
from gravitino.exceptions.handlers.rest_error_handler import RestErrorHandler


class SemanticModelErrorHandler(RestErrorHandler):
    """Error handler for Semantic Model operations."""

    def handle(self, error_response: ErrorResponse):
        error_message = error_response.format_error_message()
        code = error_response.code()
        exception_type = error_response.type()

        if code == ErrorConstants.ILLEGAL_ARGUMENTS_CODE:
            if exception_type == IllegalSemanticModelException.__name__:
                raise IllegalSemanticModelException(error_message)

            raise IllegalArgumentException(error_message)

        if code == ErrorConstants.NOT_FOUND_CODE:
            self._raise_not_found(exception_type, error_message)

        if code == ErrorConstants.ALREADY_EXISTS_CODE:
            if exception_type == SemanticModelAlreadyExistsException.__name__:
                raise SemanticModelAlreadyExistsException(error_message)

            raise AlreadyExistsException(error_message)

        if code == ErrorConstants.UNSUPPORTED_OPERATION_CODE:
            raise UnsupportedOperationException(error_message)

        if code == ErrorConstants.FORBIDDEN_CODE:
            raise ForbiddenException(error_message)

        if code == ErrorConstants.NOT_IN_USE_CODE:
            self._raise_not_in_use(exception_type, error_message)

        super().handle(error_response)

    @staticmethod
    def _raise_not_found(exception_type: str, error_message: str):
        if exception_type == NoSuchMetalakeException.__name__:
            raise NoSuchMetalakeException(error_message)
        if exception_type == NoSuchCatalogException.__name__:
            raise NoSuchCatalogException(error_message)
        if exception_type == NoSuchSchemaException.__name__:
            raise NoSuchSchemaException(error_message)
        if exception_type == NoSuchSemanticModelException.__name__:
            raise NoSuchSemanticModelException(error_message)

        raise NotFoundException(error_message)

    @staticmethod
    def _raise_not_in_use(exception_type: str, error_message: str):
        if exception_type == CatalogNotInUseException.__name__:
            raise CatalogNotInUseException(error_message)
        if exception_type == MetalakeNotInUseException.__name__:
            raise MetalakeNotInUseException(error_message)

        raise NotInUseException(error_message)


SEMANTIC_MODEL_ERROR_HANDLER = SemanticModelErrorHandler()
