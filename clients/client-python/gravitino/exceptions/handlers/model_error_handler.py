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
    NoSuchSchemaException,
    NoSuchModelException,
    NoSuchModelVersionException,
    NoSuchModelVersionURINameException,
    NotFoundException,
    ModelAlreadyExistsException,
    ModelVersionAliasesAlreadyExistException,
    AlreadyExistsException,
    CatalogNotInUseException,
    MetalakeNotInUseException,
    NotInUseException,
)
from gravitino.exceptions.handlers.rest_error_handler import RestErrorHandler


class ModelErrorHandler(RestErrorHandler):
    def handle(self, error_response: ErrorResponse):
        error_message = error_response.format_error_message()
        code = error_response.code()
        exception_type = error_response.type()

        if code == ErrorConstants.NOT_FOUND_CODE:
            if exception_type == NoSuchSchemaException.__name__:
                raise NoSuchSchemaException(error_message)
            if exception_type == NoSuchModelException.__name__:
                raise NoSuchModelException(error_message)
            if exception_type == NoSuchModelVersionException.__name__:
                raise NoSuchModelVersionException(error_message)
            if exception_type == NoSuchModelVersionURINameException.__name__:
                raise NoSuchModelVersionURINameException(error_message)

            raise NotFoundException(error_message)

        if code == ErrorConstants.ALREADY_EXISTS_CODE:
            if exception_type == ModelAlreadyExistsException.__name__:
                raise ModelAlreadyExistsException(error_message)
            if exception_type == ModelVersionAliasesAlreadyExistException.__name__:
                raise ModelVersionAliasesAlreadyExistException(error_message)

            raise AlreadyExistsException(error_message)

        if code == ErrorConstants.NOT_IN_USE_CODE:
            if exception_type == CatalogNotInUseException.__name__:
                raise CatalogNotInUseException(error_message)
            if exception_type == MetalakeNotInUseException.__name__:
                raise MetalakeNotInUseException(error_message)

            raise NotInUseException(error_message)

        super().handle(error_response)


MODEL_ERROR_HANDLER = ModelErrorHandler()
