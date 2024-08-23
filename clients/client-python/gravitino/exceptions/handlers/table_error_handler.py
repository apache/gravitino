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

from gravitino.exceptions.handlers.error_handler import ErrorHandler
from gravitino.dto.responses.error_response import ErrorResponse
from gravitino.constants.error import ErrorConstants
from gravitino.exceptions.base import (
    IllegalArugmentException,
    NoSuchSchemaException,
    NoSuchTableException,
    NotFoundException,
    TableAlreadyExistsException,
    UnsupportedOperationException
)

class TableErrorHandler(ErrorHandler):
    """Error handler specific to Table operations."""

    INSTANCE = None

    def __init__(self):
        if not TableErrorHandler.INSTANCE:
            TableErrorHandler.INSTANCE = self

    def handle(self, error_response: ErrorResponse):
        error_message = error_response.format_error_message()

        if error_response.code() == ErrorConstants.ILLEGAL_ARGUMENTS_CODE:
            raise IllegalArugmentException(error_message)
        elif error_response.code() == ErrorConstants.NOT_FOUND_CODE:
            if error_response.type() == "NoSuchSchemaException":
                raise NoSuchSchemaException(error_message)
            elif error_response.type() == "NoSuchTableException":
                raise NoSuchTableException(error_message)
            else:
                raise NotFoundException(error_message)
        elif error_response.code() == ErrorConstants.ALREADY_EXISTS_CODE:
            raise TableAlreadyExistsException(error_message)
        elif error_response.code() == ErrorConstants.INTERNAL_ERROR_CODE:
            raise RuntimeError(error_message)
        elif error_response.code() == ErrorConstants.UNSUPPORTED_OPERATION_CODE:
            raise UnsupportedOperationException(error_message)
        else:
            super().handle(error_response)


TABLE_ERROR_HANDLER = TableErrorHandler()
