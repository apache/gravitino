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

from gravitino.constants.error import ERROR_CODE_MAPPING
from gravitino.dto.responses.error_response import ErrorResponse
from gravitino.exceptions.handlers.error_handler import ErrorHandler
from gravitino.exceptions.base import RESTException


class RestErrorHandler(ErrorHandler):
    def handle(self, error_response: ErrorResponse):
        error_message = error_response.format_error_message()
        code = error_response.code()

        if code in ERROR_CODE_MAPPING:
            raise ERROR_CODE_MAPPING[code](error_message)

        raise RESTException(
            f"Unable to process: {error_message}",
        )


class CodeMappingErrorHandler(RestErrorHandler):
    """Base error handler that maps error codes to exceptions.

    Subclasses declare a class-level ``_code_exception_map`` that maps an error
    code to either an exception class or a dict of exception-type names to
    exception classes for codes that need to disambiguate by the response
    ``type`` field. Codes not present in the map are delegated to
    ``RestErrorHandler.handle``.
    """

    _code_exception_map = {}

    def handle(self, error_response: ErrorResponse):
        error_message = error_response.format_error_message()
        code = error_response.code()
        exception_type = error_response.type()

        mapping = self._code_exception_map.get(code)
        if mapping is not None:
            if isinstance(mapping, dict):
                exception_class = mapping.get(exception_type)
                if exception_class is not None:
                    raise exception_class(error_message)
            else:
                raise mapping(error_message)

        super().handle(error_response)


REST_ERROR_HANDLER = RestErrorHandler()
