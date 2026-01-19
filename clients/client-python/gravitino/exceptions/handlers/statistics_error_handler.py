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

"""Error handler for statistics-related REST API errors."""

from gravitino.constants.error import ErrorConstants
from gravitino.dto.responses.error_response import ErrorResponse
from gravitino.exceptions.base import NotFoundException
from gravitino.exceptions.handlers.rest_error_handler import RestErrorHandler
from gravitino.exceptions.statistics_exceptions import (
    IllegalStatisticNameException,
    UnmodifiableStatisticException,
)


class StatisticsErrorHandler(RestErrorHandler):
    """Error handler for statistics operations."""

    def handle(self, error_response: ErrorResponse):
        """Handle error response for statistics operations."""
        error_code = error_response.code()

        if error_code == ErrorConstants.ILLEGAL_ARGUMENTS_CODE:
            if "statistic name" in error_response.message().lower():
                raise IllegalStatisticNameException(
                    self._format_error_message(error_response)
                )
            if "unmodifiable" in error_response.message().lower():
                raise UnmodifiableStatisticException(
                    self._format_error_message(error_response)
                )

        if error_code == ErrorConstants.NOT_FOUND_CODE:
            raise NotFoundException(self._format_error_message(error_response))

        super().handle(error_response)


STATISTICS_ERROR_HANDLER = StatisticsErrorHandler()
