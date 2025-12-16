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

from gravitino.exceptions.handlers.job_error_handler import (
    JOB_ERROR_HANDLER,
)
from gravitino.exceptions.handlers.rest_error_handler import (
    RestErrorHandler,
)
from gravitino.exceptions.handlers.tag_error_handler import (
    TAG_ERROR_HANDLER,
)


class ErrorHandlers:
    """Helper class to create error handlers specific to different operations."""

    def __init__(self) -> None:
        raise NotImplementedError("This class is not meant to be instantiated")

    @staticmethod
    def tag_error_handler() -> RestErrorHandler:
        """
        Creates an error handler specific to Tag operations.

        Returns:
            RestErrorHandler: A rest error handler specific to Tag operations.
        """
        return TAG_ERROR_HANDLER

    @staticmethod
    def job_error_handler() -> RestErrorHandler:
        """
        Creates an error handler specific to Job operations.

        Returns:
            JobErrorHandler: A job error handler specific to Job operations.
        """
        return JOB_ERROR_HANDLER
