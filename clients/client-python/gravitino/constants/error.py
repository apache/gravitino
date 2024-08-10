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

from enum import IntEnum

from gravitino.exceptions.base import (
    ConnectionFailedException,
    RESTException,
    IllegalArgumentException,
    NotFoundException,
    InternalError,
    AlreadyExistsException,
    NotEmptyException,
    UnsupportedOperationException,
)


class ErrorConstants(IntEnum):
    """Constants representing error codes for responses."""

    # Error codes for REST responses error.
    REST_ERROR_CODE = 1000

    # Error codes for illegal arguments.
    ILLEGAL_ARGUMENTS_CODE = 1001

    # Error codes for internal errors.
    INTERNAL_ERROR_CODE = 1002

    # Error codes for not found.
    NOT_FOUND_CODE = 1003

    # Error codes for already exists.
    ALREADY_EXISTS_CODE = 1004

    # Error codes for non empty.
    NON_EMPTY_CODE = 1005

    # Error codes for unsupported operation.
    UNSUPPORTED_OPERATION_CODE = 1006

    # Error codes for connect to catalog failed.
    CONNECTION_FAILED_CODE = 1007

    # Error codes for invalid state.
    UNKNOWN_ERROR_CODE = 1100


EXCEPTION_MAPPING = {
    RESTException: ErrorConstants.REST_ERROR_CODE,
    IllegalArgumentException: ErrorConstants.ILLEGAL_ARGUMENTS_CODE,
    InternalError: ErrorConstants.INTERNAL_ERROR_CODE,
    NotFoundException: ErrorConstants.NOT_FOUND_CODE,
    AlreadyExistsException: ErrorConstants.ALREADY_EXISTS_CODE,
    NotEmptyException: ErrorConstants.NON_EMPTY_CODE,
    UnsupportedOperationException: ErrorConstants.UNSUPPORTED_OPERATION_CODE,
    ConnectionFailedException: ErrorConstants.CONNECTION_FAILED_CODE,
}

ERROR_CODE_MAPPING = {v: k for k, v in EXCEPTION_MAPPING.items()}
