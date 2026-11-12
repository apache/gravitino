# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from gravitino.constants.error import ErrorConstants
from gravitino.exceptions.base import (
    ForbiddenException,
    IllegalMetadataObjectException,
    IllegalPrivilegeException,
    MetalakeNotInUseException,
    NoSuchMetadataObjectException,
    NoSuchMetalakeException,
    NoSuchRoleException,
    RoleAlreadyExistsException,
    UnsupportedOperationException,
)
from gravitino.exceptions.handlers.rest_error_handler import CodeMappingErrorHandler


class RoleErrorHandler(CodeMappingErrorHandler):
    """Error handler specific to Role operations."""

    _code_exception_map = {
        ErrorConstants.ILLEGAL_ARGUMENTS_CODE: {
            IllegalPrivilegeException.__name__: IllegalPrivilegeException,
            IllegalMetadataObjectException.__name__: IllegalMetadataObjectException,
        },
        ErrorConstants.NOT_FOUND_CODE: {
            NoSuchMetalakeException.__name__: NoSuchMetalakeException,
            NoSuchRoleException.__name__: NoSuchRoleException,
            NoSuchMetadataObjectException.__name__: NoSuchMetadataObjectException,
        },
        ErrorConstants.ALREADY_EXISTS_CODE: RoleAlreadyExistsException,
        ErrorConstants.UNSUPPORTED_OPERATION_CODE: UnsupportedOperationException,
        ErrorConstants.FORBIDDEN_CODE: ForbiddenException,
        ErrorConstants.NOT_IN_USE_CODE: MetalakeNotInUseException,
        ErrorConstants.INTERNAL_ERROR_CODE: RuntimeError,
    }


ROLE_ERROR_HANDLER = RoleErrorHandler()
