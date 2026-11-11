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
    IllegalJobTemplateOperationException,
    InUseException,
    JobTemplateAlreadyExistsException,
    MetalakeNotInUseException,
    NoSuchJobException,
    NoSuchJobTemplateException,
    NoSuchMetalakeException,
)
from gravitino.exceptions.handlers.rest_error_handler import CodeMappingErrorHandler


class JobErrorHandler(CodeMappingErrorHandler):
    _code_exception_map = {
        ErrorConstants.ILLEGAL_ARGUMENTS_CODE: {
            IllegalJobTemplateOperationException.__name__: (
                IllegalJobTemplateOperationException
            )
        },
        ErrorConstants.NOT_FOUND_CODE: {
            NoSuchMetalakeException.__name__: NoSuchMetalakeException,
            NoSuchJobTemplateException.__name__: NoSuchJobTemplateException,
            NoSuchJobException.__name__: NoSuchJobException,
        },
        ErrorConstants.ALREADY_EXISTS_CODE: JobTemplateAlreadyExistsException,
        ErrorConstants.IN_USE_CODE: InUseException,
        ErrorConstants.NOT_IN_USE_CODE: {
            MetalakeNotInUseException.__name__: MetalakeNotInUseException
        },
    }


JOB_ERROR_HANDLER = JobErrorHandler()
