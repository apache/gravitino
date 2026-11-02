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
    CatalogNotInUseException,
    NoSuchCatalogException,
    NoSuchSchemaException,
    NonEmptySchemaException,
    SchemaAlreadyExistsException,
)
from gravitino.exceptions.handlers.rest_error_handler import CodeMappingErrorHandler


class SchemaErrorHandler(CodeMappingErrorHandler):
    _code_exception_map = {
        ErrorConstants.NOT_FOUND_CODE: {
            NoSuchCatalogException.__name__: NoSuchCatalogException,
            NoSuchSchemaException.__name__: NoSuchSchemaException,
        },
        ErrorConstants.ALREADY_EXISTS_CODE: SchemaAlreadyExistsException,
        ErrorConstants.NON_EMPTY_CODE: NonEmptySchemaException,
        ErrorConstants.NOT_IN_USE_CODE: CatalogNotInUseException,
    }


SCHEMA_ERROR_HANDLER = SchemaErrorHandler()
