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

import unittest

from gravitino.dto.responses.error_response import ErrorResponse
from gravitino.exceptions.base import (
    AlreadyExistsException,
    CatalogAlreadyExistsException,
    CatalogNotInUseException,
    ConnectionFailedException,
    IllegalArgumentException,
    InternalError,
    MetalakeAlreadyExistsException,
    MetalakeNotInUseException,
    NonEmptySchemaException,
    NoSuchCatalogException,
    NoSuchCredentialException,
    NoSuchFilesetException,
    NoSuchMetalakeException,
    NoSuchPartitionException,
    NoSuchSchemaException,
    NoSuchTableException,
    NotEmptyException,
    NotFoundException,
    NotInUseException,
    PartitionAlreadyExistsException,
    RESTException,
    SchemaAlreadyExistsException,
    UnsupportedOperationException,
)
from gravitino.exceptions.handlers.catalog_error_handler import CATALOG_ERROR_HANDLER
from gravitino.exceptions.handlers.credential_error_handler import (
    CREDENTIAL_ERROR_HANDLER,
)
from gravitino.exceptions.handlers.fileset_error_handler import FILESET_ERROR_HANDLER
from gravitino.exceptions.handlers.metalake_error_handler import METALAKE_ERROR_HANDLER
from gravitino.exceptions.handlers.partition_error_handler import (
    PARTITION_ERROR_HANDLER,
)
from gravitino.exceptions.handlers.rest_error_handler import REST_ERROR_HANDLER
from gravitino.exceptions.handlers.schema_error_handler import SCHEMA_ERROR_HANDLER


class TestErrorHandler(unittest.TestCase):
    def test_rest_error_handler(self):
        with self.assertRaises(RESTException):
            REST_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(RESTException, "mock error")
            )

        with self.assertRaises(IllegalArgumentException):
            REST_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(
                    IllegalArgumentException, "mock error"
                )
            )

        with self.assertRaises(InternalError):
            REST_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(InternalError, "mock error")
            )

        with self.assertRaises(NotFoundException):
            REST_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(NotFoundException, "mock error")
            )

        with self.assertRaises(NotFoundException):
            REST_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(
                    NoSuchFilesetException, "mock error"
                )
            )

        with self.assertRaises(AlreadyExistsException):
            REST_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(
                    AlreadyExistsException, "mock error"
                )
            )

        with self.assertRaises(NotEmptyException):
            REST_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(NotEmptyException, "mock error")
            )

        with self.assertRaises(UnsupportedOperationException):
            REST_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(
                    UnsupportedOperationException, "mock error"
                )
            )

        with self.assertRaises(RESTException):
            REST_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(Exception, "mock error")
            )

    def test_fileset_error_handler(self):
        with self.assertRaises(NoSuchFilesetException):
            FILESET_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(
                    NoSuchFilesetException, "mock error"
                )
            )

        with self.assertRaises(NoSuchSchemaException):
            FILESET_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(
                    NoSuchSchemaException, "mock error"
                )
            )

        with self.assertRaises(InternalError):
            FILESET_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(InternalError, "mock error")
            )

        with self.assertRaises(RESTException):
            FILESET_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(Exception, "mock error")
            )

    def test_credential_error_handler(self):
        with self.assertRaises(NoSuchCredentialException):
            CREDENTIAL_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(
                    NoSuchCredentialException, "mock error"
                )
            )

        with self.assertRaises(InternalError):
            CREDENTIAL_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(InternalError, "mock error")
            )

        with self.assertRaises(RESTException):
            CREDENTIAL_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(Exception, "mock error")
            )

    def test_metalake_error_handler(self):
        with self.assertRaises(NoSuchMetalakeException):
            METALAKE_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(
                    NoSuchMetalakeException, "mock error"
                )
            )

        with self.assertRaises(MetalakeAlreadyExistsException):
            METALAKE_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(
                    MetalakeAlreadyExistsException, "mock error"
                )
            )

        with self.assertRaises(InternalError):
            METALAKE_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(InternalError, "mock error")
            )

        with self.assertRaises(RESTException):
            METALAKE_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(Exception, "mock error")
            )

    def test_catalog_error_handler(self):
        with self.assertRaises(ConnectionFailedException):
            CATALOG_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(
                    ConnectionFailedException, "mock error"
                )
            )

        with self.assertRaises(NoSuchMetalakeException):
            CATALOG_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(
                    NoSuchMetalakeException, "mock error"
                )
            )

        with self.assertRaises(NoSuchCatalogException):
            CATALOG_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(
                    NoSuchCatalogException, "mock error"
                )
            )

        with self.assertRaises(CatalogAlreadyExistsException):
            CATALOG_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(
                    CatalogAlreadyExistsException, "mock error"
                )
            )

        with self.assertRaises(InternalError):
            CATALOG_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(InternalError, "mock error")
            )

        with self.assertRaises(RESTException):
            CATALOG_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(Exception, "mock error")
            )

    def test_schema_error_handler(self):
        with self.assertRaises(NoSuchCatalogException):
            SCHEMA_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(
                    NoSuchCatalogException, "mock error"
                )
            )

        with self.assertRaises(NoSuchSchemaException):
            SCHEMA_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(
                    NoSuchSchemaException, "mock error"
                )
            )

        with self.assertRaises(SchemaAlreadyExistsException):
            SCHEMA_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(
                    SchemaAlreadyExistsException, "mock error"
                )
            )

        with self.assertRaises(NonEmptySchemaException):
            SCHEMA_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(
                    NonEmptySchemaException, "mock error"
                )
            )

        with self.assertRaises(UnsupportedOperationException):
            SCHEMA_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(
                    UnsupportedOperationException, "mock error"
                )
            )

        with self.assertRaises(InternalError):
            SCHEMA_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(InternalError, "mock error")
            )

        with self.assertRaises(RESTException):
            SCHEMA_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(Exception, "mock error")
            )

    def test_partition_error_handler(self):
        with self.assertRaises(IllegalArgumentException):
            PARTITION_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(
                    IllegalArgumentException, "mock error"
                )
            )

        with self.assertRaises(NoSuchSchemaException):
            PARTITION_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(
                    NoSuchSchemaException, "mock error"
                )
            )

        with self.assertRaises(NoSuchTableException):
            PARTITION_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(
                    NoSuchTableException, "mock error"
                )
            )

        with self.assertRaises(NoSuchPartitionException):
            PARTITION_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(
                    NoSuchPartitionException, "mock error"
                )
            )

        with self.assertRaises(NotFoundException):
            PARTITION_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(NotFoundException, "mock error")
            )

        with self.assertRaises(PartitionAlreadyExistsException):
            PARTITION_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(
                    PartitionAlreadyExistsException, "mock error"
                )
            )

        with self.assertRaises(RuntimeError):
            PARTITION_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(RuntimeError, "mock error")
            )

        with self.assertRaises(UnsupportedOperationException):
            PARTITION_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(
                    UnsupportedOperationException, "mock error"
                )
            )

        with self.assertRaises(CatalogNotInUseException):
            PARTITION_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(
                    CatalogNotInUseException, "mock error"
                )
            )

        with self.assertRaises(MetalakeNotInUseException):
            PARTITION_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(
                    MetalakeNotInUseException, "mock error"
                )
            )

        with self.assertRaises(NotInUseException):
            PARTITION_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(NotInUseException, "mock error")
            )

        with self.assertRaises(RESTException):
            PARTITION_ERROR_HANDLER.handle(
                ErrorResponse.generate_error_response(Exception, "mock error")
            )
