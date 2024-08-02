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

import unittest

from gravitino.dto.responses.error_response import ErrorResponse
from gravitino.exceptions.base import (
    NoSuchSchemaException,
    NoSuchFilesetException,
    NoSuchMetalakeException,
    MetalakeAlreadyExistsException,
    InternalError,
    RESTException,
    NotFoundException,
    IllegalArgumentException,
    AlreadyExistsException,
    NotEmptyException,
    UnsupportedOperationException,
)

from gravitino.exceptions.handlers.rest_error_handler import REST_ERROR_HANDLER
from gravitino.exceptions.handlers.fileset_error_handler import FILESET_ERROR_HANDLER
from gravitino.exceptions.handlers.metalake_error_handler import METALAKE_ERROR_HANDLER


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
