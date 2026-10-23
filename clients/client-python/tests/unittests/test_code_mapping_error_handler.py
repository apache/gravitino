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
from io import BytesIO
from unittest.mock import patch
from urllib.error import HTTPError

from gravitino.constants.error import ErrorConstants
from gravitino.dto.responses.error_response import ErrorResponse
from gravitino.exceptions.base import (
    CatalogNotInUseException,
    FunctionAlreadyExistsException,
    IllegalArgumentException,
    IllegalJobTemplateOperationException,
    InUseException,
    JobTemplateAlreadyExistsException,
    MetalakeNotInUseException,
    ModelAlreadyExistsException,
    ModelVersionAliasesAlreadyExistException,
    NoSuchFunctionException,
    NoSuchJobException,
    NoSuchJobTemplateException,
    NoSuchMetalakeException,
    NoSuchModelException,
    NoSuchModelVersionException,
    NoSuchModelVersionURINameException,
    NoSuchSchemaException,
    NoSuchTagException,
    NotFoundException,
    RESTException,
    TagAlreadyAssociatedException,
    TagAlreadyExistsException,
)
from gravitino.exceptions.handlers.function_error_handler import FUNCTION_ERROR_HANDLER
from gravitino.exceptions.handlers.job_error_handler import JOB_ERROR_HANDLER
from gravitino.exceptions.handlers.model_error_handler import MODEL_ERROR_HANDLER
from gravitino.exceptions.handlers.secret_error_handler import SECRET_ERROR_HANDLER
from gravitino.exceptions.handlers.tag_error_handler import TAG_ERROR_HANDLER
from gravitino.utils.http_client import HTTPClient


class TestCodeMappingErrorHandler(unittest.TestCase):
    """Regression tests for the table-driven specialized error handlers.

    These cover the code-to-exception mappings of the function, model, job, tag
    and secret handlers, which the rest of the suite does not exercise. The
    ``ErrorResponse`` is built from explicit wire values and the expected
    exception is defined locally, so the assertions do not depend on the
    mapping table under test.
    """

    def _assert_code_exception_map(self, handler, cases):
        """Asserts the handler raises the expected exception for each case.

        ``cases`` is a sequence of ``(code, type, expected_exception)`` tuples.
        """
        for code, error_type, expected in cases:
            with self.subTest(
                handler=type(handler).__name__, code=code, error_type=error_type
            ):
                response = ErrorResponse(code, error_type, "mock error", None)
                with self.assertRaises(expected):
                    handler.handle(response)

    def test_function_error_handler_specialized_mappings(self):
        self._assert_code_exception_map(
            FUNCTION_ERROR_HANDLER,
            [
                (
                    ErrorConstants.NOT_FOUND_CODE,
                    "NoSuchSchemaException",
                    NoSuchSchemaException,
                ),
                (
                    ErrorConstants.NOT_FOUND_CODE,
                    "NoSuchFunctionException",
                    NoSuchFunctionException,
                ),
                (
                    ErrorConstants.ALREADY_EXISTS_CODE,
                    "FunctionAlreadyExistsException",
                    FunctionAlreadyExistsException,
                ),
                (
                    ErrorConstants.NOT_IN_USE_CODE,
                    "CatalogNotInUseException",
                    CatalogNotInUseException,
                ),
                (
                    ErrorConstants.NOT_IN_USE_CODE,
                    "MetalakeNotInUseException",
                    MetalakeNotInUseException,
                ),
            ],
        )

    def test_model_error_handler_specialized_mappings(self):
        self._assert_code_exception_map(
            MODEL_ERROR_HANDLER,
            [
                (
                    ErrorConstants.NOT_FOUND_CODE,
                    "NoSuchSchemaException",
                    NoSuchSchemaException,
                ),
                (
                    ErrorConstants.NOT_FOUND_CODE,
                    "NoSuchModelException",
                    NoSuchModelException,
                ),
                (
                    ErrorConstants.NOT_FOUND_CODE,
                    "NoSuchModelVersionException",
                    NoSuchModelVersionException,
                ),
                (
                    ErrorConstants.NOT_FOUND_CODE,
                    "NoSuchModelVersionURINameException",
                    NoSuchModelVersionURINameException,
                ),
                (
                    ErrorConstants.ALREADY_EXISTS_CODE,
                    "ModelAlreadyExistsException",
                    ModelAlreadyExistsException,
                ),
                (
                    ErrorConstants.ALREADY_EXISTS_CODE,
                    "ModelVersionAliasesAlreadyExistException",
                    ModelVersionAliasesAlreadyExistException,
                ),
                (
                    ErrorConstants.NOT_IN_USE_CODE,
                    "CatalogNotInUseException",
                    CatalogNotInUseException,
                ),
                (
                    ErrorConstants.NOT_IN_USE_CODE,
                    "MetalakeNotInUseException",
                    MetalakeNotInUseException,
                ),
            ],
        )

    def test_job_error_handler_specialized_mappings(self):
        self._assert_code_exception_map(
            JOB_ERROR_HANDLER,
            [
                (
                    ErrorConstants.ILLEGAL_ARGUMENTS_CODE,
                    "IllegalJobTemplateOperationException",
                    IllegalJobTemplateOperationException,
                ),
                (
                    ErrorConstants.NOT_FOUND_CODE,
                    "NoSuchMetalakeException",
                    NoSuchMetalakeException,
                ),
                (
                    ErrorConstants.NOT_FOUND_CODE,
                    "NoSuchJobTemplateException",
                    NoSuchJobTemplateException,
                ),
                (
                    ErrorConstants.NOT_FOUND_CODE,
                    "NoSuchJobException",
                    NoSuchJobException,
                ),
                # ALREADY_EXISTS_CODE and IN_USE_CODE map to a single class
                # regardless of the response type.
                (
                    ErrorConstants.ALREADY_EXISTS_CODE,
                    "AnyType",
                    JobTemplateAlreadyExistsException,
                ),
                (ErrorConstants.IN_USE_CODE, "AnyType", InUseException),
                (
                    ErrorConstants.NOT_IN_USE_CODE,
                    "MetalakeNotInUseException",
                    MetalakeNotInUseException,
                ),
            ],
        )

    def test_tag_error_handler_specialized_mappings(self):
        self._assert_code_exception_map(
            TAG_ERROR_HANDLER,
            [
                (
                    ErrorConstants.ILLEGAL_ARGUMENTS_CODE,
                    "AnyType",
                    IllegalArgumentException,
                ),
                (
                    ErrorConstants.NOT_FOUND_CODE,
                    "NoSuchMetalakeException",
                    NoSuchMetalakeException,
                ),
                (
                    ErrorConstants.NOT_FOUND_CODE,
                    "NoSuchTagException",
                    NoSuchTagException,
                ),
                (
                    ErrorConstants.ALREADY_EXISTS_CODE,
                    "TagAlreadyExistsException",
                    TagAlreadyExistsException,
                ),
                (
                    ErrorConstants.ALREADY_EXISTS_CODE,
                    "TagAlreadyAssociatedException",
                    TagAlreadyAssociatedException,
                ),
                (
                    ErrorConstants.NOT_IN_USE_CODE,
                    "AnyType",
                    MetalakeNotInUseException,
                ),
                (ErrorConstants.INTERNAL_ERROR_CODE, "AnyType", RuntimeError),
            ],
        )

    def test_secret_error_handler_specialized_mappings(self):
        self._assert_code_exception_map(
            SECRET_ERROR_HANDLER,
            [
                (
                    ErrorConstants.NOT_IN_USE_CODE,
                    "AnyType",
                    CatalogNotInUseException,
                ),
            ],
        )

    def test_specialized_handlers_known_code_unknown_type_fall_back(self):
        # The code is present in the handler's map, but the response type does
        # not match any entry, so the handler must fall back to the generic
        # mapping for that code.
        for handler in (FUNCTION_ERROR_HANDLER, MODEL_ERROR_HANDLER):
            with self.subTest(handler=type(handler).__name__):
                response = ErrorResponse(
                    ErrorConstants.NOT_FOUND_CODE,
                    "UnexpectedNotFoundType",
                    "mock error",
                    None,
                )
                with self.assertRaises(NotFoundException):
                    handler.handle(response)

    def test_specialized_handlers_unknown_code_fall_back(self):
        for handler in (
            FUNCTION_ERROR_HANDLER,
            MODEL_ERROR_HANDLER,
            JOB_ERROR_HANDLER,
            TAG_ERROR_HANDLER,
            SECRET_ERROR_HANDLER,
        ):
            with self.subTest(handler=type(handler).__name__):
                response = ErrorResponse(
                    1999, "FutureServerException", "Future error", None
                )
                with self.assertRaisesRegex(
                    RESTException, "Unable to process: Future error"
                ):
                    handler.handle(response)

    def test_specialized_handler_message_includes_server_stack(self):
        response = ErrorResponse(
            ErrorConstants.NOT_FOUND_CODE,
            "NoSuchFunctionException",
            "Function missing",
            [
                "at com.example.Foo.bar(Foo.java:1)",
                "at com.example.Baz.qux(Baz.java:2)",
            ],
        )
        with self.assertRaises(NoSuchFunctionException) as context:
            FUNCTION_ERROR_HANDLER.handle(response)
        self.assertEqual(
            "Function missing\n"
            "at com.example.Foo.bar(Foo.java:1)\n"
            "at com.example.Baz.qux(Baz.java:2)",
            str(context.exception),
        )

        generic_response = ErrorResponse(
            1999, "FutureServerException", "Future error", ["stack line"]
        )
        with self.assertRaisesRegex(
            RESTException, "Unable to process: Future error\nstack line"
        ):
            SECRET_ERROR_HANDLER.handle(generic_response)

    def test_function_error_handler_through_http_error_response(self):
        body = (
            b'{"code":1003,"type":"NoSuchFunctionException",'
            b'"message":"Function not found","stack":null}'
        )
        with patch("gravitino.utils.http_client.build_opener") as build_opener:
            build_opener.return_value.open.side_effect = HTTPError(
                "http://localhost:8090/api/test",
                404,
                "Not Found",
                None,
                BytesIO(body),
            )
            with self.assertRaisesRegex(NoSuchFunctionException, "Function not found"):
                HTTPClient("http://localhost:8090").get(
                    "/api/test", error_handler=FUNCTION_ERROR_HANDLER
                )
