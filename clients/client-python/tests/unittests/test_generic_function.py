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

from gravitino.api.function.function_type import FunctionType
from gravitino.api.rel.types.types import Types
from gravitino.api.tag.supports_tags import SupportsTags
from gravitino.client.generic_function import GenericFunction
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.function.function_definition_dto import FunctionDefinitionDTO
from gravitino.dto.function.function_dto import FunctionDTO
from gravitino.namespace import Namespace
from gravitino.utils.http_client import HTTPClient


class TestGenericFunction(unittest.TestCase):
    _rest_client = HTTPClient("http://localhost:8080")
    _function_namespace = Namespace.of(
        "demo_metalake",
        "demo_catalog",
        "demo_schema",
    )

    def _generic_function(self) -> GenericFunction:
        return GenericFunction(
            FunctionDTO(
                _name="demo_function",
                _function_type=FunctionType.SCALAR,
                _deterministic=True,
                _definitions=[
                    FunctionDefinitionDTO(
                        _parameters=[],
                        _return_type=Types.IntegerType.get(),
                        _impls=[],
                    )
                ],
                _comment="comment",
                _audit=AuditDTO("creator"),
            ),
            self._rest_client,
            self._function_namespace,
        )

    def test_generic_function(self) -> None:
        generic_function = self._generic_function()

        self.assertEqual("demo_function", generic_function.name())
        self.assertEqual(FunctionType.SCALAR, generic_function.function_type())
        self.assertTrue(generic_function.deterministic())
        self.assertEqual("comment", generic_function.comment())
        self.assertEqual(1, len(generic_function.definitions()))
        self.assertEqual("creator", generic_function.audit_info().creator())

    def test_extends_supports_tags_class(self) -> None:
        generic_function = self._generic_function()

        self.assertTrue(issubclass(GenericFunction, SupportsTags))
        expected_methods = ["list_tags", "list_tags_info", "get_tag", "associate_tags"]
        self.assertTrue(
            all(
                callable(getattr(generic_function, method, None))
                for method in expected_methods
            )
        )
