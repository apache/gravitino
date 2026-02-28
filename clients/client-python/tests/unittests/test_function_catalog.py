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
from unittest.mock import patch, Mock

from gravitino.api.function.function_change import FunctionChange
from gravitino.api.function.function_type import FunctionType
from gravitino.api.rel.types.types import Types
from gravitino.client.relational_catalog import RelationalCatalog
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.function.function_definition_dto import FunctionDefinitionDTO
from gravitino.dto.function.function_dto import FunctionDTO
from gravitino.dto.function.function_impl_dto import (
    SQLImplDTO,
)
from gravitino.dto.function.function_param_dto import FunctionParamDTO
from gravitino.dto.responses.drop_response import DropResponse
from gravitino.dto.responses.entity_list_response import EntityListResponse
from gravitino.dto.responses.function_list_response import FunctionListResponse
from gravitino.dto.responses.function_response import FunctionResponse
from gravitino.exceptions.base import (
    NoSuchSchemaException,
    NoSuchFunctionException,
    FunctionAlreadyExistsException,
)
from gravitino.name_identifier import NameIdentifier
from gravitino.namespace import Namespace
from gravitino.utils import HTTPClient, Response


class TestFunctionCatalog(unittest.TestCase):
    metalake_name = "test_metalake"
    catalog_name = "test_catalog"
    schema_name = "test_schema"
    catalog_namespace = Namespace.of(metalake_name)

    @classmethod
    def setUpClass(cls) -> None:
        cls.rest_client = HTTPClient("http://localhost:8090")
        cls.catalog = RelationalCatalog(
            catalog_namespace=cls.catalog_namespace,
            name=cls.catalog_name,
            catalog_type=RelationalCatalog.Type.RELATIONAL,
            provider="test_provider",
            audit=AuditDTO("anonymous"),
            rest_client=cls.rest_client,
        )

    def _get_mock_http_resp(self, json_str: str, return_code: int = 200):
        mock_http_resp = Mock()
        mock_http_resp.getcode.return_value = return_code
        mock_http_resp.read.return_value = json_str.encode("utf-8")
        mock_http_resp.info.return_value = None
        mock_http_resp.url = None
        mock_resp = Response(mock_http_resp)
        return mock_resp

    def _mock_function_dto(
        self, name, function_type, comment, deterministic
    ) -> FunctionDTO:
        params = [FunctionParamDTO(_name="param1", _data_type=Types.IntegerType.get())]
        impl = SQLImplDTO(
            _runtime="SPARK",
            _sql="SELECT param1 + 1",
            _resources=None,
            _properties={},
        )
        definition = FunctionDefinitionDTO(
            _parameters=params, _return_type=Types.IntegerType.get(), _impls=[impl]
        )
        return FunctionDTO(
            _name=name,
            _definitions=[definition],
            _function_type=function_type,
            _deterministic=deterministic,
            _comment=comment,
            _audit=AuditDTO(
                "creator", "2022-01-01T00:00:00Z", "modifier", "2022-01-01T00:00:00Z"
            ),
        )

    def test_list_functions(self):
        func1 = NameIdentifier.of(
            self.metalake_name, self.catalog_name, self.schema_name, "func1"
        )
        func2 = NameIdentifier.of(
            self.metalake_name, self.catalog_name, self.schema_name, "func2"
        )

        resp_body = EntityListResponse(_code=0, _idents=[func1, func2])
        mock_resp = self._get_mock_http_resp(resp_body.to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.get", return_value=mock_resp
        ):
            functions = self.catalog.as_function_catalog().list_functions(
                Namespace.of(self.schema_name)
            )
            self.assertEqual(2, len(functions))
            self.assertEqual("func1", functions[0].name())
            self.assertEqual("func2", functions[1].name())

        # Test NoSuchSchemaException
        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            side_effect=NoSuchSchemaException("schema not found"),
        ):
            with self.assertRaises(NoSuchSchemaException):
                self.catalog.as_function_catalog().list_functions(
                    Namespace.of(self.schema_name)
                )

    def test_list_function_infos(self):
        func1 = self._mock_function_dto("func1", FunctionType.SCALAR, "comment1", True)
        func2 = self._mock_function_dto("func2", FunctionType.SCALAR, "comment2", False)

        resp_body = FunctionListResponse(_code=0, _functions=[func1, func2])
        mock_resp = self._get_mock_http_resp(resp_body.to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.get", return_value=mock_resp
        ):
            functions = self.catalog.as_function_catalog().list_function_infos(
                Namespace.of(self.schema_name)
            )
            self.assertEqual(2, len(functions))
            self.assertEqual("func1", functions[0].name())
            self.assertEqual("func2", functions[1].name())
            self.assertEqual("comment1", functions[0].comment())
            self.assertEqual("comment2", functions[1].comment())

    def test_get_function(self):
        ident = NameIdentifier.of(self.schema_name, "func1")
        mock_function = self._mock_function_dto(
            "func1", FunctionType.SCALAR, "mock comment", True
        )
        resp_body = FunctionResponse(_code=0, _function=mock_function)
        mock_resp = self._get_mock_http_resp(resp_body.to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.get", return_value=mock_resp
        ):
            func = self.catalog.as_function_catalog().get_function(ident)
            self.assertIsNotNone(func)
            self.assertEqual("func1", func.name())
            self.assertEqual("mock comment", func.comment())

        # Test NoSuchFunctionException
        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            side_effect=NoSuchFunctionException("function not found"),
        ):
            with self.assertRaises(NoSuchFunctionException):
                self.catalog.as_function_catalog().get_function(ident)

    def test_register_function(self):
        ident = NameIdentifier.of(self.schema_name, "func1")
        mock_function = self._mock_function_dto(
            "func1", FunctionType.SCALAR, "mock comment", True
        )
        resp_body = FunctionResponse(_code=0, _function=mock_function)
        mock_resp = self._get_mock_http_resp(resp_body.to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.post", return_value=mock_resp
        ):
            func = self.catalog.as_function_catalog().register_function(
                ident,
                "mock comment",
                FunctionType.SCALAR,
                True,
                definitions=mock_function.definitions(),
            )
            self.assertIsNotNone(func)
            self.assertEqual("func1", func.name())
            self.assertEqual("mock comment", func.comment())

        # Test FunctionAlreadyExistsException
        with patch(
            "gravitino.utils.http_client.HTTPClient.post",
            side_effect=FunctionAlreadyExistsException("function already exists"),
        ):
            with self.assertRaises(FunctionAlreadyExistsException):
                self.catalog.as_function_catalog().register_function(
                    ident,
                    "mock comment",
                    FunctionType.SCALAR,
                    True,
                    definitions=mock_function.definitions(),
                )

    def test_alter_function(self):
        ident = NameIdentifier.of(self.schema_name, "func1")
        mock_function = self._mock_function_dto(
            "func1", FunctionType.SCALAR, "updated comment", True
        )
        resp_body = FunctionResponse(_code=0, _function=mock_function)
        mock_resp = self._get_mock_http_resp(resp_body.to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.put", return_value=mock_resp
        ):
            func = self.catalog.as_function_catalog().alter_function(
                ident, FunctionChange.update_comment("updated comment")
            )
            self.assertIsNotNone(func)
            self.assertEqual("updated comment", func.comment())

        # Test NoSuchFunctionException
        with patch(
            "gravitino.utils.http_client.HTTPClient.put",
            side_effect=NoSuchFunctionException("function not found"),
        ):
            with self.assertRaises(NoSuchFunctionException):
                self.catalog.as_function_catalog().alter_function(
                    ident, FunctionChange.update_comment("updated comment")
                )

    def test_drop_function(self):
        ident = NameIdentifier.of(self.schema_name, "func1")
        resp_body = DropResponse(_code=0, _dropped=True)
        mock_resp = self._get_mock_http_resp(resp_body.to_json())

        with patch(
            "gravitino.utils.http_client.HTTPClient.delete", return_value=mock_resp
        ):
            dropped = self.catalog.as_function_catalog().drop_function(ident)
            self.assertTrue(dropped)

        # Test function not exists
        resp_body = DropResponse(_code=0, _dropped=False)
        mock_resp = self._get_mock_http_resp(resp_body.to_json())
        with patch(
            "gravitino.utils.http_client.HTTPClient.delete", return_value=mock_resp
        ):
            dropped = self.catalog.as_function_catalog().drop_function(ident)
            self.assertFalse(dropped)
