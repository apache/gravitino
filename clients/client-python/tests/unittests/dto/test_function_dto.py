# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License a
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Tests for Function DTOs."""

import unittest

from gravitino.api.function.function_type import FunctionType
from gravitino.api.rel.types.types import Types
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.function.function_column_dto import FunctionColumnDTO
from gravitino.dto.function.function_definition_dto import FunctionDefinitionDTO
from gravitino.dto.function.function_dto import FunctionDTO
from gravitino.dto.function.function_impl_dto import (
    SQLImplDTO,
    PythonImplDTO,
    JavaImplDTO,
)
from gravitino.dto.function.function_param_dto import FunctionParamDTO
from gravitino.dto.function.function_resources_dto import FunctionResourcesDTO
from gravitino.dto.rel.expressions.literal_dto import LiteralDTO


class TestFunctionDTO(unittest.TestCase):
    """Tests for Function DTOs."""

    def test_function_dto(self):
        """Test FunctionDTO serialization and deserialization."""
        audit = AuditDTO(
            "creator", "2022-01-01T00:00:00Z", "modifier", "2022-01-01T00:00:00Z"
        )
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

        function_dto = FunctionDTO(
            _name="func1",
            _definitions=[definition],
            _function_type=FunctionType.SCALAR,
            _deterministic=True,
            _comment="comment",
            _audit=audit,
        )

        json_str = function_dto.to_json()
        deserialized = FunctionDTO.from_json(json_str)
        self.assertEqual(function_dto.name(), deserialized.name())
        self.assertEqual(function_dto.comment(), deserialized.comment())
        self.assertEqual(
            function_dto.audit_info().creator(), deserialized.audit_info().creator()
        )
        self.assertEqual(
            function_dto.definitions()[0].impls()[0].sql(),
            deserialized.definitions()[0].impls()[0].sql(),
        )
        self.assertEqual(
            function_dto.definitions()[0].return_type(),
            deserialized.definitions()[0].return_type(),
        )

    def test_function_definition_dto(self):
        """Test FunctionDefinitionDTO serialization and deserialization."""
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

        json_str = definition.to_json()
        deserialized = FunctionDefinitionDTO.from_json(json_str)
        self.assertEqual(
            definition.parameters()[0].name(), deserialized.parameters()[0].name()
        )
        self.assertEqual(definition.impls()[0].sql(), deserialized.impls()[0].sql())
        self.assertEqual(definition.return_type(), deserialized.return_type())

    def test_function_param_dto_default_value(self):
        """Test FunctionParamDTO supports defaultValue serdes."""
        default_value = (
            LiteralDTO.builder()
            .with_data_type(Types.IntegerType.get())
            .with_value("0")
            .build()
        )
        param = FunctionParamDTO(
            _name="x",
            _data_type=Types.IntegerType.get(),
            _comment="comment",
            _default_value=default_value,
        )

        json_str = param.to_json()
        deserialized = FunctionParamDTO.from_json(json_str)
        self.assertEqual(param, deserialized)
        self.assertIsNotNone(deserialized.default_value())

    def test_function_definition_hash_with_impls(self):
        """Test FunctionDefinitionDTO hash works when impl list is not empty."""
        definition = FunctionDefinitionDTO(
            _parameters=[
                FunctionParamDTO(_name="param1", _data_type=Types.IntegerType.get())
            ],
            _return_type=Types.IntegerType.get(),
            _impls=[
                SQLImplDTO(
                    _runtime="SPARK",
                    _sql="SELECT param1 + 1",
                    _resources=None,
                    _properties={},
                )
            ],
        )

        self.assertIsInstance(hash(definition), int)

    def test_function_impl_dto(self):
        """Test FunctionImplDTO serialization and deserialization."""
        sql_impl = SQLImplDTO(
            _runtime="SPARK",
            _sql="SELECT 1",
            _resources=None,
            _properties={},
        )
        json_str = sql_impl.to_json()
        deserialized = SQLImplDTO.from_json(json_str)
        self.assertEqual(sql_impl.sql(), deserialized.sql())
        self.assertEqual(sql_impl.runtime(), deserialized.runtime())

        python_impl = PythonImplDTO(
            _runtime="PYTHON",
            _handler="test_module.test_func",
            _code_block="def test_func(): pass",
            _resources=None,
            _properties={},
        )
        json_str = python_impl.to_json()
        deserialized = PythonImplDTO.from_json(json_str)
        self.assertEqual(python_impl.handler(), deserialized.handler())
        self.assertEqual(python_impl.code_block(), deserialized.code_block())

        java_impl = JavaImplDTO(
            _runtime="JAVA",
            _class_name="com.test.TestClass",
            _resources=None,
            _properties={},
        )
        json_str = java_impl.to_json()
        deserialized = JavaImplDTO.from_json(json_str)
        self.assertEqual(java_impl.class_name(), deserialized.class_name())

    def test_function_resources_dto(self):
        """Test FunctionResourcesDTO serialization and deserialization."""
        resources = FunctionResourcesDTO(_jars=["v1"], _files=None, _archives=None)
        json_str = resources.to_json()
        deserialized = FunctionResourcesDTO.from_json(json_str)
        self.assertEqual(resources.jars(), deserialized.jars())

    def test_function_column_dto(self):
        """Test FunctionColumnDTO serialization and deserialization."""
        column = FunctionColumnDTO(_name="col1", _data_type=Types.IntegerType.get())
        json_str = column.to_json()
        deserialized = FunctionColumnDTO.from_json(json_str)
        self.assertEqual(column.name(), deserialized.name())
        self.assertEqual(column.data_type(), deserialized.data_type())

    def test_function_type_cannot_be_null(self):
        """Test FunctionDTO rejects null functionType during deserialization."""
        json_str = (
            '{"name":"func1","functionType":null,"deterministic":true,"definitions":[]}'
        )
        with self.assertRaises(ValueError):
            FunctionDTO.from_json(json_str)
