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
from itertools import combinations, product

from gravitino.api.types.json_serdes import TypeSerdes
from gravitino.api.types.json_serdes._helper.serdes_utils import SerdesUtils
from gravitino.api.types.type import PrimitiveType
from gravitino.api.types.types import Types


class MockType(PrimitiveType):
    def __init__(self, name: str):
        self._name = name

    def name(self) -> str:
        return self._name

    def simple_string(self):
        return "mock_type"


class TestTypeSerdes(unittest.TestCase):
    def setUp(self):
        self._primitive_and_none_types = {
            **SerdesUtils.TYPES,
            **{
                "decimal(10,2)": Types.DecimalType.of(10, 2),
                "fixed(10)": Types.FixedType.of(10),
                "char(10)": Types.FixedCharType.of(10),
                "varchar(10)": Types.VarCharType.of(10),
            },
        }

    def test_serialize_primitive_and_none_type(self):
        for simple_string, type_ in self._primitive_and_none_types.items():
            self.assertEqual(TypeSerdes.serialize(data_type=type_), simple_string)

    def test_serialize_struct_type_of_primitive_and_none_types(self):
        types = self._primitive_and_none_types.values()
        fields = [
            Types.StructType.Field.not_null_field(
                name=f"field_{field_idx}",
                field_type=type_,
                comment=f"comment {field_idx}" if field_idx % 2 == 0 else None,
            )
            for type_, field_idx in zip(types, range(len(types)))
        ]

        struct_type = Types.StructType.of(*fields)
        serialized_result = TypeSerdes.serialize(struct_type)
        serialized_fields = serialized_result.get(SerdesUtils.FIELDS)

        self.assertEqual(serialized_result.get(SerdesUtils.TYPE), SerdesUtils.STRUCT)
        for field, serialized_field in zip(fields, serialized_fields):
            self.assertEqual(
                serialized_field.get(SerdesUtils.STRUCT_FIELD_NAME), field.name()
            )
            self.assertEqual(
                serialized_field.get(SerdesUtils.TYPE),
                SerdesUtils.write_data_type(field.type()),
            )
            self.assertEqual(
                serialized_field.get(SerdesUtils.STRUCT_FIELD_NULLABLE),
                field.nullable(),
            )
            self.assertEqual(
                serialized_field.get(SerdesUtils.STRUCT_FIELD_COMMENT), field.comment()
            )

    def test_serialize_list_type_of_primitive_and_none_types(self):
        for simple_string, type_ in self._primitive_and_none_types.items():
            list_type = Types.ListType.of(element_type=type_, element_nullable=False)
            serialized_result = TypeSerdes.serialize(list_type)
            self.assertEqual(serialized_result.get(SerdesUtils.TYPE), SerdesUtils.LIST)
            self.assertEqual(
                serialized_result.get(SerdesUtils.LIST_ELEMENT_TYPE), simple_string
            )
            self.assertEqual(
                serialized_result.get(SerdesUtils.LIST_ELEMENT_NULLABLE), False
            )

    def test_serialize_map_type_of_primitive_and_none_types(self):
        types = self._primitive_and_none_types.values()
        for key_type, value_type in product(types, types):
            map_type = Types.MapType.of(
                key_type=key_type, value_type=value_type, value_nullable=False
            )
            serialized_result = TypeSerdes.serialize(map_type)
            self.assertEqual(serialized_result.get(SerdesUtils.TYPE), SerdesUtils.MAP)
            self.assertEqual(
                serialized_result.get(SerdesUtils.MAP_KEY_TYPE),
                key_type.simple_string(),
            )
            self.assertEqual(
                serialized_result.get(SerdesUtils.MAP_VALUE_TYPE),
                value_type.simple_string(),
            )
            self.assertEqual(
                serialized_result.get(SerdesUtils.MAP_VALUE_NULLABLE), False
            )

    def test_serialize_union_type_of_primitive_and_none_types(self):
        types = self._primitive_and_none_types.values()
        for types in combinations(types, 2):
            union_type = Types.UnionType.of(*types)
            serialized_result = TypeSerdes.serialize(union_type)
            self.assertEqual(serialized_result.get(SerdesUtils.TYPE), SerdesUtils.UNION)
            self.assertListEqual(
                serialized_result.get(SerdesUtils.UNION_TYPES),
                [type_.simple_string() for type_ in types],
            )

    def test_serialize_external_type(self):
        external_type = Types.ExternalType.of(catalog_string="catalog_string")
        serialized_result = TypeSerdes.serialize(external_type)
        self.assertEqual(serialized_result.get(SerdesUtils.TYPE), SerdesUtils.EXTERNAL)
        self.assertEqual(
            serialized_result.get(SerdesUtils.CATALOG_STRING), "catalog_string"
        )

    def test_write_unparsed_type(self):
        unparsed_type = Types.UnparsedType.of(unparsed_type="unparsed_type")
        serialized_result = TypeSerdes.serialize(unparsed_type)
        self.assertEqual(serialized_result.get(SerdesUtils.TYPE), SerdesUtils.UNPARSED)
        self.assertEqual(
            serialized_result.get(SerdesUtils.UNPARSED_TYPE), "unparsed_type"
        )

        mock_type = MockType(name="mock")
        result = TypeSerdes.serialize(mock_type)
        self.assertEqual(result.get(SerdesUtils.TYPE), SerdesUtils.UNPARSED)
        self.assertEqual(
            result.get(SerdesUtils.UNPARSED_TYPE), mock_type.simple_string()
        )
