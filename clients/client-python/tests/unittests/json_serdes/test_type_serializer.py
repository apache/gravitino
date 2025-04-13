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

from gravitino.api.types.types import Types
from gravitino.utils.json_serdes import TypeSerializer
from gravitino.utils.json_serdes._helper.serdes_utils import SerdesUtils


class TestTypeSerializer(unittest.TestCase):
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
            self.assertEqual(TypeSerializer.serialize(data=type_), simple_string)

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
        serialized_result = TypeSerializer.serialize(struct_type)
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
            serialized_result = TypeSerializer.serialize(list_type)
            self.assertEqual(serialized_result.get(SerdesUtils.TYPE), SerdesUtils.LIST)
            self.assertEqual(
                serialized_result.get(SerdesUtils.LIST_ELEMENT_TYPE), simple_string
            )
            self.assertEqual(
                serialized_result.get(SerdesUtils.LIST_ELEMENT_NULLABLE), False
            )
