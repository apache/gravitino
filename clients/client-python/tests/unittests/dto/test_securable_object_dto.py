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

from gravitino.api.metadata_object import MetadataObject
from gravitino.dto.authorization.privilege_dto import PrivilegeDTO
from gravitino.api.authorization.privileges import Privilege
from gravitino.dto.authorization.securable_object_dto import SecurableObjectDTO


class TestSecurableObjectDTO(unittest.TestCase):
    def test_simple_name(self):
        obj = SecurableObjectDTO("my_catalog", MetadataObject.Type.CATALOG, [])
        self.assertEqual("my_catalog", obj.name())
        self.assertIsNone(obj.parent())
        self.assertEqual("my_catalog", obj.full_name())

    def test_dotted_name(self):
        obj = SecurableObjectDTO("my_catalog.my_schema", MetadataObject.Type.SCHEMA, [])
        self.assertEqual("my_schema", obj.name())
        self.assertEqual("my_catalog", obj.parent())

    def test_deeply_nested_name(self):
        obj = SecurableObjectDTO("catalog.schema.table", MetadataObject.Type.TABLE, [])
        self.assertEqual("table", obj.name())
        self.assertEqual("catalog.schema", obj.parent())

    def test_type(self):
        obj = SecurableObjectDTO("my_catalog", MetadataObject.Type.CATALOG, [])
        self.assertEqual(MetadataObject.Type.CATALOG, obj.type())

    def test_privileges(self):
        privs = [PrivilegeDTO(Privilege.Name.USE_CATALOG, Privilege.Condition.ALLOW)]
        obj = SecurableObjectDTO("my_catalog", MetadataObject.Type.CATALOG, privs)
        self.assertEqual(1, len(obj.privileges()))
        self.assertIsInstance(obj.privileges()[0], PrivilegeDTO)

    def test_privileges_immutability(self):
        privs = [PrivilegeDTO(Privilege.Name.USE_CATALOG, Privilege.Condition.ALLOW)]
        obj = SecurableObjectDTO("my_catalog", MetadataObject.Type.CATALOG, privs)
        obj.privileges().append(
            PrivilegeDTO(Privilege.Name.CREATE_TABLE, Privilege.Condition.ALLOW)
        )
        self.assertEqual(1, len(obj.privileges()))

    def test_equality_and_hash(self):
        obj1 = SecurableObjectDTO("my_catalog", MetadataObject.Type.CATALOG, [])
        obj2 = SecurableObjectDTO("my_catalog", MetadataObject.Type.CATALOG, [])
        obj3 = SecurableObjectDTO("other_catalog", MetadataObject.Type.CATALOG, [])
        self.assertEqual(obj1, obj2)
        self.assertEqual(hash(obj1), hash(obj2))
        self.assertNotEqual(obj1, obj3)

    def test_builder_validations(self):
        with self.assertRaises(ValueError):
            SecurableObjectDTO.builder().with_type(MetadataObject.Type.CATALOG).build()

        with self.assertRaises(ValueError):
            SecurableObjectDTO.builder().with_full_name("test").build()
