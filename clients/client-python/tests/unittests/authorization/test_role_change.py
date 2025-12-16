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

from __future__ import annotations

import typing
import unittest

from gravitino.api.authorization.privileges import Privilege
from gravitino.api.authorization.role_change import RoleChange
from gravitino.api.authorization.securable_objects import SecurableObjects
from gravitino.api.metadata_object import MetadataObject


class TestRoleChange(unittest.TestCase):
    def test_add_securable_object(self) -> None:
        securable_object = self.get_test_securable_object()
        role_change = RoleChange.add_securable_object(
            "role_name",
            securable_object,
        )
        self.assertEqual(role_change.role_name, "role_name")
        self.assertEqual(role_change.securable_object, securable_object)
        self.assertEqual(
            str(role_change),
            "ADDSECURABLEOBJECT role_name SecurableObject: "
            + "[fullName=test_catalog], [type=Type.CATALOG], [privileges=[allow_create_schema]]",
        )

    def test_remove_securable_object(self) -> None:
        securable_object = self.get_test_securable_object()
        role_change = RoleChange.remove_securable_object(
            "role_name",
            securable_object,
        )

        self.assertEqual(role_change.role_name, "role_name")
        self.assertEqual(role_change.securable_object, securable_object)
        self.assertEqual(
            str(role_change),
            "REMOVESECURABLEOBJECT role_name + SecurableObject: "
            + "[fullName=test_catalog], [type=Type.CATALOG], [privileges=[allow_create_schema]]",
        )

    def test_update_securable_object(self) -> None:
        securable_object = self.get_test_securable_object()
        new_securable_object = SecurableObjects.of_catalog(
            "test_catalog",
            [self.get_mock_privilege(condition=Privilege.Condition.DENY)],
        )
        role_change = RoleChange.update_securable_object(
            "role_name",
            securable_object,
            new_securable_object,
        )

        self.assertEqual(role_change.role_name, "role_name")
        self.assertEqual(role_change.securable_object, securable_object)
        self.assertEqual(role_change.new_securable_object, new_securable_object)
        self.assertEqual(
            str(role_change),
            "UPDATESECURABLEOBJECT role_name SecurableObject: "
            + "[fullName=test_catalog], [type=Type.CATALOG], [privileges=[allow_create_schema]]"
            + " SecurableObject: [fullName=test_catalog], [type=Type.CATALOG], [privileges=[deny_create_schema]]",
        )

    def get_test_securable_object(
        self,
        catalog_name: str = "test_catalog",
        privileges: typing.Optional[list[Privilege]] = None,
    ) -> SecurableObjects.SecurableObjectImpl:
        return SecurableObjects.of_catalog(
            catalog_name,
            [self.get_mock_privilege()] if privileges is None else privileges,
        )

    def get_mock_privilege(
        self,
        name: Privilege.Name = Privilege.Name.CREATE_SCHEMA,
        condition: Privilege.Condition = Privilege.Condition.ALLOW,
    ) -> Privilege:
        return MockPrivilege(name, condition)


class MockPrivilege(Privilege):
    def __init__(self, name: Privilege.Name, condition: Privilege.Condition) -> None:
        self._name = name
        self._condition = condition

    def name(self) -> Privilege.Name:
        return self._name

    def simple_string(self) -> str:
        return f"{self._condition.value.lower()}_{self._name.name.lower()}"

    def condition(self) -> Privilege.Condition:
        return self._condition

    def can_bind_to(self, obj_type: MetadataObject.Type) -> bool:
        # mock: allow everything
        return True

    def __repr__(self) -> str:
        return f"MockPrivilege(name={self._name}, condition={self._condition})"
