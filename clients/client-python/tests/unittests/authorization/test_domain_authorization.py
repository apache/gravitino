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

import json
import unittest

from gravitino.api.authorization.privileges import Privilege, Privileges
from gravitino.api.authorization.securable_objects import SecurableObjects
from gravitino.api.metadata_object import MetadataObject
from gravitino.client.dto_converters import DTOConverters
from gravitino.dto.authorization.privilege_dto import PrivilegeDTO
from gravitino.dto.authorization.securable_object_dto import SecurableObjectDTO
from gravitino.exceptions.base import (
    GravitinoRuntimeException,
    RESTException,
    IllegalArgumentException,
)


class TestDomainAuthorization(unittest.TestCase):
    def test_domain_privileges_and_role_wire_round_trip(self):
        for name, bit in (("MANAGE_DOMAIN", 42), ("USE_DOMAIN", 43)):
            for factory, condition in (
                (Privileges.allow, Privilege.Condition.ALLOW),
                (Privileges.deny, Privilege.Condition.DENY),
            ):
                with self.subTest(name=name, condition=condition):
                    privilege = factory(name)
                    self.assertIs(privilege, factory(name))
                    self.assertEqual(condition, privilege.condition())
                    self.assertEqual(0, privilege.name().high_bits)
                    self.assertEqual(1 << bit, privilege.name().low_bits)
                    for obj_type in MetadataObject.Type:
                        self.assertEqual(
                            obj_type
                            in {
                                MetadataObject.Type.METALAKE,
                                MetadataObject.Type.DOMAIN,
                            },
                            privilege.can_bind_to(obj_type),
                        )
                    dto = DTOConverters.to_privilege_dto(privilege)
                    self.assertEqual(
                        {"name": name.lower(), "condition": condition.value.lower()},
                        json.loads(dto.to_json()),
                    )
                    self.assertEqual(dto, PrivilegeDTO.from_json(dto.to_json()))
                    self.assertTrue(dto.can_bind_to(MetadataObject.Type.DOMAIN))
                    scope = SecurableObjects.parse(
                        "Finance/US. East,?&+%", MetadataObject.Type.DOMAIN, [privilege]
                    )
                    scope_dto = DTOConverters.to_securable_object_dto(scope)
                    restored = SecurableObjectDTO.from_json(scope_dto.to_json())
                    self.assertEqual(scope.name(), restored.name())
                    self.assertIsNone(restored.parent())
                    self.assertEqual(scope.privileges(), restored.privileges())
        self.assertTrue(
            Privileges.allow("MANAGE_GRANTS").can_bind_to(MetadataObject.Type.DOMAIN)
        )
        self.assertTrue(
            Privileges.deny("MANAGE_GRANTS").can_bind_to(MetadataObject.Type.DOMAIN)
        )

    def test_remote_error_messages_are_not_format_strings(self):
        for exception in (
            GravitinoRuntimeException,
            RESTException,
            IllegalArgumentException,
        ):
            for message in (
                "missing Finance/US. East,?&+%",
                "literal %s",
                "literal 100%%",
            ):
                with self.subTest(exception=exception, message=message):
                    self.assertEqual(message, str(exception(message)))
            self.assertEqual(
                "missing Finance%", str(exception("missing %s", "Finance%"))
            )
