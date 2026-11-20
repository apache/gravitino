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

from gravitino.api.authorization.privileges import Privileges
from gravitino.api.metadata_object import MetadataObject
from gravitino.client.dto_converters import DTOConverters
from gravitino.dto.authorization.privilege_dto import PrivilegeDTO


class TestGlossaryPrivileges(unittest.TestCase):
    def test_metalake_grants_round_trip(self):
        for name, bit in (
            ("VIEW_DEFINITION", 44),
            ("CREATE_DEFINITION", 45),
            ("MANAGE_DEFINITIONS", 46),
        ):
            for factory in (Privileges.allow, Privileges.deny):
                privilege = factory(name)
                self.assertEqual(1 << bit, privilege.name().low_bits)
                for scope in MetadataObject.Type:
                    self.assertEqual(
                        scope == MetadataObject.Type.METALAKE,
                        privilege.can_bind_to(scope),
                    )
                dto = DTOConverters.to_privilege_dto(privilege)
                self.assertEqual(dto, PrivilegeDTO.from_json(dto.to_json()))
