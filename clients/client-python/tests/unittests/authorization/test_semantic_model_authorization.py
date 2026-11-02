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
from gravitino.api.metadata_objects import MetadataObjects
from gravitino.client.dto_converters import DTOConverters
from gravitino.dto.authorization.privilege_dto import PrivilegeDTO
from gravitino.dto.authorization.securable_object_dto import SecurableObjectDTO
from gravitino.dto.metadata_object_dto import MetadataObjectDTO
from gravitino.dto.requests.privilege_grant_request import PrivilegeGrantRequest
from gravitino.dto.requests.privilege_revoke_request import PrivilegeRevokeRequest
from gravitino.exceptions.base import IllegalArgumentException


class TestSemanticModelAuthorization(unittest.TestCase):
    def test_metadata_mapping_and_parent(self):
        model_type = MetadataObject.Type.SEMANTIC_MODEL
        self.assertEqual("semantic_model", model_type.value)
        model = MetadataObjects.parse("catalog.schema.sales", model_type)
        self.assertEqual("sales", model.name())
        self.assertEqual("catalog.schema", model.parent())
        self.assertEqual(model_type, model.type())
        self.assertEqual(
            model, MetadataObjects.of(["catalog", "schema", "sales"], model_type)
        )
        self.assertEqual(
            model, MetadataObjects.of("sales", model_type, "catalog.schema")
        )
        schema = MetadataObjects.parent(model)
        self.assertEqual(MetadataObject.Type.SCHEMA, schema.type())
        self.assertEqual("catalog.schema", schema.full_name())
        self.assertEqual(
            MetadataObject.Type.CATALOG, MetadataObjects.parent(schema).type()
        )
        dto = MetadataObjectDTO.from_json(
            '{"type":"semantic_model","fullName":"catalog.schema.sales"}'
        )
        self.assertEqual(model_type, dto.type())
        self.assertEqual(model.full_name(), dto.full_name())
        self.assertEqual("semantic_model", json.loads(dto.to_json())["type"])

    def test_invalid_semantic_model_names(self):
        for name in ("", "sales", "schema.sales", "a.b.c.d", "catalog.*.sales"):
            with self.subTest(name=name), self.assertRaises(IllegalArgumentException):
                MetadataObjects.parse(name, MetadataObject.Type.SEMANTIC_MODEL)

    def test_privileges_match_java_contract(self):
        parents = {
            MetadataObject.Type.METALAKE,
            MetadataObject.Type.CATALOG,
            MetadataObject.Type.SCHEMA,
        }
        for name, bit in (
            ("CREATE_SEMANTIC_MODEL", 39),
            ("USE_SEMANTIC_MODEL", 40),
            ("MODIFY_SEMANTIC_MODEL", 41),
        ):
            expected_types = set(parents)
            if name != "CREATE_SEMANTIC_MODEL":
                expected_types.add(MetadataObject.Type.SEMANTIC_MODEL)
            for factory, condition in (
                (Privileges.allow, Privilege.Condition.ALLOW),
                (Privileges.deny, Privilege.Condition.DENY),
            ):
                with self.subTest(name=name, condition=condition):
                    privilege = factory(name)
                    self.assertIs(privilege, factory(name))
                    self.assertEqual(name, privilege.name().name)
                    self.assertEqual(0, privilege.name().high_bits)
                    self.assertEqual(1 << bit, privilege.name().low_bits)
                    self.assertEqual(condition, privilege.condition())
                    self.assertEqual(
                        f"{condition.value} {name.lower().replace('_', ' ')}",
                        privilege.simple_string(),
                    )
                    dto = DTOConverters.to_privilege_dto(privilege)
                    wire = {"name": name.lower(), "condition": condition.value.lower()}
                    self.assertEqual(wire, json.loads(dto.to_json()))
                    for casing in (str.lower, str.upper):
                        decoded = PrivilegeDTO.from_json(
                            json.dumps(
                                {key: casing(value) for key, value in wire.items()}
                            )
                        )
                        self.assertEqual(privilege, decoded)
                        self.assertEqual(hash(privilege), hash(decoded))
                        for obj_type in MetadataObject.Type:
                            self.assertEqual(
                                obj_type in expected_types,
                                decoded.can_bind_to(obj_type),
                            )
                            self.assertEqual(
                                obj_type in expected_types,
                                privilege.can_bind_to(obj_type),
                            )
                    for request_type in (PrivilegeGrantRequest, PrivilegeRevokeRequest):
                        request = request_type([dto])
                        request.validate()
                        self.assertEqual(
                            {"privileges": [wire]}, json.loads(request.to_json())
                        )

    def test_manage_grants_and_unrelated_privileges(self):
        for factory in (Privileges.allow, Privileges.deny):
            self.assertTrue(
                factory("MANAGE_GRANTS").can_bind_to(MetadataObject.Type.SEMANTIC_MODEL)
            )
            for name in (
                "USE_MODEL",
                "SELECT_TABLE",
                "USE_SECRETS",
                "INCLUDE_CREDENTIAL_SECRETS",
            ):
                with self.subTest(name=name):
                    self.assertFalse(
                        factory(name).can_bind_to(MetadataObject.Type.SEMANTIC_MODEL)
                    )

    def test_securable_object_round_trip(self):
        privileges = [
            Privileges.allow("USE_SEMANTIC_MODEL"),
            Privileges.deny("MODIFY_SEMANTIC_MODEL"),
        ]
        schema = SecurableObjects.of_schema(
            SecurableObjects.of_catalog("catalog", []), "schema", []
        )
        model = SecurableObjects.of_semantic_model(schema, "sales", privileges)
        self.assertEqual("catalog.schema.sales", model.full_name())
        self.assertEqual("catalog.schema", model.parent())
        self.assertEqual("sales", model.name())
        self.assertEqual(MetadataObject.Type.SEMANTIC_MODEL, model.type())
        self.assertEqual(
            model,
            SecurableObjects.parse(model.full_name(), model.type(), privileges),
        )
        dto = DTOConverters.to_securable_object_dto(model)
        wire = json.loads(dto.to_json())
        self.assertEqual("semantic_model", wire["type"])
        self.assertEqual("catalog.schema.sales", wire["fullName"])
        self.assertCountEqual(
            [
                {"name": "use_semantic_model", "condition": "allow"},
                {"name": "modify_semantic_model", "condition": "deny"},
            ],
            wire["privileges"],
        )
        decoded = SecurableObjectDTO.from_json(dto.to_json())
        self.assertEqual(model.type(), decoded.type())
        self.assertEqual(model.full_name(), decoded.full_name())
        self.assertCountEqual(privileges, decoded.privileges())
