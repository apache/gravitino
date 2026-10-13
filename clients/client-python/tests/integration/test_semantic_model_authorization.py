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

import os
import shutil
import tempfile
import uuid
from unittest.mock import patch

from gravitino import Catalog, GravitinoClient, NameIdentifier
from gravitino.api.authorization.privileges import Privileges
from gravitino.api.authorization.securable_objects import SecurableObjects
from gravitino.api.rel.column import Column
from gravitino.api.rel.types.types import Types
from gravitino.api.semantic.dataset import Dataset
from gravitino.api.semantic.semantic_model_change import SemanticModelChange
from gravitino.api.semantic.semantic_model_definition import SemanticModelDefinition
from gravitino.auth.simple_auth_provider import SimpleAuthProvider
from gravitino.client.metadata_object_role_operations import (
    MetadataObjectRoleOperations,
)
from gravitino.exceptions.base import ForbiddenException
from gravitino.namespace import Namespace
from tests.integration.integration_test_env import AuthorizationIntegrationTestEnv


class TestSemanticModelAuthorization(AuthorizationIntegrationTestEnv):
    """Exercise SDK privileges against a server with authorization enabled."""

    def setUp(self):
        self._metalake_name = f"semantic_auth_{uuid.uuid4().hex[:8]}"
        super().setUp()
        warehouse = tempfile.mkdtemp(prefix="semantic_auth_")
        self.addCleanup(shutil.rmtree, warehouse)
        self.addCleanup(super().tearDown)
        catalog = self._gravitino_client.create_catalog(
            "iceberg",
            Catalog.Type.RELATIONAL,
            "lakehouse-iceberg",
            "",
            properties={
                "catalog-backend": "memory",
                "warehouse": warehouse,
                "uri": "http://localhost",
            },
        )
        catalog.as_schemas().create_schema("semantic", "", {})
        catalog.as_table_catalog().create_table(
            NameIdentifier.of("semantic", "orders"),
            [Column.of("id", Types.LongType.get())],
            properties={},
        )
        self._definition = SemanticModelDefinition(
            [Dataset("orders", NameIdentifier.of("iceberg", "semantic", "orders"))]
        )
        models = catalog.as_semantic_model_catalog()
        self._ident = NameIdentifier.of("semantic", "sales")
        models.create_semantic_model(self._ident, "sales", self._definition, {})
        models.create_semantic_model(
            NameIdentifier.of("semantic", "hidden"), "hidden", self._definition, {}
        )
        self._schema = SecurableObjects.of_schema(
            SecurableObjects.of_catalog("iceberg", []), "semantic", []
        )
        self._gravitino_client.add_user("semantic_reader")
        self._gravitino_client.create_role(
            "reader",
            securable_objects=[
                SecurableObjects.of_catalog(
                    "iceberg", [Privileges.allow("USE_CATALOG")]
                ),
                SecurableObjects.of_schema(
                    SecurableObjects.of_catalog("iceberg", []),
                    "semantic",
                    [Privileges.allow("USE_SCHEMA"), Privileges.allow("SELECT_TABLE")],
                ),
            ],
        )
        self._gravitino_client.grant_roles_to_user(["reader"], "semantic_reader")
        with patch.dict(os.environ, {"GRAVITINO_USER": "semantic_reader"}):
            reader = GravitinoClient(
                "http://localhost:8090",
                self._metalake_name,
                auth_data_provider=SimpleAuthProvider(),
            )
        self.addCleanup(reader.close)
        self._reader_models = reader.load_catalog("iceberg").as_semantic_model_catalog()

    def tearDown(self):
        self._gravitino_client.close()

    def test_grant_revoke_use_and_role_round_trip(self):
        with self.assertRaises(ForbiddenException):
            self._reader_models.load_semantic_model(self._ident)
        self.assertEqual(
            [], self._reader_models.list_semantic_models(Namespace.of("semantic"))
        )

        privilege = Privileges.allow("USE_SEMANTIC_MODEL")
        model = SecurableObjects.of_semantic_model(self._schema, "sales", [])
        role = self._gravitino_client.grant_privileges_to_role(
            "reader", model, [privilege]
        )
        self._assert_model_privileges(role, model, [privilege])
        self._assert_model_privileges(
            self._gravitino_client.get_role("reader"), model, [privilege]
        )
        bindings = MetadataObjectRoleOperations(
            self._metalake_name,
            model,
            self._gravitino_client.get_metalake().rest_client,
        )
        self.assertEqual(["reader"], bindings.list_binding_role_names())
        self.assertEqual(
            "sales", self._reader_models.load_semantic_model(self._ident).name()
        )
        self.assertEqual(
            ["sales"],
            [
                ident.name()
                for ident in self._reader_models.list_semantic_models(
                    Namespace.of("semantic")
                )
            ],
        )
        with self.assertRaises(ForbiddenException):
            self._reader_models.alter_semantic_model(
                self._ident, SemanticModelChange.update_comment("forbidden")
            )
        with self.assertRaises(ForbiddenException):
            self._reader_models.drop_semantic_model(self._ident)

        role = self._gravitino_client.revoke_privileges_from_role(
            "reader", model, [privilege]
        )
        self._assert_model_privileges(role, model, [])
        self.assertEqual([], bindings.list_binding_role_names())
        with self.assertRaises(ForbiddenException):
            self._reader_models.load_semantic_model(self._ident)
        self.assertEqual(
            [], self._reader_models.list_semantic_models(Namespace.of("semantic"))
        )

    def test_create_requires_privilege_and_parent_access(self):
        ident = NameIdentifier.of("semantic", "created")
        with self.assertRaises(ForbiddenException):
            self._reader_models.create_semantic_model(ident, "", self._definition, {})
        privilege = Privileges.allow("CREATE_SEMANTIC_MODEL")
        self._gravitino_client.grant_privileges_to_role(
            "reader", self._schema, [privilege]
        )
        for obj, required in (
            (
                SecurableObjects.of_catalog("iceberg", []),
                Privileges.allow("USE_CATALOG"),
            ),
            (self._schema, Privileges.allow("USE_SCHEMA")),
        ):
            with self.subTest(required=required.name()):
                self._gravitino_client.revoke_privileges_from_role(
                    "reader", obj, [required]
                )
                with self.assertRaises(ForbiddenException):
                    self._reader_models.create_semantic_model(
                        ident, "", self._definition, {}
                    )
                self._gravitino_client.grant_privileges_to_role(
                    "reader", obj, [required]
                )
        created = self._reader_models.create_semantic_model(
            ident, "", self._definition, {}
        )
        self.assertEqual("created", created.name())
        with self.assertRaises(ForbiddenException):
            self._reader_models.load_semantic_model(self._ident)
        self._gravitino_client.revoke_privileges_from_role(
            "reader", self._schema, [privilege]
        )
        with self.assertRaises(ForbiddenException):
            self._reader_models.create_semantic_model(
                NameIdentifier.of("semantic", "after_revoke"), "", self._definition, {}
            )

    def test_modify_allows_load_and_alter_but_not_drop(self):
        privilege = Privileges.allow("MODIFY_SEMANTIC_MODEL")
        model = SecurableObjects.of_semantic_model(self._schema, "sales", [privilege])
        role = self._gravitino_client.create_role("editor", securable_objects=[model])
        self._assert_model_privileges(role, model, [privilege])
        self._gravitino_client.grant_roles_to_user(["editor"], "semantic_reader")
        self.assertEqual(
            "sales", self._reader_models.load_semantic_model(self._ident).name()
        )
        updated = self._reader_models.alter_semantic_model(
            self._ident, SemanticModelChange.update_comment("updated")
        )
        self.assertEqual("updated", updated.comment())
        with self.assertRaises(ForbiddenException):
            self._reader_models.drop_semantic_model(self._ident)
        denied = Privileges.deny("MODIFY_SEMANTIC_MODEL")
        self._gravitino_client.grant_privileges_to_role("reader", model, [denied])
        with self.assertRaises(ForbiddenException):
            self._reader_models.alter_semantic_model(
                self._ident, SemanticModelChange.update_comment("denied")
            )
        self._gravitino_client.revoke_privileges_from_role("reader", model, [denied])
        self.assertEqual(
            "updated", self._reader_models.load_semantic_model(self._ident).comment()
        )
        self._gravitino_client.revoke_privileges_from_role("editor", model, [privilege])
        with self.assertRaises(ForbiddenException):
            self._reader_models.alter_semantic_model(
                self._ident, SemanticModelChange.update_comment("revoked")
            )

    def test_explicit_deny_overrides_inherited_use(self):
        allowed = Privileges.allow("USE_SEMANTIC_MODEL")
        denied = Privileges.deny("USE_SEMANTIC_MODEL")
        model = SecurableObjects.of_semantic_model(self._schema, "sales", [])
        self._gravitino_client.grant_privileges_to_role(
            "reader", self._schema, [allowed]
        )
        self.assertEqual(
            "sales", self._reader_models.load_semantic_model(self._ident).name()
        )
        role = self._gravitino_client.grant_privileges_to_role(
            "reader", model, [denied]
        )
        self._assert_model_privileges(role, model, [denied])
        with self.assertRaises(ForbiddenException):
            self._reader_models.load_semantic_model(self._ident)
        self.assertEqual(
            ["hidden"],
            [
                ident.name()
                for ident in self._reader_models.list_semantic_models(
                    Namespace.of("semantic")
                )
            ],
        )
        self._gravitino_client.revoke_privileges_from_role("reader", model, [denied])
        self.assertEqual(
            "sales", self._reader_models.load_semantic_model(self._ident).name()
        )

    def _assert_model_privileges(self, role, model, expected):
        actual = [
            privilege
            for obj in role.securable_objects()
            if obj.type() == model.type() and obj.full_name() == model.full_name()
            for privilege in obj.privileges()
        ]
        self.assertCountEqual(expected, actual)
