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
import os
import shutil
import tempfile
import uuid
from unittest.mock import patch

from gravitino import Catalog, GravitinoClient, NameIdentifier
from gravitino.api.authorization.owner import Owner
from gravitino.api.authorization.privileges import Privileges
from gravitino.api.authorization.securable_objects import SecurableObjects
from gravitino.api.metadata_object import MetadataObject
from gravitino.api.metadata_objects import MetadataObjects
from gravitino.api.rel.column import Column
from gravitino.api.rel.types.types import Types
from gravitino.api.semantic.dataset import Dataset
from gravitino.api.semantic.semantic_model_change import SemanticModelChange
from gravitino.api.semantic.semantic_model_definition import SemanticModelDefinition
from gravitino.auth.simple_auth_provider import SimpleAuthProvider
from gravitino.exceptions.base import (
    ForbiddenException,
    NoSuchMetadataObjectException,
    NoSuchTagException,
    NotFoundException,
    TagAlreadyAssociatedException,
)
from tests.integration.integration_test_env import AuthorizationIntegrationTestEnv


class TestSemanticModelTagsAndOwner(AuthorizationIntegrationTestEnv):
    """Exercise Semantic Model tags and generic ownership with authorization enabled."""

    def setUp(self):
        self._metalake_name = f"semantic_tags_{uuid.uuid4().hex[:8]}"
        super().setUp()
        self.addCleanup(self._gravitino_client.close)
        warehouse = tempfile.mkdtemp(prefix="semantic_tags_")
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
        definition = SemanticModelDefinition(
            [Dataset("orders", NameIdentifier.of("iceberg", "semantic", "orders"))]
        )
        self._models = catalog.as_semantic_model_catalog()
        self._ident = NameIdentifier.of("semantic", "sales")
        self._model = self._models.create_semantic_model(
            self._ident, "", definition, {}
        )
        self._gravitino_client.create_tag(
            "reviewed", "review status", {"team": "finance"}
        )
        self._gravitino_client.create_tag("hidden", "", {})
        self._object = MetadataObjects.of(
            ["iceberg", "semantic", "sales"], MetadataObject.Type.SEMANTIC_MODEL
        )

    def tearDown(self):
        # Registered cleanups also run when setup fails.
        pass

    def test_tag_lifecycle(self):
        tags = self._model.supports_tags()
        self.assertEqual([], tags.list_tags())
        self.assertEqual([], tags.list_tags_info())
        self.assertEqual(["reviewed"], tags.associate_tags(["reviewed"], []))
        loaded = self._models.load_semantic_model(self._ident).supports_tags()
        self.assertEqual(["reviewed"], loaded.list_tags())
        details = loaded.list_tags_info()
        self.assertEqual(["reviewed"], [tag.name() for tag in details])
        tag = loaded.get_tag("reviewed")
        self.assertEqual("review status", tag.comment())
        self.assertEqual({"team": "finance"}, tag.properties())
        self.assertEqual(tag.name(), details[0].name())
        self.assertEqual(
            [(self._object.type(), self._object.full_name())],
            [
                (obj.type(), obj.full_name())
                for obj in tag.associated_objects().objects()
            ],
        )

        renamed = self._models.alter_semantic_model(
            self._ident, SemanticModelChange.rename("renamed")
        ).supports_tags()
        self.assertEqual(["reviewed"], renamed.list_tags())
        self.assertEqual([], renamed.associate_tags([], ["reviewed"]))
        self.assertEqual([], renamed.list_tags_info())

    def test_imported_model_tag_context(self):
        document = json.loads(self._models.export_ossie_document(self._ident, "json"))
        document["name"] = "imported"
        imported = self._models.import_ossie_document(
            self._ident.namespace(), json.dumps(document), "json"
        )
        tags = imported.supports_tags()
        self.assertEqual(["reviewed"], tags.associate_tags(["reviewed"], []))
        self.assertEqual(["reviewed"], tags.list_tags())
        self.assertEqual("reviewed", tags.get_tag("reviewed").name())
        self.assertEqual([], self._model.supports_tags().list_tags())
        self.assertEqual([], tags.associate_tags([], ["reviewed"]))

    def test_tag_values(self):
        tags = self._model.supports_tags()
        self.assertEqual(
            ["reviewed"], tags.assign_tags([{"name": "reviewed", "value": "yes"}])
        )
        self.assertEqual(["reviewed"], tags.list_tags())
        self.assertEqual(
            [], tags.assign_tags(tags_to_remove=[{"name": "reviewed", "value": "yes"}])
        )
        self.assertEqual([], tags.list_tags())

    def test_tag_errors(self):
        tags = self._model.supports_tags()
        for name in ("missing", "reviewed"):
            with self.subTest(tag=name), self.assertRaises(NoSuchTagException):
                tags.get_tag(name)
        tags.associate_tags(["reviewed"], [])
        with self.assertRaises(TagAlreadyAssociatedException):
            tags.associate_tags(["reviewed"], [])
        self.assertEqual(["reviewed"], tags.list_tags())
        # Missing tag additions/removals are ignored by the existing tag API.
        self.assertEqual(["reviewed"], tags.associate_tags(["missing"], ["absent"]))
        tags.associate_tags([], ["reviewed"])
        with self.assertRaises(NoSuchTagException):
            tags.get_tag("reviewed")
        self._models.drop_semantic_model(self._ident)
        with self.assertRaises(NotFoundException):
            tags.list_tags()

    def test_tag_authorization(self):
        reader = self._reader()
        model_privilege = Privileges.allow("USE_SEMANTIC_MODEL")
        self._grant(self._object, model_privilege)
        tags = (
            reader.load_catalog("iceberg")
            .as_semantic_model_catalog()
            .load_semantic_model(self._ident)
            .supports_tags()
        )
        self._model.supports_tags().associate_tags(["reviewed", "hidden"], [])
        self.assertEqual([], tags.list_tags())
        self.assertEqual([], tags.list_tags_info())
        with self.assertRaises(ForbiddenException):
            tags.get_tag("reviewed")
        with self.assertRaises(ForbiddenException):
            tags.associate_tags([], ["reviewed"])

        tag_object = SecurableObjects.of_tag("reviewed", [])
        self._grant(tag_object, Privileges.allow("VIEW_TAG"))
        self.assertEqual(["reviewed"], tags.list_tags())
        self.assertEqual(["reviewed"], [tag.name() for tag in tags.list_tags_info()])
        self.assertEqual("reviewed", tags.get_tag("reviewed").name())
        with self.assertRaises(ForbiddenException):
            tags.associate_tags([], ["reviewed"])
        apply = Privileges.allow("APPLY_TAG")
        self._grant(tag_object, apply)
        tags.associate_tags([], ["reviewed"])
        tags.associate_tags(["reviewed"], [])
        self.assertEqual(["reviewed"], tags.list_tags())

        denied = Privileges.deny("APPLY_TAG")
        metalake = SecurableObjects.of_metalake(self._metalake_name, [])
        self._grant(metalake, denied)
        with self.assertRaises(ForbiddenException):
            tags.associate_tags([], ["reviewed"])
        self._revoke(metalake, denied)
        self._revoke(tag_object, apply)
        with self.assertRaises(ForbiddenException):
            tags.associate_tags([], ["reviewed"])
        self._grant(tag_object, apply)
        self._revoke(self._object, model_privilege)
        for operation in (
            tags.list_tags,
            tags.list_tags_info,
            lambda: tags.get_tag("reviewed"),
            lambda: tags.associate_tags([], ["reviewed"]),
            lambda: tags.assign_tags(tags_to_remove=["reviewed"]),
        ):
            with self.subTest(operation=operation), self.assertRaises(
                ForbiddenException
            ):
                operation()
        self.assertCountEqual(
            ["reviewed", "hidden"], self._model.supports_tags().list_tags()
        )

    def test_owner_assignment_and_retrieval(self):
        owner = self._gravitino_client.get_owner(self._object)
        self.assertEqual("anonymous", owner.name())
        self.assertEqual(Owner.Type.USER, owner.type())
        self._gravitino_client.add_user("model_owner")
        self._gravitino_client.add_group("model_owners")
        for name, owner_type in (
            ("model_owner", Owner.Type.USER),
            ("model_owners", Owner.Type.GROUP),
        ):
            with self.subTest(owner_type=owner_type):
                self._gravitino_client.set_owner(self._object, name, owner_type)
                owner = self._gravitino_client.get_owner(self._object)
                self.assertEqual(name, owner.name())
                self.assertEqual(owner_type, owner.type())

    def test_owner_errors(self):
        for owner_type in (Owner.Type.USER, Owner.Type.GROUP):
            with self.subTest(owner_type=owner_type), self.assertRaises(
                NotFoundException
            ):
                self._gravitino_client.set_owner(self._object, "missing", owner_type)
        self.assertEqual(
            "anonymous", self._gravitino_client.get_owner(self._object).name()
        )
        self._gravitino_client.add_user("model_owner")
        self._models.drop_semantic_model(self._ident)
        with self.assertRaises(NoSuchMetadataObjectException):
            self._gravitino_client.get_owner(self._object)
        with self.assertRaises(NoSuchMetadataObjectException):
            self._gravitino_client.set_owner(
                self._object, "model_owner", Owner.Type.USER
            )

    def test_owner_authorization(self):
        reader = self._reader()
        with self.assertRaises(ForbiddenException):
            reader.get_owner(self._object)
        with self.assertRaises(ForbiddenException):
            reader.set_owner(self._object, "semantic_reader", Owner.Type.USER)
        privilege = Privileges.allow("USE_SEMANTIC_MODEL")
        self._grant(self._object, privilege)
        self.assertEqual("anonymous", reader.get_owner(self._object).name())
        with self.assertRaises(ForbiddenException):
            reader.set_owner(self._object, "semantic_reader", Owner.Type.USER)
        self._revoke(self._object, privilege)
        self._gravitino_client.set_owner(
            self._object, "semantic_reader", Owner.Type.USER
        )
        self.assertEqual("semantic_reader", reader.get_owner(self._object).name())
        models = reader.load_catalog("iceberg").as_semantic_model_catalog()
        self.assertEqual(
            "owner edit",
            models.alter_semantic_model(
                self._ident, SemanticModelChange.update_comment("owner edit")
            ).comment(),
        )
        self._gravitino_client.add_user("next_owner")
        reader.set_owner(self._object, "next_owner", Owner.Type.USER)
        self.assertEqual(
            "next_owner", self._gravitino_client.get_owner(self._object).name()
        )
        with self.assertRaises(ForbiddenException):
            reader.get_owner(self._object)
        with self.assertRaises(ForbiddenException):
            models.alter_semantic_model(
                self._ident, SemanticModelChange.update_comment("denied")
            )

    def _reader(self):
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
                    [Privileges.allow("USE_SCHEMA")],
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
        return reader

    def _grant(self, obj, privilege):
        self._gravitino_client.grant_privileges_to_role("reader", obj, [privilege])

    def _revoke(self, obj, privilege):
        self._gravitino_client.revoke_privileges_from_role("reader", obj, [privilege])
