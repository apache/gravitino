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
from random import randint

from gravitino import Catalog, GravitinoAdminClient, GravitinoClient, NameIdentifier
from gravitino.api.model_change import ModelChange
from gravitino.api.model_version_change import ModelVersionChange
from gravitino.exceptions.base import (
    ModelAlreadyExistsException,
    ModelVersionAliasesAlreadyExistException,
    NoSuchModelException,
    NoSuchModelVersionException,
    NoSuchSchemaException,
    NoSuchModelVersionURINameException,
)
from gravitino.namespace import Namespace
from tests.integration.integration_test_env import IntegrationTestEnv


class TestModelCatalog(IntegrationTestEnv):
    _metalake_name: str = "model_it_metalake" + str(randint(0, 1000))
    _catalog_name: str = "model_it_catalog" + str(randint(0, 1000))
    _schema_name: str = "model_it_schema" + str(randint(0, 1000))

    _gravitino_admin_client: GravitinoAdminClient = None
    _gravitino_client: GravitinoClient = None
    _catalog: Catalog = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        cls._gravitino_admin_client = GravitinoAdminClient(uri="http://localhost:8090")
        cls._gravitino_admin_client.create_metalake(
            cls._metalake_name, comment="comment", properties={}
        )

        cls._gravitino_client = GravitinoClient(
            uri="http://localhost:8090", metalake_name=cls._metalake_name
        )
        cls._catalog = cls._gravitino_client.create_catalog(
            name=cls._catalog_name,
            catalog_type=Catalog.Type.MODEL,
            provider=None,
            comment="comment",
            properties={},
        )

    @classmethod
    def tearDownClass(cls):
        cls._gravitino_client.drop_catalog(name=cls._catalog_name, force=True)
        cls._gravitino_admin_client.drop_metalake(name=cls._metalake_name, force=True)

        super().tearDownClass()

    def setUp(self):
        self._catalog.as_schemas().create_schema(self._schema_name, "comment", {})

    def tearDown(self):
        self._catalog.as_schemas().drop_schema(self._schema_name, True)

    def test_register_get_model(self):
        model_name = "model_it_model" + str(randint(0, 1000))
        model_ident = NameIdentifier.of(self._schema_name, model_name)
        comment = "comment"
        properties = {"k1": "v1", "k2": "v2"}

        model = self._catalog.as_model_catalog().register_model(
            model_ident, comment, properties
        )
        self.assertEqual(model_name, model.name())
        self.assertEqual(comment, model.comment())
        self.assertEqual(0, model.latest_version())
        self.assertEqual(properties, model.properties())

        # Test register model without comment and properties
        model = self._catalog.as_model_catalog().register_model(
            NameIdentifier.of(
                self._schema_name, model_name + "_no_comment_no_properties"
            ),
            comment=None,
            properties=None,
        )
        self.assertEqual(model_name + "_no_comment_no_properties", model.name())
        self.assertIsNone(model.comment())
        self.assertEqual(0, model.latest_version())
        self.assertEqual({}, model.properties())

        ## Test register same name model again
        with self.assertRaises(ModelAlreadyExistsException):
            self._catalog.as_model_catalog().register_model(
                model_ident, comment, properties
            )

        # Test register model in a non-existent schema
        with self.assertRaises(NoSuchSchemaException):
            self._catalog.as_model_catalog().register_model(
                NameIdentifier.of("non_existent_schema", model_name),
                comment,
                properties,
            )

        # Test get model
        model = self._catalog.as_model_catalog().get_model(model_ident)
        self.assertEqual(model_name, model.name())
        self.assertEqual(comment, model.comment())
        self.assertEqual(0, model.latest_version())
        self.assertEqual(properties, model.properties())

        # Test get non-existent model
        with self.assertRaises(NoSuchModelException):
            self._catalog.as_model_catalog().get_model(
                NameIdentifier.of(self._schema_name, "non_existent_model")
            )

        # Test get a model for non-existent schema
        with self.assertRaises(NoSuchModelException):
            self._catalog.as_model_catalog().get_model(
                NameIdentifier.of("non_existent_schema", model_name)
            )

    def test_register_list_models(self):
        model_name1 = "model_it_model1" + str(randint(0, 1000))
        model_name2 = "model_it_model2" + str(randint(0, 1000))
        model_ident1 = NameIdentifier.of(self._schema_name, model_name1)
        model_ident2 = NameIdentifier.of(self._schema_name, model_name2)
        comment = "comment"
        properties = {"k1": "v1", "k2": "v2"}

        self._catalog.as_model_catalog().register_model(
            model_ident1, comment, properties
        )
        self._catalog.as_model_catalog().register_model(
            model_ident2, comment, properties
        )

        models = self._catalog.as_model_catalog().list_models(
            Namespace.of(self._schema_name)
        )
        self.assertEqual(2, len(models))
        self.assertTrue(model_ident1 in models)
        self.assertTrue(model_ident2 in models)

        # Test delete and list models
        self.assertTrue(self._catalog.as_model_catalog().delete_model(model_ident1))
        models = self._catalog.as_model_catalog().list_models(
            Namespace.of(self._schema_name)
        )
        self.assertEqual(1, len(models))
        self.assertTrue(model_ident2 in models)

        self.assertTrue(self._catalog.as_model_catalog().delete_model(model_ident2))
        models = self._catalog.as_model_catalog().list_models(
            Namespace.of(self._schema_name)
        )
        self.assertEqual(0, len(models))

        # Test list models for non-existent schema
        with self.assertRaises(NoSuchSchemaException):
            self._catalog.as_model_catalog().list_models(
                Namespace.of("non_existent_schema")
            )

    def test_register_delete_model(self):
        model_name = "model_it_model" + str(randint(0, 1000))
        model_ident = NameIdentifier.of(self._schema_name, model_name)
        comment = "comment"
        properties = {"k1": "v1", "k2": "v2"}

        self._catalog.as_model_catalog().register_model(
            model_ident, comment, properties
        )
        self.assertTrue(self._catalog.as_model_catalog().delete_model(model_ident))
        # delete again will return False
        self.assertFalse(self._catalog.as_model_catalog().delete_model(model_ident))

        # Test delete model in non-existent schema
        self.assertFalse(
            self._catalog.as_model_catalog().delete_model(
                NameIdentifier.of("non_existent_schema", model_name)
            )
        )

        # Test delete non-existent model
        self.assertFalse(
            self._catalog.as_model_catalog().delete_model(
                NameIdentifier.of(self._schema_name, "non_existent_model")
            )
        )

    def test_register_rename_model(self):
        model_name = f"model_it_model{str(randint(0, 1000))}"
        model_new_name = f"model_it_model_new{str(randint(0, 1000))}"
        model_ident = NameIdentifier.of(self._schema_name, model_name)
        renamed_ident = NameIdentifier.of(self._schema_name, model_new_name)
        comment = "comment"
        properties = {"k1": "v1", "k2": "v2"}

        self._catalog.as_model_catalog().register_model(
            model_ident, comment, properties
        )

        renamed_model = self._catalog.as_model_catalog().get_model(model_ident)
        self.assertEqual(model_name, renamed_model.name())
        self.assertEqual(comment, renamed_model.comment())
        self.assertEqual(0, renamed_model.latest_version())
        self.assertEqual(properties, renamed_model.properties())

        changes = [ModelChange.rename(model_new_name)]

        self._catalog.as_model_catalog().alter_model(model_ident, *changes)
        renamed_model = self._catalog.as_model_catalog().get_model(renamed_ident)
        self.assertEqual(model_new_name, renamed_model.name())
        self.assertEqual(comment, renamed_model.comment())
        self.assertEqual(0, renamed_model.latest_version())
        self.assertEqual(properties, renamed_model.properties())

    def test_register_set_model_property(self):
        model_name = f"model_it_model{str(randint(0, 1000))}"
        model_ident = NameIdentifier.of(self._schema_name, model_name)
        comment = "comment"
        properties = {"k1": "v1", "k2": "v2"}
        self._catalog.as_model_catalog().register_model(
            model_ident, comment, properties
        )
        origin_model = self._catalog.as_model_catalog().get_model(model_ident)

        self.assertEqual(origin_model.name(), model_name)
        self.assertEqual(origin_model.comment(), comment)
        self.assertEqual(origin_model.latest_version(), 0)
        self.assertEqual(origin_model.properties(), properties)

        changes = [
            ModelChange.set_property("k1", "v11"),
            ModelChange.set_property("k3", "v3"),
        ]

        self._catalog.as_model_catalog().alter_model(model_ident, *changes)
        update_property_model = self._catalog.as_model_catalog().get_model(model_ident)

        self.assertEqual(update_property_model.name(), model_name)
        self.assertEqual(update_property_model.comment(), comment)
        self.assertEqual(update_property_model.latest_version(), 0)
        self.assertEqual(
            update_property_model.properties(), {"k1": "v11", "k2": "v2", "k3": "v3"}
        )

    def test_register_remove_model_property(self):
        model_name = f"model_it_model{str(randint(0, 1000))}"
        model_ident = NameIdentifier.of(self._schema_name, model_name)
        comment = "comment"
        properties = {"k1": "v1", "k2": "v2"}

        self._catalog.as_model_catalog().register_model(
            model_ident, comment, properties
        )
        origin_model = self._catalog.as_model_catalog().get_model(model_ident)
        self.assertEqual(origin_model.name(), model_name)
        self.assertEqual(origin_model.comment(), comment)
        self.assertEqual(origin_model.latest_version(), 0)
        self.assertEqual(origin_model.properties(), properties)

        changes = [ModelChange.remove_property("k1")]
        self._catalog.as_model_catalog().alter_model(model_ident, *changes)
        update_property_model = self._catalog.as_model_catalog().get_model(model_ident)
        self.assertEqual(update_property_model.name(), model_name)
        self.assertEqual(update_property_model.comment(), comment)
        self.assertEqual(update_property_model.latest_version(), 0)
        self.assertEqual(update_property_model.properties(), {"k2": "v2"})

    def test_register_update_model_comment(self):
        model_name = f"model_it_model{str(randint(0, 1000))}"
        model_ident = NameIdentifier.of(self._schema_name, model_name)
        comment = "comment"
        new_comment = "new comment"
        properties = {"k1": "v1", "k2": "v2"}
        self._catalog.as_model_catalog().register_model(
            model_ident, comment, properties
        )

        # Retrieve the original model
        origin_model = self._catalog.as_model_catalog().get_model(model_ident)
        self.assertEqual(origin_model.name(), model_name)
        self.assertEqual(origin_model.comment(), comment)
        self.assertEqual(origin_model.latest_version(), 0)
        self.assertEqual(origin_model.properties(), properties)

        # Alter model and validate the updated model
        changes = [ModelChange.update_comment(new_comment)]
        self._catalog.as_model_catalog().alter_model(model_ident, *changes)
        update_comment_model = self._catalog.as_model_catalog().get_model(model_ident)
        self.assertEqual(update_comment_model.name(), model_name)
        self.assertEqual(update_comment_model.comment(), new_comment)
        self.assertEqual(update_comment_model.latest_version(), 0)
        self.assertEqual(update_comment_model.properties(), properties)

    def test_link_update_model_version_comment(self):
        model_name = f"model_it_model{str(randint(0, 1000))}"
        model_ident = NameIdentifier.of(self._schema_name, model_name)
        self._catalog.as_model_catalog().register_model(model_ident, "comment", {})

        # Test link model version
        self._catalog.as_model_catalog().link_model_version(
            model_ident,
            uri="uri",
            aliases=["alias1", "alias2"],
            comment="comment",
            properties={"k1": "v1", "k2": "v2"},
        )

        model_version = self._catalog.as_model_catalog().get_model_version(
            model_ident, 0
        )
        self.assertEqual(0, model_version.version())
        self.assertEqual("uri", model_version.uri())
        self.assertEqual(["alias1", "alias2"], model_version.aliases())
        self.assertEqual("comment", model_version.comment())
        self.assertEqual({"k1": "v1", "k2": "v2"}, model_version.properties())

        model_version = self._catalog.as_model_catalog().get_model_version_by_alias(
            model_ident, "alias1"
        )
        self.assertEqual(0, model_version.version())
        self.assertEqual("uri", model_version.uri())

        model_version = self._catalog.as_model_catalog().get_model_version_by_alias(
            model_ident, "alias2"
        )
        self.assertEqual(0, model_version.version())
        self.assertEqual("uri", model_version.uri())

        changes = [ModelVersionChange.update_comment("new comment")]
        self._catalog.as_model_catalog().alter_model_version(model_ident, 0, *changes)
        updated_model_version = self._catalog.as_model_catalog().get_model_version(
            model_ident, 0
        )

        self.assertEqual(0, updated_model_version.version())
        self.assertEqual("new comment", updated_model_version.comment())
        self.assertEqual(["alias1", "alias2"], updated_model_version.aliases())
        self.assertEqual({"k1": "v1", "k2": "v2"}, updated_model_version.properties())
        self.assertEqual("uri", updated_model_version.uri())

    def test_link_update_model_version_property(self):
        model_name = f"model_it_model{str(randint(0, 1000))}"
        model_ident = NameIdentifier.of(self._schema_name, model_name)
        aliases = ["alias1", "alias2"]
        comment = "comment"
        properties = {"k1": "v1", "k2": "v2"}
        self._catalog.as_model_catalog().register_model(
            model_ident, comment, properties
        )

        self._catalog.as_model_catalog().link_model_version(
            model_ident,
            uri="uri",
            aliases=aliases,
            comment="comment",
            properties={"k1": "v1", "k2": "v2"},
        )

        original_model_version = self._catalog.as_model_catalog().get_model_version(
            model_ident, 0
        )

        self.assertEqual(0, original_model_version.version())
        self.assertEqual("uri", original_model_version.uri())
        self.assertEqual(["alias1", "alias2"], original_model_version.aliases())
        self.assertEqual("comment", original_model_version.comment())
        self.assertEqual({"k1": "v1", "k2": "v2"}, original_model_version.properties())

        changes = [
            ModelVersionChange.set_property("k1", "v11"),
            ModelVersionChange.set_property("k3", "v3"),
            ModelVersionChange.remove_property("k2"),
        ]

        self._catalog.as_model_catalog().alter_model_version(model_ident, 0, *changes)
        update_property_model = (
            self._catalog.as_model_catalog().get_model_version_by_alias(
                model_ident, aliases[0]
            )
        )

        self.assertEqual(update_property_model.version(), 0)
        self.assertEqual(update_property_model.uri(), "uri")
        self.assertEqual(update_property_model.comment(), comment)
        self.assertEqual(update_property_model.aliases(), aliases)
        self.assertEqual(update_property_model.properties(), {"k1": "v11", "k3": "v3"})

    def test_link_update_model_version_uri(self):
        model_name = f"model_it_model{str(randint(0, 1000))}"
        model_ident = NameIdentifier.of(self._schema_name, model_name)
        aliases = ["alias1", "alias2"]
        comment = "comment"
        properties = {"k1": "v1", "k2": "v2"}
        self._catalog.as_model_catalog().register_model(
            model_ident, comment, properties
        )
        self._catalog.as_model_catalog().link_model_version(
            model_ident,
            uri="uri",
            aliases=aliases,
            comment="comment",
            properties={"k1": "v1", "k2": "v2"},
        )

        original_model_version = self._catalog.as_model_catalog().get_model_version(
            model_ident, 0
        )

        self.assertEqual(0, original_model_version.version())
        self.assertEqual("uri", original_model_version.uri())
        self.assertEqual(["alias1", "alias2"], original_model_version.aliases())
        self.assertEqual("comment", original_model_version.comment())
        self.assertEqual({"k1": "v1", "k2": "v2"}, original_model_version.properties())

        changes = [ModelVersionChange.update_uri("new_uri")]
        self._catalog.as_model_catalog().alter_model_version(model_ident, 0, *changes)

        updated_model_version = self._catalog.as_model_catalog().get_model_version(
            model_ident, 0
        )
        self.assertEqual(0, updated_model_version.version())
        self.assertEqual("new_uri", updated_model_version.uri())
        self.assertEqual(["alias1", "alias2"], updated_model_version.aliases())
        self.assertEqual("comment", updated_model_version.comment())
        self.assertEqual({"k1": "v1", "k2": "v2"}, updated_model_version.properties())

    def test_link_add_model_version_uri(self):
        model_name = f"model_it_model{str(randint(0, 1000))}"
        model_ident = NameIdentifier.of(self._schema_name, model_name)
        aliases = ["alias1", "alias2"]
        comment = "comment"
        properties = {"k1": "v1", "k2": "v2"}
        self._catalog.as_model_catalog().register_model(
            model_ident, comment, properties
        )
        self._catalog.as_model_catalog().link_model_version_with_multiple_uris(
            model_ident,
            uris={"n1": "u1"},
            aliases=aliases,
            comment="comment",
            properties={"k1": "v1", "k2": "v2"},
        )

        original_model_version = self._catalog.as_model_catalog().get_model_version(
            model_ident, 0
        )

        self.assertEqual(0, original_model_version.version())
        self.assertEqual({"n1": "u1"}, original_model_version.uris())
        self.assertEqual(["alias1", "alias2"], original_model_version.aliases())
        self.assertEqual("comment", original_model_version.comment())
        self.assertEqual({"k1": "v1", "k2": "v2"}, original_model_version.properties())

        changes = [ModelVersionChange.add_uri("n2", "u2")]
        self._catalog.as_model_catalog().alter_model_version(model_ident, 0, *changes)

        updated_model_version = self._catalog.as_model_catalog().get_model_version(
            model_ident, 0
        )
        self.assertEqual(0, updated_model_version.version())
        self.assertEqual({"n1": "u1", "n2": "u2"}, updated_model_version.uris())
        self.assertEqual(["alias1", "alias2"], updated_model_version.aliases())
        self.assertEqual("comment", updated_model_version.comment())
        self.assertEqual({"k1": "v1", "k2": "v2"}, updated_model_version.properties())

    def test_link_remove_model_version_uri(self):
        model_name = f"model_it_model{str(randint(0, 1000))}"
        model_ident = NameIdentifier.of(self._schema_name, model_name)
        aliases = ["alias1", "alias2"]
        comment = "comment"
        properties = {"k1": "v1", "k2": "v2"}
        self._catalog.as_model_catalog().register_model(
            model_ident, comment, properties
        )
        self._catalog.as_model_catalog().link_model_version_with_multiple_uris(
            model_ident,
            uris={"n1": "u1", "n2": "u2"},
            aliases=aliases,
            comment="comment",
            properties={"k1": "v1", "k2": "v2"},
        )

        original_model_version = self._catalog.as_model_catalog().get_model_version(
            model_ident, 0
        )

        self.assertEqual(0, original_model_version.version())
        self.assertEqual({"n1": "u1", "n2": "u2"}, original_model_version.uris())
        self.assertEqual(["alias1", "alias2"], original_model_version.aliases())
        self.assertEqual("comment", original_model_version.comment())
        self.assertEqual({"k1": "v1", "k2": "v2"}, original_model_version.properties())

        changes = [ModelVersionChange.remove_uri("n1")]
        self._catalog.as_model_catalog().alter_model_version(model_ident, 0, *changes)

        updated_model_version = self._catalog.as_model_catalog().get_model_version(
            model_ident, 0
        )
        self.assertEqual(0, updated_model_version.version())
        self.assertEqual({"n2": "u2"}, updated_model_version.uris())
        self.assertEqual(["alias1", "alias2"], updated_model_version.aliases())
        self.assertEqual("comment", updated_model_version.comment())
        self.assertEqual({"k1": "v1", "k2": "v2"}, updated_model_version.properties())

    def test_link_update_model_version_aliases(self):
        model_name = f"model_it_model{str(randint(0, 1000))}"
        model_ident = NameIdentifier.of(self._schema_name, model_name)
        aliases = ["alias1", "alias2"]
        comment = "comment"
        properties = {"k1": "v1", "k2": "v2"}
        self._catalog.as_model_catalog().register_model(
            model_ident, comment, properties
        )
        self._catalog.as_model_catalog().link_model_version(
            model_ident,
            uri="uri",
            aliases=aliases,
            comment="comment",
            properties={"k1": "v1", "k2": "v2"},
        )

        original_model_version = self._catalog.as_model_catalog().get_model_version(
            model_ident, 0
        )

        self.assertEqual(0, original_model_version.version())
        self.assertEqual("uri", original_model_version.uri())
        self.assertEqual(["alias1", "alias2"], original_model_version.aliases())
        self.assertEqual("comment", original_model_version.comment())
        self.assertEqual({"k1": "v1", "k2": "v2"}, original_model_version.properties())

        # todo
        changes = [
            ModelVersionChange.update_aliases(
                ["alias2", "alias3"],
                ["alias1", "alias2"],
            )
        ]
        self._catalog.as_model_catalog().alter_model_version(model_ident, 0, *changes)

        updated_model_version = self._catalog.as_model_catalog().get_model_version(
            model_ident, 0
        )
        self.assertEqual(0, updated_model_version.version())
        self.assertEqual("uri", updated_model_version.uri())
        self.assertEqual(["alias2", "alias3"], updated_model_version.aliases())
        self.assertEqual("comment", updated_model_version.comment())
        self.assertEqual({"k1": "v1", "k2": "v2"}, updated_model_version.properties())

    def test_link_get_model_version(self):
        model_name = "model_it_model" + str(randint(0, 1000))
        model_ident = NameIdentifier.of(self._schema_name, model_name)
        self._catalog.as_model_catalog().register_model(model_ident, "comment", {})

        # Test link model version
        self._catalog.as_model_catalog().link_model_version(
            model_ident,
            uri="uri",
            aliases=["alias1", "alias2"],
            comment="comment",
            properties={"k1": "v1", "k2": "v2"},
        )

        # Test link model version to a non-existent model
        with self.assertRaises(NoSuchModelException):
            self._catalog.as_model_catalog().link_model_version(
                NameIdentifier.of(self._schema_name, "non_existent_model"),
                uri="uri",
                aliases=["alias1", "alias2"],
                comment="comment",
                properties={"k1": "v1", "k2": "v2"},
            )

        # Test link model version with existing aliases
        with self.assertRaises(ModelVersionAliasesAlreadyExistException):
            self._catalog.as_model_catalog().link_model_version(
                model_ident,
                uri="uri",
                aliases=["alias1", "alias2"],
                comment="comment",
                properties={"k1": "v1", "k2": "v2"},
            )

        model_version = self._catalog.as_model_catalog().get_model_version(
            model_ident, 0
        )
        self.assertEqual(0, model_version.version())
        self.assertEqual("uri", model_version.uri())
        self.assertEqual(["alias1", "alias2"], model_version.aliases())
        self.assertEqual("comment", model_version.comment())
        self.assertEqual({"k1": "v1", "k2": "v2"}, model_version.properties())

        model_version = self._catalog.as_model_catalog().get_model_version_by_alias(
            model_ident, "alias1"
        )
        self.assertEqual(0, model_version.version())
        self.assertEqual("uri", model_version.uri())

        model_version = self._catalog.as_model_catalog().get_model_version_by_alias(
            model_ident, "alias2"
        )
        self.assertEqual(0, model_version.version())
        self.assertEqual("uri", model_version.uri())

        # Test get model version from non-existent model
        with self.assertRaises(NoSuchModelVersionException):
            self._catalog.as_model_catalog().get_model_version(
                NameIdentifier.of(self._schema_name, "non_existent_model"), 0
            )

        with self.assertRaises(NoSuchModelVersionException):
            self._catalog.as_model_catalog().get_model_version_by_alias(
                NameIdentifier.of(self._schema_name, "non_existent_model"), "alias1"
            )

        # Test get non-existent model version
        with self.assertRaises(NoSuchModelVersionException):
            self._catalog.as_model_catalog().get_model_version(model_ident, 1)

        with self.assertRaises(NoSuchModelVersionException):
            self._catalog.as_model_catalog().get_model_version_by_alias(
                model_ident, "non_existent_alias"
            )

        # Test link model version with None aliases, comment and properties
        self._catalog.as_model_catalog().link_model_version(
            model_ident, uri="uri", aliases=None, comment=None, properties=None
        )
        model_version = self._catalog.as_model_catalog().get_model_version(
            model_ident, 1
        )
        self.assertEqual(1, model_version.version())
        self.assertEqual("uri", model_version.uri())
        self.assertEqual([], model_version.aliases())
        self.assertIsNone(model_version.comment())
        self.assertEqual({}, model_version.properties())

    def test_link_list_model_versions(self):
        model_name = "model_it_model" + str(randint(0, 1000))
        model_ident = NameIdentifier.of(self._schema_name, model_name)
        self._catalog.as_model_catalog().register_model(model_ident, "comment", {})

        # Test link model versions
        self._catalog.as_model_catalog().link_model_version(
            model_ident,
            uri="uri1",
            aliases=["alias1", "alias2"],
            comment="comment",
            properties={"k1": "v1", "k2": "v2"},
        )

        self._catalog.as_model_catalog().link_model_version(
            model_ident,
            uri="uri2",
            aliases=["alias3", "alias4"],
            comment="comment",
            properties={"k1": "v1", "k2": "v2"},
        )

        model_versions = self._catalog.as_model_catalog().list_model_versions(
            model_ident
        )
        self.assertEqual(2, len(model_versions))
        self.assertTrue(0 in model_versions)
        self.assertTrue(1 in model_versions)

        # Test delete model version
        self.assertTrue(
            self._catalog.as_model_catalog().delete_model_version(model_ident, 0)
        )
        model_versions = self._catalog.as_model_catalog().list_model_versions(
            model_ident
        )
        self.assertEqual(1, len(model_versions))
        self.assertTrue(1 in model_versions)

        self.assertTrue(
            self._catalog.as_model_catalog().delete_model_version(model_ident, 1)
        )
        model_versions = self._catalog.as_model_catalog().list_model_versions(
            model_ident
        )
        self.assertEqual(0, len(model_versions))

        # Test list model versions for non-existent model
        with self.assertRaises(NoSuchModelException):
            self._catalog.as_model_catalog().list_model_versions(
                NameIdentifier.of(self._schema_name, "non_existent_model")
            )

    def test_link_list_model_version_infos(self):
        model_name = "model_it_model" + str(randint(0, 1000))
        model_ident = NameIdentifier.of(self._schema_name, model_name)
        self._catalog.as_model_catalog().register_model(model_ident, "comment", {})

        self._catalog.as_model_catalog().link_model_version(
            model_ident,
            uri="uri1",
            aliases=["alias1", "alias2"],
            comment="comment",
            properties={"k1": "v1", "k2": "v2"},
        )

        model_versions = self._catalog.as_model_catalog().list_model_version_infos(
            model_ident
        )
        self.assertEqual(1, len(model_versions))
        self.assertEqual(model_versions[0].version(), 0)
        self.assertEqual(model_versions[0].uri(), "uri1")
        self.assertEqual(model_versions[0].comment(), "comment")
        self.assertEqual(model_versions[0].aliases(), ["alias1", "alias2"])
        self.assertEqual(model_versions[0].properties(), {"k1": "v1", "k2": "v2"})

        self.assertTrue(
            self._catalog.as_model_catalog().delete_model_version(model_ident, 0)
        )
        model_versions = self._catalog.as_model_catalog().list_model_version_infos(
            model_ident
        )
        self.assertEqual(0, len(model_versions))

        with self.assertRaises(NoSuchModelException):
            self._catalog.as_model_catalog().list_model_version_infos(
                NameIdentifier.of(self._schema_name, "non_existent_model")
            )

    def test_link_model_version_with_multiple_uris(self):
        model_name = "model_it_model" + str(randint(0, 1000))
        model_ident = NameIdentifier.of(self._schema_name, model_name)
        self._catalog.as_model_catalog().register_model(model_ident, "comment", {})

        # Test link model version
        self._catalog.as_model_catalog().link_model_version_with_multiple_uris(
            model_ident,
            uris={"n1": "u1", "n2": "u2"},
            aliases=["alias1", "alias2"],
            comment="comment",
            properties={"k1": "v1", "k2": "v2"},
        )

        # Test get model version
        model_version = self._catalog.as_model_catalog().get_model_version(
            model_ident, 0
        )
        self.assertEqual(0, model_version.version())
        self.assertEqual({"n1": "u1", "n2": "u2"}, model_version.uris())
        self.assertEqual(["alias1", "alias2"], model_version.aliases())
        self.assertEqual("comment", model_version.comment())
        self.assertEqual({"k1": "v1", "k2": "v2"}, model_version.properties())

        # Test get model version by alias
        model_version = self._catalog.as_model_catalog().get_model_version_by_alias(
            model_ident, "alias1"
        )
        self.assertEqual(0, model_version.version())
        self.assertEqual({"n1": "u1", "n2": "u2"}, model_version.uris())

        model_version = self._catalog.as_model_catalog().get_model_version_by_alias(
            model_ident, "alias2"
        )
        self.assertEqual(0, model_version.version())
        self.assertEqual({"n1": "u1", "n2": "u2"}, model_version.uris())

        # Test list model versions
        model_versions = self._catalog.as_model_catalog().list_model_versions(
            model_ident
        )
        self.assertEqual(1, len(model_versions))
        self.assertTrue(0 in model_versions)

        # Test list model version infos
        model_versions = self._catalog.as_model_catalog().list_model_versions(
            model_ident
        )
        self.assertEqual(1, len(model_versions))
        self.assertTrue(0 in model_versions)
        model_versions = self._catalog.as_model_catalog().list_model_version_infos(
            model_ident
        )
        self.assertEqual(1, len(model_versions))
        self.assertEqual(0, model_versions[0].version())
        self.assertEqual({"n1": "u1", "n2": "u2"}, model_versions[0].uris())
        self.assertEqual("comment", model_versions[0].comment())
        self.assertEqual(["alias1", "alias2"], model_versions[0].aliases())
        self.assertEqual({"k1": "v1", "k2": "v2"}, model_versions[0].properties())

    def test_get_model_version_uri(self):
        model_name = "model_it_model" + str(randint(0, 1000))
        model_ident = NameIdentifier.of(self._schema_name, model_name)
        self._catalog.as_model_catalog().register_model(model_ident, "comment", {})

        # link model version
        self._catalog.as_model_catalog().link_model_version_with_multiple_uris(
            model_ident,
            uris={"n1": "u1", "n2": "u2"},
            aliases=["alias1", "alias2"],
            comment="comment",
            properties={"k1": "v1", "k2": "v2"},
        )

        # Test get model version
        model_version = self._catalog.as_model_catalog().get_model_version(
            model_ident, 0
        )
        self.assertEqual(0, model_version.version())
        self.assertEqual({"n1": "u1", "n2": "u2"}, model_version.uris())
        self.assertEqual(["alias1", "alias2"], model_version.aliases())
        self.assertEqual("comment", model_version.comment())
        self.assertEqual({"k1": "v1", "k2": "v2"}, model_version.properties())

        # Test get model version uri
        model_version_uri = self._catalog.as_model_catalog().get_model_version_uri(
            model_ident, 0, "n1"
        )
        self.assertEqual("u1", model_version_uri)
        model_version_uri = self._catalog.as_model_catalog().get_model_version_uri(
            model_ident, 0, "n2"
        )
        self.assertEqual("u2", model_version_uri)
        with self.assertRaises(NoSuchModelVersionURINameException):
            self._catalog.as_model_catalog().get_model_version_uri(model_ident, 0, "n3")

        # Test get model version uri by alias
        model_version_uri = (
            self._catalog.as_model_catalog().get_model_version_uri_by_alias(
                model_ident, "alias1", "n1"
            )
        )
        self.assertEqual("u1", model_version_uri)
        model_version_uri = (
            self._catalog.as_model_catalog().get_model_version_uri_by_alias(
                model_ident, "alias1", "n2"
            )
        )
        self.assertEqual("u2", model_version_uri)
        with self.assertRaises(NoSuchModelVersionURINameException):
            self._catalog.as_model_catalog().get_model_version_uri_by_alias(
                model_ident, "alias1", "n3"
            )

    def test_link_delete_model_version(self):
        model_name = "model_it_model" + str(randint(0, 1000))
        model_ident = NameIdentifier.of(self._schema_name, model_name)
        self._catalog.as_model_catalog().register_model(model_ident, "comment", {})

        self._catalog.as_model_catalog().link_model_version(
            model_ident,
            uri="uri",
            aliases=["alias1"],
            comment="comment",
            properties={"k1": "v1", "k2": "v2"},
        )

        self.assertTrue(
            self._catalog.as_model_catalog().delete_model_version(model_ident, 0)
        )
        self.assertFalse(
            self._catalog.as_model_catalog().delete_model_version(model_ident, 0)
        )
        self.assertFalse(
            self._catalog.as_model_catalog().delete_model_version_by_alias(
                model_ident, "alias1"
            )
        )

        self._catalog.as_model_catalog().link_model_version(
            model_ident,
            uri="uri",
            aliases=["alias2"],
            comment="comment",
            properties={"k1": "v1", "k2": "v2"},
        )

        self.assertTrue(
            self._catalog.as_model_catalog().delete_model_version_by_alias(
                model_ident, "alias2"
            )
        )
        self.assertFalse(
            self._catalog.as_model_catalog().delete_model_version_by_alias(
                model_ident, "alias2"
            )
        )
        self.assertFalse(
            self._catalog.as_model_catalog().delete_model_version(model_ident, 1)
        )

        # Test delete model version for non-existent model
        self.assertFalse(
            self._catalog.as_model_catalog().delete_model_version(
                NameIdentifier.of(self._schema_name, "non_existent_model"), 0
            )
        )

        self.assertFalse(
            self._catalog.as_model_catalog().delete_model_version_by_alias(
                NameIdentifier.of(self._schema_name, "non_existent_model"), "alias1"
            )
        )
