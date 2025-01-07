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

from gravitino import GravitinoAdminClient, GravitinoClient, Catalog, NameIdentifier
from gravitino.exceptions.base import (
    ModelAlreadyExistsException,
    NoSuchSchemaException,
    NoSuchModelException,
    ModelVersionAliasesAlreadyExistException,
    NoSuchModelVersionException,
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
