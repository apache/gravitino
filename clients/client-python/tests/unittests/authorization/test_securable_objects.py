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

import unittest

from gravitino.api.authorization.privileges import Privilege
from gravitino.api.authorization.securable_objects import SecurableObjects
from gravitino.api.metadata_object import MetadataObject
from tests.unittests.authorization.test_role_change import MockPrivilege


class TestSecurableObject(unittest.TestCase):
    def test_metalake_object(self) -> None:
        # metalake object
        metalake_privilege = MockPrivilege(
            Privilege.Name.CREATE_CATALOG, Privilege.Condition.ALLOW
        )
        metalake_object = SecurableObjects.of_metalake("metalake", [metalake_privilege])
        another_metalake_object = SecurableObjects.of(
            MetadataObject.Type.METALAKE,
            ["metalake"],
            [metalake_privilege],
        )

        self.assertEqual("metalake", metalake_object.full_name())
        self.assertEqual(MetadataObject.Type.METALAKE, metalake_object.type())
        self.assertEqual(metalake_object, another_metalake_object)

    def test_catalog_object(self) -> None:
        # Catalog object
        catalog_privilege = MockPrivilege(
            Privilege.Name.USE_CATALOG,
            Privilege.Condition.ALLOW,
        )
        catalog_object = SecurableObjects.of_catalog(
            "catalog",
            [catalog_privilege],
        )
        another_catalog_object = SecurableObjects.of(
            MetadataObject.Type.CATALOG,
            ["catalog"],
            [catalog_privilege],
        )

        self.assertEqual("catalog", catalog_object.full_name())
        self.assertEqual(MetadataObject.Type.CATALOG, catalog_object.type())
        self.assertEqual(catalog_object, another_catalog_object)

    def test_schema_object(self) -> None:
        catalog_object = SecurableObjects.of_catalog(
            "catalog",
            [
                MockPrivilege(
                    Privilege.Name.USE_CATALOG,
                    Privilege.Condition.ALLOW,
                )
            ],
        )
        # schema object
        schema_privilege = MockPrivilege(
            Privilege.Name.USE_SCHEMA,
            Privilege.Condition.ALLOW,
        )
        schema_object = SecurableObjects.of_schema(
            catalog_object,
            "schema",
            [schema_privilege],
        )
        another_schema_object = SecurableObjects.of(
            MetadataObject.Type.SCHEMA,
            ["catalog", "schema"],
            [schema_privilege],
        )

        self.assertEqual("catalog.schema", schema_object.full_name())
        self.assertEqual(MetadataObject.Type.SCHEMA, schema_object.type())
        self.assertEqual(schema_object, another_schema_object)

    def test_table_object(self) -> None:
        catalog_object = SecurableObjects.of_catalog(
            "catalog",
            [
                MockPrivilege(
                    Privilege.Name.USE_CATALOG,
                    Privilege.Condition.ALLOW,
                )
            ],
        )

        schema_object = SecurableObjects.of_schema(
            catalog_object,
            "schema",
            [
                MockPrivilege(
                    Privilege.Name.USE_SCHEMA,
                    Privilege.Condition.ALLOW,
                )
            ],
        )

        table_privilege = MockPrivilege(
            Privilege.Name.SELECT_TABLE,
            Privilege.Condition.ALLOW,
        )

        table_object = SecurableObjects.of_table(
            schema_object,
            "table",
            [table_privilege],
        )
        another_table_object = SecurableObjects.of(
            MetadataObject.Type.TABLE,
            ["catalog", "schema", "table"],
            [table_privilege],
        )

        self.assertEqual("catalog.schema.table", table_object.full_name())
        self.assertEqual(MetadataObject.Type.TABLE, table_object.type())
        self.assertEqual(table_object, another_table_object)

    def test_fileset_object(self) -> None:
        catalog_object = SecurableObjects.of_catalog(
            "catalog",
            [
                MockPrivilege(
                    Privilege.Name.USE_CATALOG,
                    Privilege.Condition.ALLOW,
                )
            ],
        )
        schema_object = SecurableObjects.of_schema(
            catalog_object,
            "schema",
            [
                MockPrivilege(
                    Privilege.Name.USE_SCHEMA,
                    Privilege.Condition.ALLOW,
                )
            ],
        )

        # fileset object
        fileset_privilege = MockPrivilege(
            Privilege.Name.USE_SCHEMA,
            Privilege.Condition.ALLOW,
        )
        fileset_object = SecurableObjects.of_fileset(
            schema_object,
            "fileset",
            [fileset_privilege],
        )

        another_fileset_object = SecurableObjects.of(
            MetadataObject.Type.FILESET,
            ["catalog", "schema", "fileset"],
            [fileset_privilege],
        )

        self.assertEqual("catalog.schema.fileset", fileset_object.full_name())
        self.assertEqual(MetadataObject.Type.FILESET, fileset_object.type())
        self.assertEqual(fileset_object, another_fileset_object)

    def test_topic_object(self) -> None:
        catalog_object = SecurableObjects.of_catalog(
            "catalog",
            [
                MockPrivilege(
                    Privilege.Name.USE_CATALOG,
                    Privilege.Condition.ALLOW,
                )
            ],
        )

        schema_object = SecurableObjects.of_schema(
            catalog_object,
            "schema",
            [
                MockPrivilege(
                    Privilege.Name.USE_SCHEMA,
                    Privilege.Condition.ALLOW,
                )
            ],
        )
        # Topic object
        topic_privilege = MockPrivilege(
            Privilege.Name.CONSUME_TOPIC,
            Privilege.Condition.ALLOW,
        )
        topic_object = SecurableObjects.of_topic(
            schema_object,
            "topic",
            [topic_privilege],
        )
        another_topic_object = SecurableObjects.of(
            MetadataObject.Type.TOPIC,
            ["catalog", "schema", "topic"],
            [topic_privilege],
        )

        self.assertEqual("catalog.schema.topic", topic_object.full_name())
        self.assertEqual(MetadataObject.Type.TOPIC, topic_object.type())
        self.assertEqual(topic_object, another_topic_object)

    def test_tag_object(self) -> None:
        tag_privilege = MockPrivilege(
            Privilege.Name.APPLY_TAG,
            Privilege.Condition.ALLOW,
        )
        tag_object = SecurableObjects.of_tag(
            "tag",
            [tag_privilege],
        )

        another_tag_object = SecurableObjects.of(
            MetadataObject.Type.TAG,
            ["tag"],
            [tag_privilege],
        )

        self.assertEqual("tag", tag_object.full_name())
        self.assertEqual(MetadataObject.Type.TAG, tag_object.type())
        self.assertEqual(tag_object, another_tag_object)

    def test_policy_object(self) -> None:
        policy_privilege = MockPrivilege(
            Privilege.Name.APPLY_POLICY,
            Privilege.Condition.ALLOW,
        )
        policy_object = SecurableObjects.of_policy(
            "policy",
            [policy_privilege],
        )

        another_policy_object = SecurableObjects.of(
            MetadataObject.Type.POLICY,
            ["policy"],
            [policy_privilege],
        )

        self.assertEqual("policy", policy_object.full_name())
        self.assertEqual(MetadataObject.Type.POLICY, policy_object.type())
        self.assertEqual(policy_object, another_policy_object)

    def test_job_object(self) -> None:
        job_tempalte_privilege = MockPrivilege(
            Privilege.Name.RUN_JOB,
            Privilege.Condition.ALLOW,
        )
        job_tempalte_object = SecurableObjects.of_job_template(
            "job_template", [job_tempalte_privilege]
        )

        another_job_tempalte_object = SecurableObjects.of(
            MetadataObject.Type.JOB_TEMPLATE,
            ["job_template"],
            [job_tempalte_privilege],
        )

        self.assertEqual("job_template", job_tempalte_object.full_name())
        self.assertEqual(MetadataObject.Type.JOB_TEMPLATE, job_tempalte_object.type())
        self.assertEqual(job_tempalte_object, another_job_tempalte_object)

    def test_metalake_object_with_error(self) -> None:
        metalake_privilege = MockPrivilege(
            Privilege.Name.CREATE_CATALOG, Privilege.Condition.ALLOW
        )
        with self.assertRaises(ValueError) as context:
            _ = SecurableObjects.of(
                MetadataObject.Type.METALAKE,
                ["metalake", "catalog"],
                metalake_privilege,
            )

            self.assertIn("the length of names must be 1", str(context.exception))

    def test_catalog_object_with_error(self) -> None:
        catalog_privilege = MockPrivilege(
            Privilege.Name.USE_CATALOG,
            Privilege.Condition.ALLOW,
        )

        with self.assertRaises(ValueError) as context:
            _ = SecurableObjects.of(
                MetadataObject.Type.CATALOG,
                ["metalake", "catalog"],
                catalog_privilege,
            )

            self.assertIn("the length of names must be 1", str(context.exception))

    def test_table_object_with_error(self) -> None:
        table_privilege = MockPrivilege(
            Privilege.Name.SELECT_TABLE,
            Privilege.Condition.ALLOW,
        )

        with self.assertRaises(ValueError) as context:
            _ = SecurableObjects.of(
                MetadataObject.Type.TABLE,
                ["catalog"],
                table_privilege,
            )

            self.assertIn("the length of names must be 3", str(context.exception))

    def test_topic_object_with_error(self) -> None:
        topic_privilege = MockPrivilege(
            Privilege.Name.CONSUME_TOPIC,
            Privilege.Condition.ALLOW,
        )

        with self.assertRaises(ValueError) as context:
            _ = SecurableObjects.of(
                MetadataObject.Type.TOPIC,
                ["metalake"],
                topic_privilege,
            )

            self.assertIn("the length of names must be 3", str(context.exception))

    def test_fileset_object_with_error(self) -> None:
        fileset_privilege = MockPrivilege(
            Privilege.Name.USE_SCHEMA,
            Privilege.Condition.ALLOW,
        )
        with self.assertRaises(ValueError) as context:
            _ = SecurableObjects.of(
                MetadataObject.Type.FILESET,
                ["metalake"],
                fileset_privilege,
            )

            self.assertIn("the length of names must be 3", str(context.exception))

    def test_schema_object_with_error(self) -> None:
        schema_privilege = MockPrivilege(
            Privilege.Name.USE_SCHEMA,
            Privilege.Condition.ALLOW,
        )
        with self.assertRaises(ValueError) as context:
            _ = SecurableObjects.of(
                MetadataObject.Type.SCHEMA,
                ["catalog", "schema", "table"],
                schema_privilege,
            )

            self.assertIn("the length of names must be 2", str(context.exception))
