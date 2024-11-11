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

import logging
from typing import Dict, List

from gravitino import GravitinoAdminClient, GravitinoMetalake, MetalakeChange
from gravitino.dto.dto_converters import DTOConverters
from gravitino.dto.requests.metalake_updates_request import MetalakeUpdatesRequest
from gravitino.exceptions.base import (
    GravitinoRuntimeException,
    NoSuchMetalakeException,
    MetalakeAlreadyExistsException,
)
from tests.integration.integration_test_env import IntegrationTestEnv

logger = logging.getLogger(__name__)


class TestMetalake(IntegrationTestEnv):
    metalake_name: str = "TestMetalake_metalake"
    metalake_new_name = metalake_name + "_new"

    metalake_comment: str = "metalake_comment"
    metalake_properties_key1: str = "metalake_properties_key1"
    metalake_properties_value1: str = "metalake_properties_value1"
    metalake_properties_key2: str = "metalake_properties_key2"
    metalake_properties_value2: str = "metalake_properties_value2"
    metalake_properties: Dict[str, str] = {
        metalake_properties_key1: metalake_properties_value1,
        metalake_properties_key2: metalake_properties_value2,
    }

    gravitino_admin_client: GravitinoAdminClient = GravitinoAdminClient(
        uri="http://localhost:8090"
    )

    def tearDown(self):
        self.clean_test_data()

    def clean_test_data(self):
        try:
            logger.info(
                "Drop metalake %s[%s]",
                self.metalake_name,
                self.drop_metalake(self.metalake_name),
            )
        except GravitinoRuntimeException:
            logger.warning("Failed to drop metalake %s", self.metalake_name)

        try:
            logger.info(
                "Drop metalake %s[%s]",
                self.metalake_new_name,
                self.drop_metalake(self.metalake_new_name),
            )
        except GravitinoRuntimeException:
            logger.warning("Failed to drop metalake %s", self.metalake_new_name)

    def test_create_metalake(self):
        metalake = self.create_metalake(self.metalake_name)
        self.assertEqual(metalake.name(), self.metalake_name)
        self.assertEqual(metalake.comment(), self.metalake_comment)
        self.assertEqual(metalake.properties(), self.metalake_properties)
        self.assertEqual(metalake.audit_info().creator(), "anonymous")

    def create_metalake(self, metalake_name) -> GravitinoMetalake:
        return self.gravitino_admin_client.create_metalake(
            metalake_name,
            self.metalake_comment,
            self.metalake_properties,
        )

    def test_failed_create_metalake(self):
        self.create_metalake(self.metalake_name)
        with self.assertRaises(MetalakeAlreadyExistsException):
            _ = self.create_metalake(self.metalake_name)

    def test_nullable_comment_metalake(self):
        self.create_metalake(self.metalake_name)
        changes = (MetalakeChange.update_comment(None),)
        null_comment_metalake = self.gravitino_admin_client.alter_metalake(
            self.metalake_name, *changes
        )
        self.assertIsNone(null_comment_metalake.comment())

    def test_alter_metalake(self):
        self.create_metalake(self.metalake_name)

        metalake_new_name = self.metalake_name + "_new"
        metalake_new_comment = self.metalake_comment + "_new"
        metalake_properties_new_value: str = "metalake_properties_new_value1"

        changes = (
            MetalakeChange.rename(metalake_new_name),
            MetalakeChange.update_comment(metalake_new_comment),
            MetalakeChange.remove_property(self.metalake_properties_key1),
            MetalakeChange.set_property(
                self.metalake_properties_key2, metalake_properties_new_value
            ),
        )

        metalake = self.gravitino_admin_client.alter_metalake(
            self.metalake_name, *changes
        )
        self.assertEqual(metalake.name(), metalake_new_name)
        self.assertEqual(metalake.comment(), metalake_new_comment)
        self.assertEqual(
            metalake.properties().get(self.metalake_properties_key2),
            metalake_properties_new_value,
        )
        self.assertTrue(self.metalake_properties_key1 not in metalake.properties())

    def drop_metalake(self, metalake_name: str) -> bool:
        return self.gravitino_admin_client.drop_metalake(metalake_name, True)

    def test_drop_metalake(self):
        self.create_metalake(self.metalake_name)
        self.assertTrue(self.drop_metalake(self.metalake_name))

    def test_metalake_update_request_to_json(self):
        changes = (
            MetalakeChange.rename("my_metalake_new"),
            MetalakeChange.update_comment("new metalake comment"),
        )
        reqs = [DTOConverters.to_metalake_update_request(change) for change in changes]
        updates_request = MetalakeUpdatesRequest(reqs)
        valid_json = (
            '{"updates": [{"@type": "rename", "newName": "my_metalake_new"}, '
            '{"@type": "updateComment", "newComment": "new metalake comment"}]}'
        )
        self.assertEqual(updates_request.to_json(), valid_json)

    def test_list_metalakes(self):
        self.create_metalake(self.metalake_name)
        metalake_list: List[GravitinoMetalake] = (
            self.gravitino_admin_client.list_metalakes()
        )
        self.assertTrue(
            any(item.name() == self.metalake_name for item in metalake_list)
        )

    def test_load_metalakes(self):
        self.create_metalake(self.metalake_name)
        metalake = self.gravitino_admin_client.load_metalake(self.metalake_name)
        self.assertIsNotNone(metalake)
        self.assertEqual(metalake.name(), self.metalake_name)
        self.assertEqual(metalake.comment(), self.metalake_comment)
        self.assertEqual(
            metalake.properties(), {**self.metalake_properties, "in-use": "true"}
        )
        self.assertEqual(metalake.audit_info().creator(), "anonymous")

    def test_failed_load_metalakes(self):
        with self.assertRaises(NoSuchMetalakeException):
            _ = self.gravitino_admin_client.load_metalake(self.metalake_name)
