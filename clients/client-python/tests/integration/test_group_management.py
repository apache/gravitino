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
import uuid

from gravitino import GravitinoAdminClient, GravitinoClient
from gravitino.exceptions.base import (
    GroupAlreadyExistsException,
    NoSuchGroupException,
)
from tests.integration.integration_test_env import IntegrationTestEnv

logger = logging.getLogger(__name__)


class TestGroupManagement(IntegrationTestEnv):
    _metalake_name: str = f"test_group_metalake_{uuid.uuid4().hex[:8]}"
    _gravitino_admin_client: GravitinoAdminClient = GravitinoAdminClient(
        uri="http://localhost:8090"
    )
    _gravitino_client: GravitinoClient

    def setUp(self):
        self._gravitino_admin_client.create_metalake(
            self._metalake_name, comment="test group management", properties={}
        )
        self._gravitino_client = GravitinoClient(
            uri="http://localhost:8090", metalake_name=self._metalake_name
        )

    def tearDown(self):
        try:
            self._gravitino_admin_client.drop_metalake(self._metalake_name, force=True)
        except Exception:  # pylint: disable=broad-except
            logger.warning("Failed to drop metalake %s", self._metalake_name)

    def test_add_and_get_group(self):
        added = self._gravitino_client.add_group("engineers")
        self.assertEqual("engineers", added.name())

        retrieved = self._gravitino_client.get_group("engineers")
        self.assertEqual("engineers", retrieved.name())

    def test_add_duplicate_group(self):
        self._gravitino_client.add_group("engineers")
        with self.assertRaises(GroupAlreadyExistsException):
            self._gravitino_client.add_group("engineers")

    def test_list_group_names(self):
        self._gravitino_client.add_group("engineers")
        self._gravitino_client.add_group("admins")

        names = self._gravitino_client.list_group_names()
        self.assertIn("engineers", names)
        self.assertIn("admins", names)

    def test_list_groups(self):
        self._gravitino_client.add_group("engineers")
        self._gravitino_client.add_group("admins")

        groups = self._gravitino_client.list_groups()
        group_names = [g.name() for g in groups]
        self.assertIn("engineers", group_names)
        self.assertIn("admins", group_names)

    def test_remove_group(self):
        self._gravitino_client.add_group("engineers")
        self.assertTrue(self._gravitino_client.remove_group("engineers"))

        with self.assertRaises(NoSuchGroupException):
            self._gravitino_client.get_group("engineers")

    def test_remove_nonexistent_group(self):
        self.assertFalse(self._gravitino_client.remove_group("nonexistent"))
