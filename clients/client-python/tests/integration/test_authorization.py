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
from random import randint

from gravitino import GravitinoAdminClient, GravitinoClient
from gravitino.api.authorization.privileges import Privilege, Privileges
from gravitino.api.authorization.securable_objects import SecurableObjects
from gravitino.exceptions.base import GravitinoRuntimeException
from tests.integration.integration_test_env import IntegrationTestEnv

logger = logging.getLogger(__name__)


class TestAuthorization(IntegrationTestEnv):
    metalake_name: str = "TestAuth_metalake" + str(randint(1, 10000))
    metalake_comment: str = "authorization test metalake"

    gravitino_admin_client: GravitinoAdminClient = GravitinoAdminClient(
        uri="http://localhost:8090"
    )

    def _get_client(self) -> GravitinoClient:
        return GravitinoClient(
            uri="http://localhost:8090",
            metalake_name=self.metalake_name,
        )

    def setUp(self):
        self.gravitino_admin_client.create_metalake(
            self.metalake_name,
            self.metalake_comment,
            {},
        )

    def tearDown(self):
        self.clean_test_data()

    def clean_test_data(self):
        try:
            self.gravitino_admin_client.drop_metalake(self.metalake_name, True)
        except GravitinoRuntimeException:
            logger.warning("Failed to drop metalake %s", self.metalake_name)

    # ========== User management ==========

    def test_add_and_get_user(self):
        client = self._get_client()
        user = client.add_user("test_user_add")
        self.assertIsNotNone(user)
        self.assertEqual("test_user_add", user.name())

        fetched = client.get_user("test_user_add")
        self.assertEqual("test_user_add", fetched.name())

    def test_list_user_names(self):
        client = self._get_client()
        client.add_user("test_user_list_names")
        names = client.list_user_names()
        self.assertIn("test_user_list_names", names)

    def test_list_users(self):
        client = self._get_client()
        client.add_user("test_user_list_details")
        users = client.list_users()
        user_names = [u.name() for u in users]
        self.assertIn("test_user_list_details", user_names)

    def test_remove_user(self):
        client = self._get_client()
        client.add_user("test_user_remove")
        result = client.remove_user("test_user_remove")
        self.assertTrue(result)

    # ========== Group management ==========

    def test_add_and_get_group(self):
        client = self._get_client()
        group = client.add_group("test_group_add")
        self.assertIsNotNone(group)
        self.assertEqual("test_group_add", group.name())

        fetched = client.get_group("test_group_add")
        self.assertEqual("test_group_add", fetched.name())

    def test_list_group_names(self):
        client = self._get_client()
        client.add_group("test_group_list_names")
        names = client.list_group_names()
        self.assertIn("test_group_list_names", names)

    def test_list_groups(self):
        client = self._get_client()
        client.add_group("test_group_list_details")
        groups = client.list_groups()
        group_names = [g.name() for g in groups]
        self.assertIn("test_group_list_details", group_names)

    def test_remove_group(self):
        client = self._get_client()
        client.add_group("test_group_remove")
        result = client.remove_group("test_group_remove")
        self.assertTrue(result)

    # ========== Role management ==========

    def test_create_and_get_role(self):
        client = self._get_client()
        metalake_obj = SecurableObjects.of_metalake(
            self.metalake_name,
            [Privileges.allow(Privilege.Name.CREATE_FILESET)],
        )
        role = client.create_role(
            "test_role_create",
            {"purpose": "test"},
            [metalake_obj],
        )
        self.assertIsNotNone(role)
        self.assertEqual("test_role_create", role.name())
        self.assertEqual(role.properties().get("purpose"), "test")

        fetched = client.get_role("test_role_create")
        self.assertEqual("test_role_create", fetched.name())

    def test_list_role_names(self):
        client = self._get_client()
        client.create_role("test_role_list", None, [])
        names = client.list_role_names()
        self.assertIn("test_role_list", names)

    def test_delete_role(self):
        client = self._get_client()
        client.create_role("test_role_delete", None, [])
        result = client.delete_role("test_role_delete")
        self.assertTrue(result)

    # ========== Permission operations ==========

    def test_grant_revoke_roles_to_user(self):
        client = self._get_client()
        client.add_user("test_perm_user")
        client.create_role("test_perm_role", None, [])

        user = client.grant_roles_to_user(["test_perm_role"], "test_perm_user")
        self.assertIn("test_perm_role", user.roles())

        user = client.revoke_roles_from_user(["test_perm_role"], "test_perm_user")
        self.assertNotIn("test_perm_role", user.roles())

    def test_grant_revoke_roles_to_group(self):
        client = self._get_client()
        client.add_group("test_perm_group")
        client.create_role("test_perm_role_g", None, [])

        group = client.grant_roles_to_group(["test_perm_role_g"], "test_perm_group")
        self.assertIn("test_perm_role_g", group.roles())

        group = client.revoke_roles_from_group(
            ["test_perm_role_g"], "test_perm_group"
        )
        self.assertNotIn("test_perm_role_g", group.roles())

    def test_grant_revoke_privileges_to_role(self):
        client = self._get_client()
        metalake_obj = SecurableObjects.of_metalake(
            self.metalake_name,
            [Privileges.allow(Privilege.Name.CREATE_FILESET)],
        )
        client.create_role("test_priv_role", None, [metalake_obj])

        # Grant a new privilege on the metalake to the role
        role = client.grant_privileges_to_role(
            "test_priv_role",
            metalake_obj,
            [Privileges.allow(Privilege.Name.CREATE_TABLE)],
        )
        self.assertIsNotNone(role)

        # Revoke the privilege
        role = client.revoke_privileges_from_role(
            "test_priv_role",
            metalake_obj,
            [Privileges.allow(Privilege.Name.CREATE_TABLE)],
        )
        self.assertIsNotNone(role)
