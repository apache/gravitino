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
import os
import uuid

from gravitino import GravitinoAdminClient, GravitinoClient
from gravitino.api.authorization.privileges import Privileges
from gravitino.api.authorization.securable_objects import SecurableObjects
from gravitino.exceptions.base import (
    NoSuchRoleException,
    RoleAlreadyExistsException,
)
from tests.integration.integration_test_env import IntegrationTestEnv

logger = logging.getLogger(__name__)


class TestRoleManagement(IntegrationTestEnv):
    _metalake_name: str = f"test_role_metalake_{uuid.uuid4().hex[:8]}"
    _gravitino_admin_client: GravitinoAdminClient = None
    _gravitino_client: GravitinoClient = None

    @classmethod
    def setUpClass(cls):
        cls._get_gravitino_home()
        conf_path = os.path.join(cls.gravitino_home, "conf", "gravitino.conf")
        auth_confs = {
            "gravitino.authorization.enable": "true",
            "gravitino.authorization.serviceAdmins": "anonymous",
        }
        cls._reset_conf(auth_confs, conf_path)
        cls._append_conf(auth_confs, conf_path)
        if (
            os.environ.get("START_EXTERNAL_GRAVITINO") is not None
            and os.environ.get("START_EXTERNAL_GRAVITINO").lower() == "true"
        ):
            cls.restart_server()
        else:
            super().setUpClass()
        cls._gravitino_admin_client = GravitinoAdminClient(uri="http://localhost:8090")

    @classmethod
    def tearDownClass(cls):
        conf_path = os.path.join(cls.gravitino_home, "conf", "gravitino.conf")
        reset_confs = {
            "gravitino.authorization.enable": "false",
            "gravitino.authorization.serviceAdmins": "anonymous",
        }
        cls._reset_conf(reset_confs, conf_path)
        cls._append_conf(reset_confs, conf_path)
        if (
            os.environ.get("START_EXTERNAL_GRAVITINO") is not None
            and os.environ.get("START_EXTERNAL_GRAVITINO").lower() == "true"
        ):
            cls.restart_server()
        else:
            super().tearDownClass()

    def setUp(self):
        self._gravitino_admin_client.create_metalake(
            self._metalake_name, comment="test role management", properties={}
        )
        self._gravitino_client = GravitinoClient(
            uri="http://localhost:8090", metalake_name=self._metalake_name
        )

    def tearDown(self):
        try:
            self._gravitino_admin_client.drop_metalake(self._metalake_name, force=True)
        except Exception:  # pylint: disable=broad-except
            logger.warning("Failed to drop metalake %s", self._metalake_name)

    def test_create_and_get_role(self):
        privileges = [Privileges.allow("USE_CATALOG")]
        securable_objects = [
            SecurableObjects.of_metalake(self._metalake_name, privileges)
        ]
        created = self._gravitino_client.create_role(
            "test_role",
            properties={"k": "v"},
            securable_objects=securable_objects,
        )
        self.assertEqual("test_role", created.name())

        retrieved = self._gravitino_client.get_role("test_role")
        self.assertEqual("test_role", retrieved.name())

    def test_create_duplicate_role(self):
        self._gravitino_client.create_role("dup_role")
        with self.assertRaises(RoleAlreadyExistsException):
            self._gravitino_client.create_role("dup_role")

    def test_delete_role(self):
        self._gravitino_client.create_role("del_role")
        self.assertTrue(self._gravitino_client.delete_role("del_role"))

        with self.assertRaises(NoSuchRoleException):
            self._gravitino_client.get_role("del_role")

    def test_list_role_names(self):
        self._gravitino_client.create_role("role_a")
        self._gravitino_client.create_role("role_b")

        names = self._gravitino_client.list_role_names()
        self.assertIn("role_a", names)
        self.assertIn("role_b", names)

    def test_grant_revoke_roles_to_user(self):
        self._gravitino_client.create_role("user_role")
        self._gravitino_client.add_user("alice")

        granted = self._gravitino_client.grant_roles_to_user(["user_role"], "alice")
        self.assertIn("user_role", granted.roles())

        revoked = self._gravitino_client.revoke_roles_from_user(["user_role"], "alice")
        self.assertNotIn("user_role", revoked.roles())

    def test_grant_revoke_roles_to_group(self):
        self._gravitino_client.create_role("group_role")
        self._gravitino_client.add_group("engineers")

        granted = self._gravitino_client.grant_roles_to_group(
            ["group_role"], "engineers"
        )
        self.assertIn("group_role", granted.roles())

        revoked = self._gravitino_client.revoke_roles_from_group(
            ["group_role"], "engineers"
        )
        self.assertNotIn("group_role", revoked.roles())

    def test_grant_revoke_privileges_to_role(self):
        self._gravitino_client.create_role("priv_role")

        privileges = [Privileges.allow("USE_CATALOG")]
        securable_obj = SecurableObjects.of_metalake(self._metalake_name, privileges)

        granted = self._gravitino_client.grant_privileges_to_role(
            "priv_role", securable_obj, privileges
        )
        self.assertEqual("priv_role", granted.name())
        self.assertGreater(len(granted.securable_objects()), 0)
        granted_privs = granted.securable_objects()[0].privileges()
        granted_names = [p.name().name for p in granted_privs]
        self.assertIn("USE_CATALOG", granted_names)

        revoked = self._gravitino_client.revoke_privileges_from_role(
            "priv_role", securable_obj, privileges
        )
        self.assertEqual("priv_role", revoked.name())
        revoked_objs = revoked.securable_objects()
        if revoked_objs:
            revoked_names = [p.name().name for p in revoked_objs[0].privileges()]
            self.assertNotIn("USE_CATALOG", revoked_names)
