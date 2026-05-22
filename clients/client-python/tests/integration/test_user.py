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
from random import randint

from gravitino import GravitinoAdminClient, GravitinoClient
from gravitino.exceptions.base import (
    GravitinoRuntimeException,
    NoSuchUserException,
    UserAlreadyExistsException,
)

from tests.integration.integration_test_env import IntegrationTestEnv

logger = logging.getLogger(__name__)


class TestUser(IntegrationTestEnv):
    metalake_name: str = "test_user_metalake" + str(randint(1, 10000))

    gravitino_admin_client: GravitinoAdminClient = None
    gravitino_client: GravitinoClient = None

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
        cls.gravitino_admin_client = GravitinoAdminClient(uri="http://localhost:8090")

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
        self.init_test_env()

    def tearDown(self):
        self.clean_test_data()

    def init_test_env(self):
        self.gravitino_admin_client.create_metalake(
            self.metalake_name, comment="", properties={}
        )
        self.gravitino_client = GravitinoClient(
            uri="http://localhost:8090", metalake_name=self.metalake_name
        )

    def clean_test_data(self):
        self.gravitino_client = GravitinoClient(
            uri="http://localhost:8090", metalake_name=self.metalake_name
        )
        try:
            self.gravitino_admin_client.drop_metalake(self.metalake_name, force=True)
        except GravitinoRuntimeException:
            logger.warning("Failed to drop metalake %s", self.metalake_name)

    def test_add_user(self):
        user = self.gravitino_client.add_user("test_add_user")

        self.assertIsNotNone(user)
        self.assertEqual("test_add_user", user.name())
        self.assertIsNotNone(user.audit_info())

    def test_get_user(self):
        self.gravitino_client.add_user("test_get_user")

        user = self.gravitino_client.get_user("test_get_user")

        self.assertIsNotNone(user)
        self.assertEqual("test_get_user", user.name())

    def test_list_users(self):
        self.gravitino_client.add_user("test_list_user_1")
        self.gravitino_client.add_user("test_list_user_2")

        users = self.gravitino_client.list_users()

        self.assertIsNotNone(users)
        user_names = [u.name() for u in users]
        self.assertIn("test_list_user_1", user_names)
        self.assertIn("test_list_user_2", user_names)

    def test_list_user_names(self):
        self.gravitino_client.add_user("test_list_names_1")
        self.gravitino_client.add_user("test_list_names_2")

        names = self.gravitino_client.list_user_names()

        self.assertIsNotNone(names)
        self.assertIn("test_list_names_1", names)
        self.assertIn("test_list_names_2", names)

    def test_remove_user(self):
        self.gravitino_client.add_user("test_remove_user")

        removed = self.gravitino_client.remove_user("test_remove_user")
        self.assertTrue(removed)

        with self.assertRaises(NoSuchUserException):
            self.gravitino_client.get_user("test_remove_user")

    def test_get_nonexistent_user(self):
        with self.assertRaises(NoSuchUserException):
            self.gravitino_client.get_user("nonexistent_user")

    def test_add_duplicate_user(self):
        self.gravitino_client.add_user("test_duplicate_user")

        with self.assertRaises(UserAlreadyExistsException):
            self.gravitino_client.add_user("test_duplicate_user")

    def test_remove_nonexistent_user(self):
        removed = self.gravitino_client.remove_user("nonexistent_user")
        self.assertFalse(removed)
