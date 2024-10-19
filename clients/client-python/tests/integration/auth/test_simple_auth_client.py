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

from gravitino import (
    GravitinoClient,
    GravitinoAdminClient,
)
from gravitino.auth.simple_auth_provider import SimpleAuthProvider

from tests.integration.auth.test_auth_common import TestCommonAuth
from tests.integration.integration_test_env import IntegrationTestEnv

logger = logging.getLogger(__name__)


class TestSimpleAuthClient(IntegrationTestEnv, TestCommonAuth):

    def setUp(self):
        os.environ["GRAVITINO_USER"] = self.creator
        self.gravitino_admin_client = GravitinoAdminClient(
            uri="http://localhost:8090", auth_data_provider=SimpleAuthProvider()
        )

        self.init_test_env()

    def init_test_env(self):
        self.gravitino_admin_client.create_metalake(
            self.metalake_name, comment="", properties={}
        )
        self.gravitino_client = GravitinoClient(
            uri="http://localhost:8090",
            metalake_name=self.metalake_name,
            auth_data_provider=SimpleAuthProvider(),
        )

        super().init_test_env()

    def tearDown(self):
        self.clean_test_data()
