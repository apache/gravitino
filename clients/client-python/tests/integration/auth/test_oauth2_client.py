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

import os
import subprocess
import logging
import unittest
import sys
import requests
from jwcrypto import jwk

from gravitino.auth.auth_constants import AuthConstants
from gravitino.auth.default_oauth2_token_provider import DefaultOAuth2TokenProvider
from gravitino import GravitinoAdminClient, GravitinoClient
from gravitino.exceptions.base import GravitinoRuntimeException

from tests.integration.auth.test_auth_common import TestCommonAuth
from tests.integration.integration_test_env import (
    IntegrationTestEnv,
    check_gravitino_server_status,
)
from tests.integration.containers.oauth2_container import OAuth2Container

logger = logging.getLogger(__name__)

DOCKER_TEST = os.environ.get("DOCKER_TEST")


@unittest.skipIf(
    DOCKER_TEST == "false",
    "Skipping tests when DOCKER_TEST=false",
)
class TestOAuth2(IntegrationTestEnv, TestCommonAuth):

    oauth2_container: OAuth2Container = None

    @classmethod
    def setUpClass(cls):

        cls._get_gravitino_home()

        cls.oauth2_container = OAuth2Container()
        cls.oauth2_container_ip = cls.oauth2_container.get_ip()

        cls.oauth2_server_uri = f"http://{cls.oauth2_container_ip}:8177"

        # Get PEM from OAuth Server
        default_sign_key = cls._get_default_sign_key()

        cls.config = {
            "gravitino.authenticators": "oauth",
            "gravitino.authenticator.oauth.serviceAudience": "test",
            "gravitino.authenticator.oauth.defaultSignKey": default_sign_key,
            "gravitino.authenticator.oauth.serverUri": cls.oauth2_server_uri,
            "gravitino.authenticator.oauth.tokenPath": "/oauth2/token",
        }

        cls.oauth2_conf_path = f"{cls.gravitino_home}/conf/gravitino.conf"

        # append the hadoop conf to server
        cls._append_conf(cls.config, cls.oauth2_conf_path)
        # restart the server
        cls._restart_server_with_oauth()

    @classmethod
    def tearDownClass(cls):
        try:
            # reset server conf
            cls._reset_conf(cls.config, cls.oauth2_conf_path)
            # restart server
            cls.restart_server()
        finally:
            # close oauth2 container
            cls.oauth2_container.close()

    @classmethod
    def _get_default_sign_key(cls) -> str:

        jwk_uri = f"{cls.oauth2_server_uri}/oauth2/jwks"

        # Get JWK from OAuth2 Server
        res = requests.get(jwk_uri).json()
        key = res["keys"][0]

        # Convert JWK to PEM
        pem = jwk.JWK(**key).export_to_pem().decode("utf-8")

        default_sign_key = "".join(pem.split("\n")[1:-2])

        return default_sign_key

    @classmethod
    def _restart_server_with_oauth(cls):
        logger.info("Restarting Gravitino server...")
        gravitino_home = os.environ.get("GRAVITINO_HOME")
        gravitino_startup_script = os.path.join(gravitino_home, "bin/gravitino.sh")
        if not os.path.exists(gravitino_startup_script):
            raise GravitinoRuntimeException(
                f"Can't find Gravitino startup script: {gravitino_startup_script}, "
                "Please execute `./gradlew compileDistribution -x test` in the Gravitino "
                "project root directory."
            )

        # Restart Gravitino Server
        env_vars = os.environ.copy()
        result = subprocess.run(
            [gravitino_startup_script, "restart"],
            env=env_vars,
            capture_output=True,
            text=True,
            check=False,
        )
        if result.stdout:
            logger.info("stdout: %s", result.stdout)
        if result.stderr:
            logger.info("stderr: %s", result.stderr)

        oauth2_token_provider = DefaultOAuth2TokenProvider(
            f"{cls.oauth2_server_uri}", "test:test", "test", "oauth2/token"
        )

        auth_header = {
            AuthConstants.HTTP_HEADER_AUTHORIZATION: oauth2_token_provider.get_token_data().decode(
                "utf-8"
            )
        }

        if not check_gravitino_server_status(headers=auth_header):
            logger.error("ERROR: Can't start Gravitino server!")
            sys.exit(0)

    def setUp(self):
        oauth2_token_provider = DefaultOAuth2TokenProvider(
            f"{self.oauth2_server_uri}", "test:test", "test", "oauth2/token"
        )

        self.gravitino_admin_client = GravitinoAdminClient(
            uri="http://localhost:8090", auth_data_provider=oauth2_token_provider
        )

        self.init_test_env()

    def init_test_env(self):

        self.gravitino_admin_client.create_metalake(
            self.metalake_name, comment="", properties={}
        )

        oauth2_token_provider = DefaultOAuth2TokenProvider(
            f"{self.oauth2_server_uri}", "test:test", "test", "oauth2/token"
        )

        self.gravitino_client = GravitinoClient(
            uri="http://localhost:8090",
            metalake_name=self.metalake_name,
            auth_data_provider=oauth2_token_provider,
        )

        super().init_test_env()

    def tearDown(self):
        self.clean_test_data()
