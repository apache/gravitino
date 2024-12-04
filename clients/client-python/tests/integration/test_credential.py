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
from unittest import TestCase

from gravitino import (
    NameIdentifier,
    GravitinoAdminClient,
    GravitinoClient,
)
from gravitino.api.credential.s3_token_credential import S3TokenCredential
from tests.integration.integration_test_env import IntegrationTestEnv

logger = logging.getLogger(__name__)


class TestGetCredentials(TestCase):
    metalake_name: str = "test"
    catalog_name: str = "catalog"
    schema_name: str = "schema"
    fileset_name: str = "fileset"

    catalog_ident: NameIdentifier = NameIdentifier.of(metalake_name, catalog_name)
    schema_ident: NameIdentifier = NameIdentifier.of(
        metalake_name, catalog_name, schema_name
    )
    fileset_ident: NameIdentifier = NameIdentifier.of(schema_name, fileset_name)

    gravitino_client: GravitinoClient = GravitinoClient(
        uri="http://localhost:8090", metalake_name=metalake_name, check_version=False
    )

    def test_get_catalog_credentials(self):
        catalog = self.gravitino_client.load_catalog(name=self.catalog_name)
        credentials = (
            catalog.as_fileset_catalog().support_credentials().get_credentials()
        )
        self.assertEqual(1, len(credentials))
        credential = credentials[0]
        self.assertEqual(
            S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE, credential.credential_type()
        )
        self.assertEqual("access-id", credential.access_key_id())
        self.assertEqual("secret-key", credential.secret_access_key())
        self.assertEqual("token", credential.session_token())

    def test_get_fileset_credentials(self):
        catalog = self.gravitino_client.load_catalog(name=self.catalog_name)
        fileset = catalog.as_fileset_catalog().load_fileset(ident=self.fileset_ident)
        credentials = fileset.support_credentials().get_credentials()
        self.assertEqual(1, len(credentials))

        credential = credentials[0]
        self.assertEqual(
            S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE, credential.credential_type()
        )
        self.assertEqual("access-id", credential.access_key_id())
        self.assertEqual("secret-key", credential.secret_access_key())
        self.assertEqual("token", credential.session_token())
