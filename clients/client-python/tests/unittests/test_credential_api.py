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
from typing import List
import json
import unittest
from http.client import HTTPResponse
from unittest.mock import patch, Mock

from gravitino import GravitinoClient, NameIdentifier
from gravitino.api.credential.credential import Credential
from gravitino.api.credential.s3_token_credential import S3TokenCredential
from gravitino.client.generic_fileset import GenericFileset
from gravitino.namespace import Namespace
from gravitino.utils import Response, HTTPClient
from tests.unittests import mock_base


@mock_base.mock_data
class TestCredentialApi(unittest.TestCase):
    def test_get_credentials(self, *mock_method):
        json_str = self._get_s3_token_str()
        mock_resp = self._get_mock_http_resp(json_str)

        metalake_name: str = "metalake_demo"
        catalog_name: str = "fileset_catalog"
        gravitino_client = GravitinoClient(
            uri="http://localhost:8090", metalake_name=metalake_name
        )
        catalog = gravitino_client.load_catalog(catalog_name)

        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            return_value=mock_resp,
        ):
            credentials = (
                catalog.as_fileset_catalog().support_credentials().get_credentials()
            )
            self._check_credential(credentials)

        fileset_dto = catalog.as_fileset_catalog().load_fileset(
            NameIdentifier.of("schema", "fileset")
        )
        fileset = GenericFileset(
            fileset_dto,
            HTTPClient("http://localhost:8090"),
            Namespace.of(metalake_name, catalog_name, "schema"),
        )

        # check the request path is ok
        # pylint: disable=protected-access
        request_path = fileset._object_credential_operations._request_path
        expected_path = (
            f"api/metalakes/{metalake_name}/objects/fileset/"
            f"fileset_catalog.schema.fileset/credentials"
        )

        self.assertEqual(expected_path, request_path)

        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            return_value=mock_resp,
        ):
            credentials = fileset.support_credentials().get_credentials()
            self._check_credential(credentials)

    def _get_mock_http_resp(self, json_str: str):
        mock_http_resp = Mock(HTTPResponse)
        mock_http_resp.getcode.return_value = 200
        mock_http_resp.read.return_value = json_str
        mock_http_resp.info.return_value = None
        mock_http_resp.url = None
        mock_resp = Response(mock_http_resp)
        return mock_resp

    def _get_s3_token_str(self):
        json_data = {
            "code": 0,
            "credentials": [
                {
                    "credentialType": "s3-token",
                    "expireTimeInMs": 1000,
                    "credentialInfo": {
                        "s3-access-key-id": "access_id",
                        "s3-secret-access-key": "secret_key",
                        "s3-session-token": "token",
                    },
                }
            ],
        }
        return json.dumps(json_data)

    def _check_credential(self, credentials: List[Credential]):
        self.assertEqual(1, len(credentials))
        s3_credential: S3TokenCredential = credentials[0]
        self.assertEqual(
            S3TokenCredential.S3_TOKEN_CREDENTIAL_TYPE, s3_credential.credential_type()
        )
        self.assertEqual("access_id", s3_credential.access_key_id())
        self.assertEqual("secret_key", s3_credential.secret_access_key())
        self.assertEqual("token", s3_credential.session_token())
        self.assertEqual(1000, s3_credential.expire_time_in_ms())
