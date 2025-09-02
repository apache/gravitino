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
import unittest

from gravitino import GravitinoAdminClient, GravitinoClient
from gravitino.client.gravitino_client_config import GravitinoClientConfig
from gravitino.constants.timeout import TIMEOUT
from tests.unittests import mock_base


@mock_base.mock_data
class TestMetalake(unittest.TestCase):
    # pylint: disable=W0212
    def test_gravitino_client_headers(self, *mock_methods):
        expected_headers = {
            "k1": "v1",
        }
        gravitino_admin_client = GravitinoAdminClient(
            uri="http://localhost:8090",
            request_headers=expected_headers,
        )
        self.assertEqual(
            expected_headers["k1"],
            gravitino_admin_client._rest_client.request_headers["k1"],
        )

        gravitino_client = GravitinoClient(
            uri="http://localhost:8090",
            metalake_name="test",
            request_headers=expected_headers,
        )
        self.assertEqual(
            expected_headers["k1"], gravitino_client._rest_client.request_headers["k1"]
        )

    def test_gravitino_client_timeout(self, *mock_methods):
        gravitino_admin_client = GravitinoAdminClient(
            uri="http://localhost:8090",
        )
        self.assertEqual(TIMEOUT, gravitino_admin_client._rest_client.timeout)

        gravitino_admin_client = GravitinoAdminClient(
            uri="http://localhost:8090",
            client_config={"gravitino_client_request_timeout": 60},
        )
        self.assertEqual(60, gravitino_admin_client._rest_client.timeout)

        gravitino_client = GravitinoClient(
            uri="http://localhost:8090",
            metalake_name="test",
            client_config={"gravitino_client_request_timeout": 60},
        )
        self.assertEqual(60, gravitino_client._rest_client.timeout)

    def test_invalid_gravitino_client_config(self, *mock_methods):
        # test invalid config
        self.assertRaisesRegex(
            ValueError,
            "Invalid property for client: gravitino_client_request_timeout_xxxxxx",
            GravitinoClientConfig.build_from_properties,
            {"gravitino_client_request_timeout_xxxxxx": 1},
        )

        client_config = GravitinoClientConfig.build_from_properties(
            {"gravitino_client_request_timeout": -1}
        )
        self.assertRaisesRegex(
            ValueError,
            "Value '-1' for key 'gravitino_client_request_timeout' is invalid. The value must be a positive number",
            client_config.get_client_request_timeout,
        )

        client_config = GravitinoClientConfig.build_from_properties(
            {"gravitino_client_request_timeout": "a"}
        )
        self.assertRaisesRegex(
            ValueError,
            "Value 'a' for key 'gravitino_client_request_timeout' must be an integer",
            client_config.get_client_request_timeout,
        )
