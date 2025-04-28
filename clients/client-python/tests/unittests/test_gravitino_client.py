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
            expected_headers, gravitino_admin_client._rest_client.request_headers
        )

        gravitino_client = GravitinoClient(
            uri="http://localhost:8090",
            metalake_name="test",
            request_headers=expected_headers,
        )
        self.assertEqual(
            expected_headers, gravitino_client._rest_client.request_headers
        )
