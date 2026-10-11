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
from unittest.mock import Mock, patch

from gravitino.utils.http_client import HTTPClient


class TestHTTPClientClose(unittest.TestCase):
    @patch("gravitino.utils.http_client.build_opener")
    def test_close_without_auth_provider_is_idempotent(self, build_opener):
        client = HTTPClient("http://localhost")

        client.close()
        client.close()

        build_opener.assert_not_called()

    @patch("gravitino.utils.http_client.build_opener")
    def test_close_closes_auth_provider_once(self, build_opener):
        auth_data_provider = Mock()
        client = HTTPClient("http://localhost", auth_data_provider=auth_data_provider)

        client.close()
        client.close()

        auth_data_provider.close.assert_called_once_with()
        build_opener.assert_not_called()

    @patch("gravitino.utils.http_client.build_opener")
    def test_close_retries_after_auth_provider_failure(self, build_opener):
        auth_data_provider = Mock()
        auth_data_provider.close.side_effect = [RuntimeError("cleanup failed"), None]
        client = HTTPClient("http://localhost", auth_data_provider=auth_data_provider)

        with self.assertRaisesRegex(RuntimeError, "cleanup failed"):
            client.close()

        client.close()
        client.close()

        self.assertEqual(2, auth_data_provider.close.call_count)
        build_opener.assert_not_called()
