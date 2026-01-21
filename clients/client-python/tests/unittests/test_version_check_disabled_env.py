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
import sys
import types
import unittest
from unittest.mock import Mock, patch

package_root = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "gravitino")
)
dummy_package = types.ModuleType("gravitino")
dummy_package.__path__ = [package_root]
sys.modules.setdefault("gravitino", dummy_package)

dummy_converters = types.ModuleType("gravitino.client.dto_converters")
dummy_converters.DTOConverters = object
sys.modules.setdefault("gravitino.client.dto_converters", dummy_converters)

from gravitino.client.gravitino_admin_client import GravitinoAdminClient
from gravitino.client.gravitino_client_base import GravitinoClientBase


class TestVersionCheckDisabledEnv(unittest.TestCase):
    def test_env_true_disables_version_check(self):
        with patch.dict(os.environ, {"GRAVITINO_VERSION_CHECK_DISABLED": "true"}):
            with patch.object(
                GravitinoClientBase,
                "get_client_version",
                autospec=True,
                return_value=Mock(version=lambda: None),
            ):
                with patch.object(
                    GravitinoClientBase, "check_version", autospec=True
                ) as check_version:
                    GravitinoAdminClient(uri="http://localhost:8090")
                    check_version.assert_not_called()

    def test_env_false_keeps_version_check(self):
        with patch.dict(os.environ, {"GRAVITINO_VERSION_CHECK_DISABLED": "false"}):
            with patch.object(
                GravitinoClientBase,
                "get_client_version",
                autospec=True,
                return_value=Mock(version=lambda: None),
            ):
                with patch.object(
                    GravitinoClientBase, "check_version", autospec=True
                ) as check_version:
                    GravitinoAdminClient(uri="http://localhost:8090")
                    check_version.assert_called_once()

    def test_env_unset_keeps_version_check(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("GRAVITINO_VERSION_CHECK_DISABLED", None)
            with patch.object(
                GravitinoClientBase,
                "get_client_version",
                autospec=True,
                return_value=Mock(version=lambda: None),
            ):
                with patch.object(
                    GravitinoClientBase, "check_version", autospec=True
                ) as check_version:
                    GravitinoAdminClient(uri="http://localhost:8090")
                    check_version.assert_called_once()
