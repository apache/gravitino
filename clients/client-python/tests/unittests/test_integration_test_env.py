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
import shutil
import subprocess
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

from gravitino.exceptions.base import GravitinoRuntimeException
from tests.integration.integration_test_env import IntegrationTestEnv


class TestIntegrationTestEnv(unittest.TestCase):
    def setUp(self):
        self.patches = ExitStack()
        self.addCleanup(self.patches.close)
        home = Path(self.patches.enter_context(tempfile.TemporaryDirectory()))
        self.script = home / "bin" / "gravitino.sh"
        self.script.parent.mkdir()
        self.script.touch()
        self.data = home / "data"
        self.data.mkdir()
        self.metadata = self.data / "metadata"
        self.metadata.write_text("existing metadata", encoding="utf-8")

        self.patches.enter_context(
            patch.dict(os.environ, {"GRAVITINO_HOME": str(home)})
        )
        self.run = self.patches.enter_context(
            patch("tests.integration.integration_test_env.subprocess.run")
        )
        self.run.return_value = subprocess.CompletedProcess([], 0, "", "")
        self.ready = self.patches.enter_context(
            patch(
                "tests.integration.integration_test_env.check_gravitino_server_status",
                return_value=True,
            )
        )
        self.client = self.patches.enter_context(
            patch("tests.integration.integration_test_env.GravitinoAdminClient")
        )
        self.patches.enter_context(
            patch.object(IntegrationTestEnv, "gravitino_admin_client", None)
        )

    def test_restart_stops_server_before_removing_data(self):
        events = []
        remove_data = shutil.rmtree

        def run_command(command, **kwargs):
            events.append(command[1])
            self.assertEqual("anonymous", kwargs["env"]["HADOOP_USER_NAME"])
            return subprocess.CompletedProcess(command, 0, "", "")

        def remove_directory(path):
            self.assertEqual(["stop"], events)
            events.append("remove data")
            remove_data(path)

        self.run.side_effect = run_command
        self.ready.side_effect = lambda: events.append("ready") or True
        with patch(
            "tests.integration.integration_test_env.shutil.rmtree",
            side_effect=remove_directory,
        ):
            IntegrationTestEnv.restart_server()

        self.assertEqual(["stop", "remove data", "start", "ready"], events)
        self.assertFalse(self.data.exists())
        self.assertIs(
            self.client.return_value, IntegrationTestEnv.gravitino_admin_client
        )

    def test_restart_without_data_directory(self):
        shutil.rmtree(self.data)
        with patch("tests.integration.integration_test_env.shutil.rmtree") as remove:
            IntegrationTestEnv.restart_server()
        remove.assert_not_called()
        self.assertEqual(
            ["stop", "start"], [call.args[0][1] for call in self.run.call_args_list]
        )
        self.ready.assert_called_once_with()

    def test_failed_stop_preserves_data_and_does_not_start(self):
        self.run.side_effect = subprocess.CalledProcessError(
            1, [str(self.script), "stop"]
        )
        with self.assertRaises(subprocess.CalledProcessError):
            IntegrationTestEnv.restart_server()
        self.assertEqual("existing metadata", self.metadata.read_text(encoding="utf-8"))
        self.assertEqual(1, self.run.call_count)
        self.ready.assert_not_called()
        self.client.assert_not_called()

    def test_failed_cleanup_does_not_start(self):
        with patch(
            "tests.integration.integration_test_env.shutil.rmtree",
            side_effect=OSError("Cannot remove test metadata"),
        ):
            with self.assertRaises(OSError):
                IntegrationTestEnv.restart_server()
        self.assertEqual(1, self.run.call_count)
        self.assertEqual("stop", self.run.call_args.args[0][1])
        self.ready.assert_not_called()

    def test_failed_start_does_not_poll_readiness(self):
        self.run.side_effect = [
            subprocess.CompletedProcess([str(self.script), "stop"], 0, "", ""),
            subprocess.CompletedProcess([str(self.script), "start"], 1, "", "failed"),
        ]
        with self.assertRaises(subprocess.CalledProcessError):
            IntegrationTestEnv.restart_server()
        self.assertFalse(self.data.exists())
        self.ready.assert_not_called()
        self.client.assert_not_called()

    def test_unready_server_does_not_create_client(self):
        self.ready.return_value = False
        with self.assertRaises(GravitinoRuntimeException):
            IntegrationTestEnv.restart_server()
        self.client.assert_not_called()

    def test_missing_script_preserves_data(self):
        self.script.unlink()
        with self.assertRaises(GravitinoRuntimeException):
            IntegrationTestEnv.restart_server()
        self.assertTrue(self.metadata.exists())
        self.run.assert_not_called()
