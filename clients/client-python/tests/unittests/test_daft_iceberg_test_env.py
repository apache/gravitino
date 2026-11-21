# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Failure paths for the real-service fixture, without Daft or a running JVM."""

import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import requests

from tests.integration import test_daft_iceberg as contract
from tests.integration.daft_iceberg_test_env import DaftIcebergTestEnv


class TestDaftIcebergTestEnv(unittest.TestCase):
    """Assert setup failures still restore configuration and release resources."""

    def setUp(self):
        # addCleanup also releases the directory when setUp fails on Python 3.10.
        # pylint: disable=consider-using-with
        directory = tempfile.TemporaryDirectory()
        # pylint: enable=consider-using-with
        self.addCleanup(directory.cleanup)
        self.home = Path(directory.name)
        (self.home / "bin").mkdir()
        (self.home / "bin/gravitino.sh").write_text("fixture script", encoding="utf-8")
        (self.home / "conf").mkdir()
        self.original_conf = (
            b"# preserve comments and duplicate overridden properties\r\n"
            b"gravitino.server.webserver.httpPort = 8090\r\n"
            b"gravitino.server.webserver.httpPort = 8091\r\n"
            b"unrelated = value=with=equals\r\n"
        )
        self.conf = self.home / "conf/gravitino.conf"
        self.conf.write_bytes(self.original_conf)
        with mock.patch(
            "tests.integration.daft_iceberg_test_env.available_port",
            side_effect=[18090, 19001],
        ):
            self.env = DaftIcebergTestEnv(self.home, self.home / "logs")
        self.addCleanup(self.env.session.close)
        self.process = mock.Mock()
        self.process.poll.return_value = None

    def test_fixture_ports_are_distinct_after_repeated_allocation(self):
        with mock.patch(
            "tests.integration.daft_iceberg_test_env.available_port",
            side_effect=[18090, 18090, 19001],
        ):
            fixture = DaftIcebergTestEnv(self.home, self.home / "logs")
        self.addCleanup(fixture.session.close)
        self.assertEqual("http://127.0.0.1:18090", fixture.server_url)
        self.assertEqual("http://127.0.0.1:19001/iceberg/", fixture.iceberg_url)

    def test_startup_failure_restores_configuration_and_stops_owned_process(self):
        with (
            mock.patch(
                "tests.integration.daft_iceberg_test_env.subprocess.Popen",
                return_value=self.process,
            ),
            mock.patch.object(
                self.env, "wait_ready", side_effect=RuntimeError("startup failed")
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "startup failed"):
                self.env.start()
            warehouse = Path(self.env.warehouse.name)
            self.assertNotEqual(self.original_conf, self.conf.read_bytes())
            self.env.close()
        self.process.terminate.assert_called_once()
        self.assertEqual(self.original_conf, self.conf.read_bytes())
        self.assertFalse(warehouse.exists())
        self.env.close()  # Cleanup after partial setup is safe to call again.

    def test_catalog_setup_failure_drops_the_created_metalake(self):
        with (
            mock.patch(
                "tests.integration.daft_iceberg_test_env.subprocess.Popen",
                return_value=self.process,
            ),
            mock.patch.object(self.env, "wait_ready"),
            mock.patch.object(
                self.env,
                "request",
                side_effect=[{"code": 0}, requests.HTTPError("catalog failed"), {}],
            ) as request,
        ):
            with self.assertRaisesRegex(requests.HTTPError, "catalog failed"):
                self.env.start()
            self.env.close()
            request.assert_called_with(
                "DELETE", f"/metalakes/{self.env.metalake}?force=true"
            )
        self.assertEqual(self.original_conf, self.conf.read_bytes())
        self.process.terminate.assert_called_once()

    def test_metadata_cleanup_failure_does_not_prevent_other_cleanup(self):
        response = requests.Response()
        response.status_code = 403
        with (
            mock.patch(
                "tests.integration.daft_iceberg_test_env.subprocess.Popen",
                return_value=self.process,
            ),
            mock.patch.object(self.env, "wait_ready"),
            mock.patch.object(self.env, "request", return_value={}) as request,
        ):
            self.env.start()
            warehouse = Path(self.env.warehouse.name)
            request.side_effect = requests.HTTPError("delete failed", response=response)
            with self.assertRaisesRegex(RuntimeError, "cleanup failed"):
                self.env.close()
        self.process.terminate.assert_called_once()
        self.assertEqual(self.original_conf, self.conf.read_bytes())
        self.assertFalse(warehouse.exists())
        self.assertIsNone(self.env.server_log)

    def test_unconfirmed_creation_is_cleaned_after_transport_or_response_failure(self):
        errors = [
            requests.ReadTimeout("create response timed out"),
            RuntimeError("invalid create response"),
        ]
        for error in errors:
            with self.subTest(error=type(error).__name__):
                self.process.reset_mock()
                with (
                    mock.patch(
                        "tests.integration.daft_iceberg_test_env.subprocess.Popen",
                        return_value=self.process,
                    ),
                    mock.patch.object(self.env, "wait_ready"),
                    mock.patch.object(
                        self.env, "request", side_effect=[error, {"code": 0}]
                    ) as request,
                ):
                    with self.assertRaises(type(error)):
                        self.env.start()
                    warehouse = Path(self.env.warehouse.name)
                    self.env.close()
                    request.assert_called_with(
                        "DELETE", f"/metalakes/{self.env.metalake}?force=true"
                    )
                self.assertEqual(self.original_conf, self.conf.read_bytes())
                self.assertFalse(warehouse.exists())
                self.process.terminate.assert_called_once()

    def test_missing_metalake_after_unconfirmed_creation_is_already_clean(self):
        response = requests.Response()
        response.status_code = 404
        with (
            mock.patch(
                "tests.integration.daft_iceberg_test_env.subprocess.Popen",
                return_value=self.process,
            ),
            mock.patch.object(self.env, "wait_ready"),
            mock.patch.object(
                self.env,
                "request",
                side_effect=[
                    requests.ReadTimeout("create response timed out"),
                    requests.HTTPError("metalake not found", response=response),
                ],
            ) as request,
        ):
            with self.assertRaises(requests.ReadTimeout):
                self.env.start()
            self.env.close()
            request.assert_called_with(
                "DELETE", f"/metalakes/{self.env.metalake}?force=true"
            )
            self.assertFalse(self.env.metalake_cleanup_required)
            self.env.close()
            self.assertEqual(2, request.call_count)
        self.assertEqual(self.original_conf, self.conf.read_bytes())
        self.process.terminate.assert_called_once()

    def test_name_conflict_does_not_delete_an_existing_metalake(self):
        response = requests.Response()
        response.status_code = 409
        with (
            mock.patch(
                "tests.integration.daft_iceberg_test_env.subprocess.Popen",
                return_value=self.process,
            ),
            mock.patch.object(self.env, "wait_ready"),
            mock.patch.object(
                self.env,
                "request",
                side_effect=requests.HTTPError("metalake exists", response=response),
            ) as request,
        ):
            with self.assertRaises(requests.HTTPError):
                self.env.start()
            self.env.close()
            self.assertEqual(1, request.call_count)
            self.assertEqual("POST", request.call_args.args[0])
        self.assertEqual(self.original_conf, self.conf.read_bytes())
        self.process.terminate.assert_called_once()

    def test_unresponsive_process_is_killed_and_reaped(self):
        self.env.process = self.process
        self.process.wait.side_effect = [subprocess.TimeoutExpired("server", 30), 0]
        self.env.close()
        self.process.terminate.assert_called_once()
        self.process.kill.assert_called_once()
        self.assertEqual(
            [mock.call(timeout=30), mock.call(timeout=10)],
            self.process.wait.call_args_list,
        )

    def test_exited_process_fails_readiness_immediately(self):
        self.env.process = self.process
        self.process.poll.return_value = 1
        with mock.patch.object(self.env.session, "get") as get:
            with self.assertRaisesRegex(RuntimeError, "exited before readiness"):
                self.env.wait_ready("http://127.0.0.1:18090/api/version")
            get.assert_not_called()
        self.env.close()

    def test_readiness_timeout_is_an_error(self):
        with self.assertRaisesRegex(RuntimeError, "readiness timed out"):
            self.env.wait_ready("http://127.0.0.1:18090/api/version", timeout_s=0)

    def test_iceberg_readiness_overrides_the_metadata_vendor_accept_header(self):
        response = mock.MagicMock(spec=requests.Response)
        response.__enter__.return_value = response
        response.status_code = 200
        with mock.patch.object(self.env.session, "send", return_value=response) as send:
            self.env.wait_ready(self.env.iceberg_url + "v1/config")
        prepared_request = send.call_args.args[0]
        self.assertEqual("application/json", prepared_request.headers["Accept"])
        self.assertEqual(
            "application/vnd.gravitino.v1+json", self.env.session.headers["Accept"]
        )

    def test_rest_error_code_is_rejected_even_for_http_success(self):
        response = mock.MagicMock()
        response.__enter__.return_value = response
        response.json.return_value = {"code": 1001}
        with mock.patch.object(self.env.session, "request", return_value=response):
            with self.assertRaisesRegex(RuntimeError, "Gravitino request failed"):
                self.env.request("POST", "/metalakes", {"name": "fixture"})

    def test_malformed_rest_response_is_a_visible_fixture_error(self):
        response = mock.MagicMock()
        response.__enter__.return_value = response
        response.json.return_value = None
        with mock.patch.object(self.env.session, "request", return_value=response):
            with self.assertRaisesRegex(RuntimeError, "Gravitino request failed"):
                self.env.request("DELETE", "/metalakes/fixture?force=true")

    def test_missing_dependencies_fail_in_the_dedicated_task(self):
        with (
            mock.patch.object(contract, "missing_dependencies", return_value=["daft"]),
            mock.patch.dict(os.environ, {"DAFT_ICEBERG_IT_REQUIRED": "true"}),
        ):
            with self.assertRaisesRegex(RuntimeError, "dependencies missing: daft"):
                contract.TestDaftIcebergIntegration.setUpClass()

    def test_default_discovery_can_omit_the_optional_contract(self):
        with (
            mock.patch.object(contract, "missing_dependencies", return_value=["daft"]),
            mock.patch.dict(os.environ, {"DAFT_ICEBERG_IT_REQUIRED": "false"}),
        ):
            with self.assertRaisesRegex(
                unittest.SkipTest, "dependencies missing: daft"
            ):
                contract.TestDaftIcebergIntegration.setUpClass()

    def test_default_discovery_does_not_start_a_second_server(self):
        with (
            mock.patch.object(contract, "missing_dependencies", return_value=[]),
            mock.patch.dict(os.environ, {"DAFT_ICEBERG_IT_REQUIRED": "false"}),
            mock.patch.object(contract, "DaftIcebergTestEnv") as fixture,
        ):
            with self.assertRaisesRegex(unittest.SkipTest, "daftIcebergIT task"):
                contract.TestDaftIcebergIntegration.setUpClass()
            fixture.assert_not_called()

    def test_termination_exits_the_driver_for_atexit_cleanup(self):
        with self.assertRaises(SystemExit) as raised:
            contract.exit_on_termination(15, None)
        self.assertEqual(143, raised.exception.code)
