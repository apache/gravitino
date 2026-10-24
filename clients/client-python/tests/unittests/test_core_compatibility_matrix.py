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

"""Failure and isolation tests for the installed-core compatibility runner."""

import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
from zipfile import ZipFile

from tests.compatibility import test_core_metadata as metadata_tests

SCRIPT = (
    Path(__file__).resolve().parents[2] / "scripts/run_core_compatibility_matrix.py"
)
SPEC = importlib.util.spec_from_file_location("core_compatibility_matrix", SCRIPT)
MATRIX = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MATRIX
SPEC.loader.exec_module(MATRIX)


class TestCoreCompatibilityMatrix(unittest.TestCase):
    """Reject false compatibility evidence and preserve useful failure reports."""

    def test_clean_environment_removes_source_and_version_overrides(self):
        """Inherited overrides must not invalidate installed-package evidence."""
        with patch.dict(
            os.environ,
            {
                "PYTHONPATH": "/sdk/source",
                "PYTHONHOME": "/different/python",
                "GRAVITINO_VERSION_CHECK_DISABLED": "true",
            },
        ):
            environment = MATRIX.clean_environment()
        self.assertNotIn("PYTHONPATH", environment)
        self.assertNotIn("PYTHONHOME", environment)
        self.assertNotIn("GRAVITINO_VERSION_CHECK_DISABLED", environment)
        self.assertEqual(environment["PYTHONNOUSERSITE"], "1")
        self.assertIn("127.0.0.1", environment["NO_PROXY"])
        self.assertEqual(environment["NO_PROXY"], environment["no_proxy"])

    def test_probe_rejects_wrong_versions_interpreters_and_source_imports(self):
        """A successful import is insufficient without exact environment assertions."""
        case = MATRIX.CASES[0]
        probe = {
            "python": [3, 10],
            "prefix": "/case/venv",
            "sdk_path": "/case/venv/lib/gravitino/__init__.py",
            "versions": {
                "apache-gravitino": "2.0.0.dev0",
                "dataclasses-json": "0.6.7",
                "simplejson": "3.20.1",
            },
        }
        self.assertEqual(
            MATRIX.assert_probe(probe, case, "2.0.0.dev0"), probe["versions"]
        )
        with self.subTest("source checkout"):
            invalid = {**probe, "sdk_path": "/checkout/gravitino/__init__.py"}
            with self.assertRaisesRegex(ValueError, "outside"):
                MATRIX.assert_probe(invalid, case, "2.0.0.dev0")
        with self.subTest("wrong Python"):
            with self.assertRaisesRegex(ValueError, "Python"):
                MATRIX.assert_probe({**probe, "python": [3, 12]}, case, "2.0.0.dev0")
        with self.subTest("resolver selected another version"):
            invalid = {
                **probe,
                "versions": {**probe["versions"], "simplejson": "4.2.0"},
            }
            with self.assertRaisesRegex(ValueError, "simplejson"):
                MATRIX.assert_probe(invalid, case, "2.0.0.dev0")

    def test_failed_command_keeps_output_and_fails(self):
        """Resolver and test failures remain visible in a retained execution log."""
        result = subprocess.CompletedProcess(["pip"], 1, "resolving\n", "conflict\n")
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            log = directory / "execution.log"
            with patch.object(MATRIX.subprocess, "run", return_value=result):
                with self.assertRaisesRegex(RuntimeError, "exit 1"):
                    MATRIX.run_command(["pip"], directory, log, {})
            content = log.read_text(encoding="utf-8")
        self.assertIn("resolving", content)
        self.assertIn("conflict", content)
        self.assertIn("exit_code=1", content)

    def test_test_workspace_does_not_copy_the_sdk(self):
        """Only test fixtures are available from the isolated working directory."""
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            source = directory / "source"
            (source / "tests").mkdir(parents=True)
            (source / "tests/test_fixture.py").write_text("fixture", encoding="utf-8")
            (source / "gravitino").mkdir()
            (source / "gravitino/__init__.py").write_text("sdk", encoding="utf-8")
            workspace = directory / "workspace"
            with patch.object(MATRIX, "CLIENT_ROOT", source):
                MATRIX.prepare_tests(workspace)
            self.assertTrue((workspace / "tests/test_fixture.py").is_file())
            self.assertFalse((workspace / "gravitino").exists())

    def test_same_version_wheel_replaces_installed_content(self):
        """A real pip install must consume new bytes despite an unchanged version."""
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            python = MATRIX.prepare_environment(sys.executable, directory / "venv")
            environment = MATRIX.clean_environment()
            log = directory / "install.log"
            for marker in ("old", "new-content"):
                wheel_dir = directory / marker
                wheel_dir.mkdir()
                wheel = wheel_dir / "apache_gravitino-2.0.0.dev0-py3-none-any.whl"
                metadata_dir = "apache_gravitino-2.0.0.dev0.dist-info"
                with ZipFile(wheel, "w") as archive:
                    archive.writestr("gravitino/__init__.py", f"MARKER = {marker!r}\n")
                    archive.writestr(
                        f"{metadata_dir}/METADATA",
                        "Metadata-Version: 2.1\nName: apache-gravitino\nVersion: 2.0.0.dev0\n",
                    )
                    archive.writestr(
                        f"{metadata_dir}/WHEEL",
                        "Wheel-Version: 1.0\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
                    )
                    archive.writestr(f"{metadata_dir}/RECORD", "")
                MATRIX.install_artifact(python, wheel, [], directory, log, environment)
                actual = MATRIX.run_command(
                    [str(python), "-c", "import gravitino; print(gravitino.MARKER)"],
                    directory,
                    log,
                    environment,
                )
                self.assertEqual(actual.strip(), marker)

    def test_owned_server_preserves_an_existing_listener(self):
        """An occupied endpoint is rejected without starting or stopping a service."""
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            with (
                patch.object(MATRIX.socket, "create_connection") as connection,
                patch.object(MATRIX.subprocess, "Popen") as start,
            ):
                with self.assertRaisesRegex(RuntimeError, "already in use"):
                    with MATRIX.owned_server(
                        directory, "http://localhost:8090", directory
                    ):
                        self.fail("An existing service must not be reused")
                connection.return_value.close.assert_called_once()
                start.assert_not_called()

    def test_owned_server_cleans_up_startup_and_test_failures(self):
        """Readiness and matrix exceptions both terminate only the owned process."""
        for startup_failure in (True, False):
            with (
                self.subTest(startup_failure=startup_failure),
                tempfile.TemporaryDirectory() as temporary,
            ):
                directory = Path(temporary)
                process = Mock()
                process.poll.return_value = None
                with (
                    patch.object(
                        MATRIX.socket,
                        "create_connection",
                        side_effect=ConnectionRefusedError,
                    ),
                    patch.object(MATRIX.subprocess, "Popen", return_value=process),
                    patch.object(
                        MATRIX,
                        "wait_for_server",
                        side_effect=(
                            RuntimeError("startup") if startup_failure else None
                        ),
                    ),
                ):
                    with self.assertRaisesRegex(RuntimeError, "startup|test"):
                        with MATRIX.owned_server(
                            directory, "http://localhost:8090", directory
                        ):
                            raise RuntimeError("test")
                    process.terminate.assert_called_once()
                    process.wait.assert_called_once_with(timeout=10)
                    process.kill.assert_not_called()

    def test_owned_server_kills_only_its_unresponsive_process(self):
        """Bounded shutdown escalates for the owned process when termination stalls."""
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            process = Mock()
            process.poll.return_value = None
            process.wait.side_effect = [subprocess.TimeoutExpired("server", 10), 0]
            with (
                patch.object(
                    MATRIX.socket,
                    "create_connection",
                    side_effect=ConnectionRefusedError,
                ),
                patch.object(MATRIX.subprocess, "Popen", return_value=process) as start,
                patch.object(MATRIX, "wait_for_server"),
            ):
                with MATRIX.owned_server(directory, "http://localhost:8090", directory):
                    pass
                process.terminate.assert_called_once()
                process.kill.assert_called_once()
                self.assertEqual(process.wait.call_count, 2)
                environment = start.call_args.kwargs["env"]
                self.assertEqual(environment["GRAVITINO_HOME"], str(directory))
                self.assertEqual(
                    environment["GRAVITINO_CONF_DIR"], str(directory / "conf")
                )
                self.assertEqual(
                    environment["GRAVITINO_LOG_DIR"], str(directory / "logs")
                )

    def test_readiness_rejects_early_exit_and_timeout(self):
        """A dead or unready server cannot produce compatibility results."""
        process = Mock()
        with patch.object(MATRIX.time, "monotonic", return_value=0):
            process.poll.return_value = 1
            with self.assertRaisesRegex(RuntimeError, "exited"):
                MATRIX.wait_for_server(
                    process, "http://localhost:8090", Path("server.log")
                )
        with patch.object(MATRIX.time, "monotonic", side_effect=[0, 31]):
            with self.assertRaisesRegex(RuntimeError, "timed out"):
                MATRIX.wait_for_server(
                    process, "http://localhost:8090", Path("server.log")
                )

    def test_provenance_accepts_build_venv_and_rejects_source(self):
        """A venv under build is an installed artifact, not the SDK source package."""
        for sdk, prefix, expected in (
            (
                "/checkout/build/venv/lib/gravitino/__init__.py",
                "/checkout/build/venv",
                "provenance accepted",
            ),
            ("/checkout/gravitino/__init__.py", "/checkout", "checkout must not"),
            ("/other/gravitino/__init__.py", "/checkout/build/venv", "installed SDK"),
        ):
            with (
                self.subTest(sdk=sdk),
                patch.object(metadata_tests.gravitino, "__file__", sdk),
                patch.object(metadata_tests.sys, "prefix", prefix),
                patch.dict(
                    os.environ,
                    {
                        "GRAVITINO_CORE_COMPATIBILITY_SOURCE": "/checkout",
                        "GRAVITINO_CORE_COMPATIBILITY_URI": "http://localhost:8090",
                    },
                ),
                patch.object(
                    metadata_tests,
                    "GravitinoAdminClient",
                    side_effect=RuntimeError("provenance accepted"),
                ),
            ):
                with self.assertRaisesRegex((ValueError, RuntimeError), expected):
                    metadata_tests.TestInstalledCoreMetadata.setUpClass()

    def test_matrix_stops_and_records_the_first_failure(self):
        """A failed case cannot be reported as success or trigger later cases."""
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            artifact = directory / "sdk.whl"
            artifact.write_bytes(b"artifact")
            options = SimpleNamespace(
                artifact=artifact,
                matrix_dir=directory,
                python=sys.executable,
                gravitino_home=None,
                selected=["core-min", "ray-arrow17"],
            )
            with (
                patch.object(MATRIX, "parse_args", return_value=options),
                patch.object(MATRIX, "build_wheel", return_value=artifact),
                patch.object(
                    MATRIX, "run_case", side_effect=RuntimeError("resolver conflict")
                ) as run_case,
            ):
                self.assertEqual(MATRIX.main(), 1)
            run_case.assert_called_once()
            report = json.loads(
                (directory / "results.json").read_text(encoding="utf-8")
            )
        self.assertEqual(report["results"][0]["status"], "failed")
        self.assertIn("resolver conflict", report["results"][0]["error"])

    def test_wheel_build_failure_cannot_leave_old_pass_results(self):
        """Artifact build errors must invalidate results from an earlier run."""
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            artifact = directory / "sdk.tar.gz"
            artifact.write_bytes(b"artifact")
            (directory / "results.json").write_text(
                '{"results": [{"status": "passed"}]}', encoding="utf-8"
            )
            options = SimpleNamespace(
                artifact=artifact, matrix_dir=directory, python=sys.executable
            )
            with (
                patch.object(MATRIX, "parse_args", return_value=options),
                patch.object(MATRIX, "build_wheel", side_effect=RuntimeError("build")),
                patch.object(MATRIX, "run_case") as run_case,
            ):
                with self.assertRaisesRegex(RuntimeError, "build"):
                    MATRIX.main()
                run_case.assert_not_called()
            report = json.loads(
                (directory / "results.json").read_text(encoding="utf-8")
            )
        self.assertEqual(report["results"], [])
        self.assertNotIn("wheel_sha256", report)

    def test_startup_failure_cannot_leave_old_pass_results(self):
        """A reused output directory must not publish successes from an older run."""
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            artifact = directory / "sdk.whl"
            artifact.write_bytes(b"artifact")
            (directory / "results.json").write_text(
                '{"results": [{"status": "passed"}]}', encoding="utf-8"
            )
            options = SimpleNamespace(
                artifact=artifact,
                matrix_dir=directory,
                python=sys.executable,
                selected=["core-min"],
                gravitino_home=directory,
                gravitino_uri="http://localhost:8090",
            )
            with (
                patch.object(MATRIX, "parse_args", return_value=options),
                patch.object(MATRIX, "build_wheel", return_value=artifact),
                patch.object(
                    MATRIX, "owned_server", side_effect=RuntimeError("startup")
                ),
                patch.object(MATRIX, "run_case") as run_case,
            ):
                with self.assertRaisesRegex(RuntimeError, "startup"):
                    MATRIX.main()
                run_case.assert_not_called()
            report = json.loads(
                (directory / "results.json").read_text(encoding="utf-8")
            )
            self.assertEqual(report["results"], [])


if __name__ == "__main__":
    unittest.main()
