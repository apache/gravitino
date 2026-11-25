#
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
#

import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


class TestDockerEntrypoint(unittest.TestCase):
    LIB_DIRS = (
        "catalogs/lakehouse-iceberg/libs",
        "iceberg-rest-server/libs",
        "iceberg-rest-server-experiment/libs",
    )

    def setUp(self):
        temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(temporary_directory.cleanup)
        self.home = Path(temporary_directory.name) / "gravitino with spaces"
        (self.home / "docker").mkdir(parents=True)
        (self.home / "bin").mkdir()
        self.entrypoint = self.home / "docker/docker-entrypoint.sh"
        shutil.copyfile(
            Path(__file__).resolve().parents[1] / "docker-entrypoint.sh",
            self.entrypoint,
        )
        server = self.home / "bin/gravitino.sh"
        server.write_text(
            '#!/bin/bash\nprintf "%s\\n" "$@" "$JAVA_OPTS" > server-invocation\n',
            encoding="utf-8",
        )
        server.chmod(0o755)

    def test_bundle_jars_are_linked_into_all_iceberg_classpaths(self):
        bundles = self.home / "iceberg-bundles"
        bundles.mkdir()
        for provider in ("aws", "gcp", "azure", "aliyun"):
            (bundles / f"gravitino-iceberg-{provider}-bundle.jar").touch()
        (bundles / "README.txt").touch()

        self.run_entrypoint()

        for relative_directory in self.LIB_DIRS:
            directory = self.home / relative_directory
            self.assertTrue(directory.is_dir(), str(directory))
            self.assertEqual(4, len(list(directory.iterdir())))
            for jar in bundles.glob("*.jar"):
                link = directory / jar.name
                self.assertTrue(link.is_symlink(), str(link))
                self.assertEqual(jar, link.resolve())

    def test_repeated_startup_refreshes_bundle_links(self):
        bundles = self.home / "iceberg-bundles"
        bundles.mkdir()
        jar = bundles / "gravitino-iceberg-aws-bundle.jar"
        jar.touch()
        self.run_entrypoint()
        for relative_directory in self.LIB_DIRS:
            link = self.home / relative_directory / jar.name
            self.assertTrue(link.is_symlink(), str(link))
            link.unlink()
            link.symlink_to(self.home / "missing.jar")

        self.run_entrypoint()

        for relative_directory in self.LIB_DIRS:
            self.assertEqual(jar, (self.home / relative_directory / jar.name).resolve())

    def test_startup_without_optional_bundles(self):
        self.run_entrypoint()
        for relative_directory in self.LIB_DIRS:
            self.assertFalse((self.home / relative_directory).exists())

    def test_startup_with_empty_bundle_directory(self):
        (self.home / "iceberg-bundles").mkdir()
        self.run_entrypoint()
        for relative_directory in self.LIB_DIRS:
            self.assertTrue((self.home / relative_directory).is_dir())
            self.assertEqual([], list((self.home / relative_directory).iterdir()))

    def test_existing_server_libraries_are_preserved(self):
        (self.home / "iceberg-bundles").mkdir()
        (self.home / "iceberg-bundles/cloud-bundle.jar").touch()
        for relative_directory in self.LIB_DIRS:
            directory = self.home / relative_directory
            directory.mkdir(parents=True)
            (directory / "server.jar").write_bytes(b"existing server library")

        self.run_entrypoint()

        for relative_directory in self.LIB_DIRS:
            self.assertEqual(
                b"existing server library",
                (self.home / relative_directory / "server.jar").read_bytes(),
            )
            self.assertTrue(
                (self.home / relative_directory / "cloud-bundle.jar").is_symlink()
            )

    def test_jdbc_driver_links(self):
        drivers = self.home / "jdbc-drivers"
        drivers.mkdir()
        for name in ("mysql-connector-java-8.0.27.jar", "postgresql-42.7.14.jar"):
            (drivers / name).touch()

        self.run_entrypoint()

        for jar in drivers.iterdir():
            link = self.home / "libs" / jar.name
            self.assertTrue(link.is_symlink())
            self.assertEqual(jar, link.resolve())

    def run_entrypoint(self):
        result = subprocess.run(
            ["bash", str(self.entrypoint)],
            env={**os.environ, "SKIP_CONFIG_REWRITE": "true", "JAVA_OPTS": "-Xms64m"},
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertEqual(
            ["run", "-Xms64m -XX:-UseContainerSupport"],
            (self.home / "server-invocation").read_text(encoding="utf-8").splitlines(),
        )


if __name__ == "__main__":
    unittest.main()
