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

import re
import subprocess
import sys
import tempfile
import unittest
from email.parser import Parser
from pathlib import Path


class TestPackageMetadata(unittest.TestCase):
    """Verify the built package separates core and optional dependencies."""

    def test_core_requirements_and_storage_extras(self):
        client_root = Path(__file__).resolve().parents[2]
        with tempfile.TemporaryDirectory() as egg_base:
            result = subprocess.run(
                [
                    sys.executable,
                    "setup.py",
                    "egg_info",
                    "--egg-base",
                    egg_base,
                ],
                cwd=client_root,
                capture_output=True,
                check=False,
                text=True,
            )
            self.assertEqual(
                result.returncode,
                0,
                result.stdout + result.stderr,
            )

            metadata_files = list(Path(egg_base).glob("*.egg-info/PKG-INFO"))
            self.assertEqual(len(metadata_files), 1)
            metadata_content = metadata_files[0].read_text(encoding="utf-8")
            metadata = Parser().parsestr(metadata_content)
            source_files = set(
                (metadata_files[0].parent / "SOURCES.txt")
                .read_text(encoding="utf-8")
                .splitlines()
            )

        requirements = metadata.get_all("Requires-Dist", [])
        extra_marker = "extra =="
        base_requirements = [
            requirement
            for requirement in requirements
            if extra_marker not in requirement
        ]
        base_names = {
            re.split(r"[<>=!~; ]", requirement, maxsplit=1)[0]
            .strip()
            .lower()
            .replace("_", "-")
            for requirement in base_requirements
        }

        self.assertIn("dataclasses-json", base_names)
        self.assertIn("simplejson", base_names)
        for optional_name in (
            "black",
            "cachetools",
            "fsspec",
            "gcsfs",
            "adlfs",
            "ossfs",
            "pyarrow",
            "pre-commit",
            "readerwriterlock",
            "requests",
            "s3fs",
            "flake8",
        ):
            with self.subTest(requirement=optional_name):
                self.assertNotIn(optional_name, base_names)

        extras = set(metadata.get_all("Provides-Extra", []))
        self.assertTrue(
            {
                "dev",
                "lance",
                "gvfs",
                "hdfs",
                "s3",
                "gcs",
                "oss",
                "azure",
                "storage",
            }.issubset(extras)
        )

        for requirements_file in (
            "requirements.txt",
            "requirements-dev.txt",
            "requirements-gvfs.txt",
            "requirements-hdfs.txt",
            "requirements-s3.txt",
            "requirements-gcs.txt",
            "requirements-oss.txt",
            "requirements-azure.txt",
            "requirements-lance.txt",
        ):
            with self.subTest(source=requirements_file):
                self.assertIn(requirements_file, source_files)

        shared_gvfs_requirements = {"cachetools", "fsspec", "readerwriterlock"}

        def requirement_names_for_extra(extra_name):
            extra_requirements = [
                requirement
                for requirement in requirements
                if f'extra == "{extra_name}"' in requirement
            ]
            return {
                re.split(r"[<>=!~; ]", requirement, maxsplit=1)[0]
                .strip()
                .lower()
                .replace("_", "-")
                for requirement in extra_requirements
            }

        gvfs_requirements = requirement_names_for_extra("gvfs")
        self.assertTrue(shared_gvfs_requirements.issubset(gvfs_requirements))
        self.assertTrue(
            gvfs_requirements.isdisjoint({"pyarrow", "s3fs", "gcsfs", "ossfs", "adlfs"})
        )

        for extra_name, provider in (
            ("hdfs", "pyarrow"),
            ("s3", "s3fs"),
            ("gcs", "gcsfs"),
            ("oss", "ossfs"),
            ("azure", "adlfs"),
        ):
            with self.subTest(extra=extra_name):
                extra_requirements = requirement_names_for_extra(extra_name)
                self.assertTrue(shared_gvfs_requirements.issubset(extra_requirements))
                self.assertIn(provider, extra_requirements)

        storage_requirements = requirement_names_for_extra("storage")
        self.assertTrue(
            shared_gvfs_requirements.union(
                {"pyarrow", "s3fs", "gcsfs", "ossfs", "adlfs"}
            ).issubset(storage_requirements)
        )
