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

import os
import subprocess
import sys
import unittest
from pathlib import Path


class TestClientImportWithoutStorageDependencies(unittest.TestCase):
    """Verify core imports do not eagerly load optional GVFS packages."""

    def setUp(self):
        self.client_root = Path(__file__).resolve().parents[2]

    def test_metadata_client_import_does_not_load_storage_packages(self):
        script = r"""
import builtins

blocked = {
    "cachetools",
    "fsspec",
    "gcsfs",
    "adlfs",
    "ossfs",
    "pyarrow",
    "readerwriterlock",
    "s3fs",
}
original_import = builtins.__import__

def import_without_storage(
    name, globals=None, locals=None, fromlist=(), level=0
):
    if name.split(".", maxsplit=1)[0] in blocked:
        missing_module = name.split(".", maxsplit=1)[0]
        raise ModuleNotFoundError(
            f"No module named '{name}'", name=missing_module
        )
    return original_import(name, globals, locals, fromlist, level)

builtins.__import__ = import_without_storage
from gravitino import GravitinoClient
assert GravitinoClient.__name__ == "GravitinoClient"
"""
        result = subprocess.run(
            [sys.executable, "-c", script],
            cwd=self.client_root,
            env=os.environ.copy(),
            capture_output=True,
            check=False,
            text=True,
        )

        self.assertEqual(result.returncode, 0, result.stderr)

    def test_gvfs_access_without_common_extra_has_install_guidance(self):
        script = r"""
import builtins

original_import = builtins.__import__

def import_without_fsspec(
    name, globals=None, locals=None, fromlist=(), level=0
):
    if name.split(".", maxsplit=1)[0] == "fsspec":
        raise ModuleNotFoundError("No module named 'fsspec'", name="fsspec")
    return original_import(name, globals, locals, fromlist, level)

builtins.__import__ = import_without_fsspec
try:
    from gravitino import gvfs
except ImportError as error:
    assert "apache-gravitino[gvfs]" in str(error)
    assert "apache-gravitino[storage]" in str(error)
else:
    raise AssertionError("Accessing GVFS without its extra should fail")
"""
        result = subprocess.run(
            [sys.executable, "-c", script],
            cwd=self.client_root,
            env=os.environ.copy(),
            capture_output=True,
            check=False,
            text=True,
        )

        self.assertEqual(result.returncode, 0, result.stderr)
