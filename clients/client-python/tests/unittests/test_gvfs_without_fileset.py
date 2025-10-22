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
from unittest.mock import patch

from gravitino.exceptions.base import NoSuchFilesetException
from gravitino.filesystem.gvfs import GravitinoVirtualFileSystem


class TestGVFSWithoutFileset(unittest.TestCase):

    @patch.object(GravitinoVirtualFileSystem, "_get_gvfs_operations_class")
    def test_when_fileset_not_created(self, mock_get_gvfs_operations):
        mock_operations = unittest.mock.MagicMock()
        methods = [
            "ls",
            "info",
            "exists",
            "cp_file",
            "mv",
            "rm",
            "rm_file",
            "rmdir",
            "open",
            "mkdir",
            "makedirs",
            "created",
            "modified",
            "cat_file",
            "get_file",
        ]
        for method in methods:
            getattr(mock_operations, method).side_effect = NoSuchFilesetException(
                f"{method} failed"
            )
        mock_get_gvfs_operations.return_value = mock_operations

        fs = GravitinoVirtualFileSystem(
            server_uri="http://localhost:9090",
            metalake_name="metalake_demo",
            skip_instance_cache=True,
        )

        self.assertRaises(
            FileNotFoundError, fs.ls, "fileset/test_catalog/schema/fileset"
        )
        self.assertRaises(
            FileNotFoundError, fs.info, "fileset/test_catalog/schema/fileset"
        )

        self.assertFalse(fs.exists("fileset/test_catalog/schema/fileset"))

        with self.assertRaises(FileNotFoundError):
            fs.cp_file(
                "fileset/test_catalog/schema/fileset/a",
                "fileset/test_catalog/schema/fileset/b",
            )

        self.assertRaises(
            FileNotFoundError, fs.rm, "fileset/test_catalog/schema/fileset/a"
        )
        self.assertRaises(
            FileNotFoundError,
            fs.rm_file,
            "fileset/test_catalog/schema/fileset/a",
        )
        self.assertRaises(
            FileNotFoundError, fs.rmdir, "fileset/test_catalog/schema/fileset/a"
        )

        self.assertRaises(OSError, fs.open, "fileset/test_catalog/schema/fileset", "w")
        self.assertRaises(OSError, fs.open, "fileset/test_catalog/schema/fileset", "wb")
        self.assertRaises(OSError, fs.open, "fileset/test_catalog/schema/fileset", "a")
        self.assertRaises(OSError, fs.open, "fileset/test_catalog/schema/fileset", "ab")
        self.assertRaises(OSError, fs.open, "fileset/test_catalog/schema/fileset", "x")
        self.assertRaises(OSError, fs.open, "fileset/test_catalog/schema/fileset", "xb")

        self.assertRaises(
            FileNotFoundError,
            fs.open,
            "fileset/test_catalog/schema/fileset/a",
            "r",
        )
        self.assertRaises(
            FileNotFoundError,
            fs.open,
            "fileset/test_catalog/schema/fileset/a",
            "rb",
        )

        self.assertRaises(OSError, fs.mkdir, "fileset/test_catalog/schema/fileset/a")
        self.assertRaises(
            OSError, fs.makedirs, "fileset/test_catalog/schema/fileset/a/b"
        )

        self.assertRaises(
            FileNotFoundError,
            fs.created,
            "fileset/test_catalog/schema/fileset/a",
        )
        self.assertRaises(
            FileNotFoundError,
            fs.modified,
            "fileset/test_catalog/schema/fileset/a",
        )

        self.assertRaises(
            FileNotFoundError,
            fs.cat_file,
            "fileset/test_catalog/schema/fileset/a",
        )

        with self.assertRaises(FileNotFoundError):
            fs.get("fileset/test_catalog/schema/fileset/a", "file://tmp/a")
