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

from gravitino.filesystem.gvfs_config import GVFSConfig
from gravitino.filesystem.gvfs_default_operations import DefaultGVFSOperations


# pylint: disable=protected-access
class TestGVFSUserDefinedConfigs(unittest.TestCase):
    """Test cases for _get_user_defined_configs method."""

    def test_get_user_defined_configs_single_location(self):
        """Test _get_user_defined_configs with single location."""
        options = {
            f"{GVFSConfig.FS_GRAVITINO_PATH_CONFIG_PREFIX}cluster1": "s3://bucket1",
            f"{GVFSConfig.FS_GRAVITINO_PATH_CONFIG_PREFIX}cluster1_key1": "value1",
            f"{GVFSConfig.FS_GRAVITINO_PATH_CONFIG_PREFIX}cluster1_key2": "value2",
        }
        operations = DefaultGVFSOperations(options=options)

        path = "s3://bucket1/path/to/file"
        configs = operations._get_user_defined_configs(path)

        self.assertEqual(len(configs), 2)
        self.assertEqual(configs["key1"], "value1")
        self.assertEqual(configs["key2"], "value2")

    def test_get_user_defined_configs_multiple_locations(self):
        """Test _get_user_defined_configs with multiple location configurations."""
        options = {
            f"{GVFSConfig.FS_GRAVITINO_PATH_CONFIG_PREFIX}cluster1": "s3://bucket1",
            f"{GVFSConfig.FS_GRAVITINO_PATH_CONFIG_PREFIX}cluster1_key1": "value1",
            f"{GVFSConfig.FS_GRAVITINO_PATH_CONFIG_PREFIX}cluster1_key2": "value2",
            f"{GVFSConfig.FS_GRAVITINO_PATH_CONFIG_PREFIX}cluster2": "s3://bucket2",
            f"{GVFSConfig.FS_GRAVITINO_PATH_CONFIG_PREFIX}cluster2_key1": "value3",
            f"{GVFSConfig.FS_GRAVITINO_PATH_CONFIG_PREFIX}cluster2_key2": "value4",
        }
        operations = DefaultGVFSOperations(options=options)

        # Test cluster1
        path1 = "s3://bucket1/path/to/file1"
        configs1 = operations._get_user_defined_configs(path1)
        self.assertEqual(len(configs1), 2)
        self.assertEqual(configs1["key1"], "value1")
        self.assertEqual(configs1["key2"], "value2")

        # Test cluster2
        path2 = "s3://bucket2/path/to/file2"
        configs2 = operations._get_user_defined_configs(path2)
        self.assertEqual(len(configs2), 2)
        self.assertEqual(configs2["key1"], "value3")
        self.assertEqual(configs2["key2"], "value4")

    def test_get_user_defined_configs_edge_cases(self):
        """Test _get_user_defined_configs with edge cases."""
        # Test with empty path
        options1 = {
            f"{GVFSConfig.FS_GRAVITINO_PATH_CONFIG_PREFIX}cluster1": "s3://bucket1",
            f"{GVFSConfig.FS_GRAVITINO_PATH_CONFIG_PREFIX}cluster1_key1": "value1",
        }
        operations1 = DefaultGVFSOperations(options=options1)
        self.assertEqual(operations1._get_user_defined_configs(""), {})
        self.assertEqual(operations1._get_user_defined_configs(None), {})

        # Test with no options
        operations2 = DefaultGVFSOperations(options=None)
        self.assertEqual(operations2._get_user_defined_configs("s3://bucket1/path"), {})

        # Test with empty options
        operations3 = DefaultGVFSOperations(options={})
        self.assertEqual(operations3._get_user_defined_configs("s3://bucket1/path"), {})

        # Test with no matching location
        options4 = {
            f"{GVFSConfig.FS_GRAVITINO_PATH_CONFIG_PREFIX}cluster1": "s3://bucket1",
            f"{GVFSConfig.FS_GRAVITINO_PATH_CONFIG_PREFIX}cluster1_key1": "value1",
        }
        operations4 = DefaultGVFSOperations(options=options4)
        self.assertEqual(operations4._get_user_defined_configs("s3://bucket2/path"), {})

        # Test with property without location definition
        options5 = {
            f"{GVFSConfig.FS_GRAVITINO_PATH_CONFIG_PREFIX}cluster1_key1": "value1",
        }
        operations5 = DefaultGVFSOperations(options=options5)
        self.assertEqual(operations5._get_user_defined_configs("s3://bucket1/path"), {})
