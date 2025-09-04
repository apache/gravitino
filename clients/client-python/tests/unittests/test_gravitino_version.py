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

from gravitino.client.gravitino_version import GravitinoVersion
from gravitino.dto.version_dto import VersionDTO
from gravitino.exceptions.base import BadRequestException, GravitinoRuntimeException


class TestGravitinoVersion(unittest.TestCase):
    def test_parse_version_string(self):
        # Test a valid the version string
        version = GravitinoVersion(VersionDTO("0.6.0", "2023-01-01", "1234567"))

        self.assertEqual(version.major, 0)
        self.assertEqual(version.minor, 6)
        self.assertEqual(version.patch, 0)

        # Test a valid the version string with hyphen separator
        version = GravitinoVersion(VersionDTO("0.6.0-0.6.0", "2023-01-01", "1234567"))

        self.assertEqual(version.major, 0)
        self.assertEqual(version.minor, 6)
        self.assertEqual(version.patch, 0)

        # Test a valid the version string with SNAPSHOT
        version = GravitinoVersion(
            VersionDTO("0.6.0-SNAPSHOT", "2023-01-01", "1234567")
        )

        self.assertEqual(version.major, 0)
        self.assertEqual(version.minor, 6)
        self.assertEqual(version.patch, 0)

        # Test a valid the version string with SNAPSHOT and hyphen separator
        version = GravitinoVersion(
            VersionDTO("0.6.0-SNAPSHOT-0.6.0-SNAPSHOT", "2023-01-01", "1234567")
        )

        self.assertEqual(version.major, 0)
        self.assertEqual(version.minor, 6)
        self.assertEqual(version.patch, 0)

        version = GravitinoVersion(
            VersionDTO(
                "0.9.0-incubating-SNAPSHOT-0.8.0-SNAPSHOT", "2023-01-01", "1234567"
            )
        )

        self.assertEqual(version.major, 0)
        self.assertEqual(version.minor, 9)
        self.assertEqual(version.patch, 0)

        # Test a valid the version string with alpha
        version = GravitinoVersion(VersionDTO("0.6.0-alpha", "2023-01-01", "1234567"))

        self.assertEqual(version.major, 0)
        self.assertEqual(version.minor, 6)
        self.assertEqual(version.patch, 0)

        # Test a valid the version string with alpha and hyphen separator
        version = GravitinoVersion(
            VersionDTO("0.6.0-alpha-0.6.0-alpha", "2023-01-01", "1234567")
        )

        self.assertEqual(version.major, 0)
        self.assertEqual(version.minor, 6)
        self.assertEqual(version.patch, 0)

        # Test a valid the version string with pypi format
        version = GravitinoVersion(VersionDTO("0.6.0.dev21", "2023-01-01", "1234567"))

        self.assertEqual(version.major, 0)
        self.assertEqual(version.minor, 6)
        self.assertEqual(version.patch, 0)

        # Test a valid the version string with pypi format and hyphen separator
        version = GravitinoVersion(
            VersionDTO("0.6.0.dev21-0.6.0.dev21", "2023-01-01", "1234567")
        )

        self.assertEqual(version.major, 0)
        self.assertEqual(version.minor, 6)
        self.assertEqual(version.patch, 0)

        # Test an invalid the version string with 2 part
        with self.assertRaises(BadRequestException):
            GravitinoVersion(VersionDTO("0.6", "2023-01-01", "1234567"))

        # Test an invalid the version string with 2 part and hyphen separator
        with self.assertRaises(BadRequestException):
            GravitinoVersion(VersionDTO("0.6-0.6", "2023-01-01", "1234567"))

        # Test an invalid the version string with not number
        with self.assertRaises(BadRequestException):
            GravitinoVersion(VersionDTO("a.b.c", "2023-01-01", "1234567"))

        # Test an invalid the version string with not number and hyphen separator
        with self.assertRaises(BadRequestException):
            GravitinoVersion(VersionDTO("a.b.c-a.b.c", "2023-01-01", "1234567"))

    def test_version_compare(self):
        # test equal
        version1 = GravitinoVersion(VersionDTO("0.6.0", "2023-01-01", "1234567"))
        version2 = GravitinoVersion(VersionDTO("0.6.0", "2023-01-01", "1234567"))

        self.assertEqual(version1, version2)

        version1 = GravitinoVersion(VersionDTO("0.6.0-0.7.0", "2023-01-01", "1234567"))
        version2 = GravitinoVersion(VersionDTO("0.6.0-0.7.0", "2023-01-01", "1234567"))

        self.assertEqual(version1, version2)

        # test less than
        version1 = GravitinoVersion(VersionDTO("0.6.0", "2023-01-01", "1234567"))
        version2 = GravitinoVersion(VersionDTO("0.12.0", "2023-01-01", "1234567"))

        self.assertLess(version1, version2)

        version1 = GravitinoVersion(VersionDTO("0.6.0-0.12.0", "2023-01-01", "1234567"))
        version2 = GravitinoVersion(VersionDTO("0.12.0-0.6.0", "2023-01-01", "1234567"))

        self.assertLess(version1, version2)

        # test greater than
        version1 = GravitinoVersion(VersionDTO("1.6.0", "2023-01-01", "1234567"))
        version2 = GravitinoVersion(VersionDTO("0.6.0", "2023-01-01", "1234567"))

        self.assertGreater(version1, version2)

        version1 = GravitinoVersion(VersionDTO("1.6.0-0.6.0", "2023-01-01", "1234567"))
        version2 = GravitinoVersion(VersionDTO("0.6.0-1.6.0", "2023-01-01", "1234567"))

        self.assertGreater(version1, version2)

        # version1.minor < version2.minor and version1.patch > version.patch
        version1 = GravitinoVersion(VersionDTO("0.6.1", "2023-01-01", "1234567"))
        version2 = GravitinoVersion(VersionDTO("0.7.0", "2023-01-01", "1234567"))

        self.assertFalse(version1 > version2)
        self.assertGreater(version2, version1)

        version1 = GravitinoVersion(VersionDTO("0.6.1-0.7.0", "2023-01-01", "1234567"))
        version2 = GravitinoVersion(VersionDTO("0.7.0-0.6.1", "2023-01-01", "1234567"))

        self.assertFalse(version1 > version2)
        self.assertGreater(version2, version1)

        # test equal with suffix
        version1 = GravitinoVersion(
            VersionDTO("0.6.0-SNAPSHOT", "2023-01-01", "1234567")
        )
        version2 = GravitinoVersion(VersionDTO("0.6.0", "2023-01-01", "1234567"))

        self.assertEqual(version1, version2)

        version1 = GravitinoVersion(
            VersionDTO("0.6.0-SNAPSHOT-0.6.0", "2023-01-01", "1234567")
        )
        version2 = GravitinoVersion(
            VersionDTO("0.6.0-0.6.0-SNAPSHOT", "2023-01-01", "1234567")
        )

        self.assertEqual(version1, version2)

        # test compare with other class

        version1 = GravitinoVersion(VersionDTO("0.6.0", "2023-01-01", "1234567"))
        version2 = "0.6.0"

        self.assertRaises(GravitinoRuntimeException, version1.__eq__, version2)
        self.assertRaises(GravitinoRuntimeException, version1.__gt__, version2)

        version1 = GravitinoVersion(VersionDTO("0.6.0-0.6.0", "2023-01-01", "1234567"))
        version2 = "0.6.0-0.6.0"

        self.assertRaises(GravitinoRuntimeException, version1.__eq__, version2)
        self.assertRaises(GravitinoRuntimeException, version1.__gt__, version2)

    def test_compatible_server_version(self):
        version1 = GravitinoVersion(VersionDTO("1.6.0", "2023-01-01", "1234567"))
        version2 = GravitinoVersion(VersionDTO("1.6.0", "2023-01-01", "1234567"))
        self.assertTrue(version1.compatible_with_server_version(version2))

        version1 = GravitinoVersion(VersionDTO("1.6.1", "2023-01-01", "1234567"))
        version2 = GravitinoVersion(VersionDTO("1.6.2", "2023-01-01", "1234567"))
        self.assertTrue(version1.compatible_with_server_version(version2))

        version1 = GravitinoVersion(VersionDTO("1.6.2", "2023-01-01", "1234567"))
        version2 = GravitinoVersion(VersionDTO("1.6.1", "2023-01-01", "1234567"))
        self.assertTrue(version1.compatible_with_server_version(version2))

        version1 = GravitinoVersion(VersionDTO("1.6.0", "2023-01-01", "1234567"))
        version2 = GravitinoVersion(VersionDTO("2.6.1", "2023-01-01", "1234567"))
        self.assertTrue(version1.compatible_with_server_version(version2))

        version1 = GravitinoVersion(VersionDTO("1.6.0", "2023-01-01", "1234567"))
        version2 = GravitinoVersion(VersionDTO("1.4.0", "2023-01-01", "1234567"))
        self.assertFalse(version1.compatible_with_server_version(version2))

        version1 = GravitinoVersion(VersionDTO("1.6.0", "2023-01-01", "1234567"))
        version2 = GravitinoVersion(VersionDTO("0.6.0", "2023-01-01", "1234567"))
        self.assertFalse(version1.compatible_with_server_version(version2))
