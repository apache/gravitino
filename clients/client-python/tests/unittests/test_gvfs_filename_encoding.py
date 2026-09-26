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
"""Tests for file name encoding and special-character handling in GVFS.

See https://github.com/apache/gravitino/issues/5869. These tests verify that
the GVFS path-conversion layer round-trips file names containing spaces,
URL-significant ASCII characters, multi-byte Unicode (CJK / accents / Cyrillic),
emoji, and path-structure characters without garbling them.

The conversion functions exercised here are effectively pure given their string
arguments, so the local storage handler (``LOCA_HANDLER``) and the
``gvfs_utils`` helpers are called directly without mocking a Gravitino server.

Finding: because Python 3 ``str`` is Unicode, every special-character category
below converts and round-trips correctly. The single confirmed defect is the
all-occurrences ``str.replace`` in ``actual_path_to_gvfs_path`` (documented via
``unittest.expectedFailure`` below); fixing it is left to a follow-up under the
parent epic #5504.
"""

import unittest

from gravitino import NameIdentifier
from gravitino.filesystem.gvfs_storage_handler import LOCA_HANDLER
from gravitino.filesystem.gvfs_utils import (
    extract_identifier,
    get_sub_path_from_virtual_path,
    to_gvfs_path_prefix,
)

# pylint: disable=protected-access

# Special-character categories covered by issue #5869.
_SPACES_AND_ASCII_SPECIALS = [
    "my file.txt",
    "a#1.txt",
    "a%20.txt",
    "a&b.txt",
    "q?x.txt",
    "a+b.txt",
    "a=b.txt",
    "a@b.txt",
    "a!b.txt",
]
_UNICODE_SCRIPTS = [
    "日本語.txt",  # CJK
    "café.txt",  # accented Latin
    "файл.txt",  # Cyrillic
    "مرحبا.txt",  # Arabic (RTL)
]
_EMOJIS = [
    "🎉.txt",  # astral-plane / surrogate pair
    "📁dir",
    "a🎉b.txt",
]
_PATH_STRUCTURE = [
    "a%2Fb.txt",  # encoded slash, must stay literal
    ".hidden",  # leading dot
    "trailing.",  # trailing dot
    "x" * 300,  # very long name (> 255 chars)
]

_ALL_SPECIAL_NAMES = (
    _SPACES_AND_ASCII_SPECIALS + _UNICODE_SCRIPTS + _EMOJIS + _PATH_STRUCTURE
)

_METALAKE = "metalake_demo"
_FILESET_LOCATION = "file:/tmp/test_fileset/"
_ACTUAL_PREFIX = "/tmp/test_fileset/"
_GVFS_PREFIX = "fileset/test_catalog/test_schema/test_fileset"


class TestGvfsFilenameEncoding(unittest.TestCase):
    """Document GVFS special-character / encoding behavior for issue #5869."""

    def test_actual_path_to_gvfs_path_special_filenames(self):
        """A special-character file name maps back to the matching gvfs path."""
        for name in _ALL_SPECIAL_NAMES:
            with self.subTest(name=name):
                actual_path = f"{_ACTUAL_PREFIX}{name}"
                gvfs_path = LOCA_HANDLER.actual_path_to_gvfs_path(
                    actual_path, _FILESET_LOCATION, _GVFS_PREFIX
                )
                self.assertEqual(f"{_GVFS_PREFIX}/{name}", gvfs_path)

    def test_actual_path_to_gvfs_path_special_nested_dirs(self):
        """Special characters inside nested directory segments survive."""
        for name in _ALL_SPECIAL_NAMES:
            with self.subTest(name=name):
                actual_path = f"{_ACTUAL_PREFIX}{name}/inner/data.txt"
                gvfs_path = LOCA_HANDLER.actual_path_to_gvfs_path(
                    actual_path, _FILESET_LOCATION, _GVFS_PREFIX
                )
                self.assertEqual(f"{_GVFS_PREFIX}/{name}/inner/data.txt", gvfs_path)

    def test_actual_info_to_gvfs_info_special_filenames(self):
        """File-info conversion rewrites the name and passes metadata through."""
        for name in _ALL_SPECIAL_NAMES:
            with self.subTest(name=name):
                entry = {
                    "name": f"{_ACTUAL_PREFIX}{name}",
                    "size": 1024,
                    "type": "file",
                    "mtime": 1700000000,
                }
                info = LOCA_HANDLER.actual_info_to_gvfs_info(
                    entry, _FILESET_LOCATION, _GVFS_PREFIX
                )
                self.assertEqual(f"{_GVFS_PREFIX}/{name}", info["name"])
                self.assertEqual(1024, info["size"])
                self.assertEqual("file", info["type"])
                self.assertEqual(1700000000, info["mtime"])

    def test_round_trip_special_chars_in_sub_path(self):
        """A gvfs path with a special-character sub-path round-trips cleanly."""
        identifier = NameIdentifier.of(
            _METALAKE, "test_catalog", "test_schema", "test_fileset"
        )
        prefix = to_gvfs_path_prefix(identifier)
        for name in _ALL_SPECIAL_NAMES:
            with self.subTest(name=name):
                sub_path = f"/{name}"
                virtual_path = f"{prefix}{sub_path}"

                extracted = extract_identifier(_METALAKE, virtual_path)
                self.assertEqual("test_fileset", extracted.name())
                self.assertEqual(
                    sub_path,
                    get_sub_path_from_virtual_path(extracted, virtual_path),
                )

    def test_round_trip_special_chars_in_fileset_name(self):
        """Special characters in the fileset name segment are preserved."""
        # '/' is excluded: it is the path separator and cannot appear in a
        # single name segment. Everything else round-trips.
        fileset_names = [n for n in _ALL_SPECIAL_NAMES if "/" not in n]
        for fileset_name in fileset_names:
            with self.subTest(fileset_name=fileset_name):
                identifier = NameIdentifier.of(
                    _METALAKE, "test_catalog", "test_schema", fileset_name
                )
                virtual_path = f"{to_gvfs_path_prefix(identifier)}/data.txt"

                extracted = extract_identifier(_METALAKE, virtual_path)
                self.assertEqual(fileset_name, extracted.name())
                self.assertEqual(
                    "/data.txt",
                    get_sub_path_from_virtual_path(extracted, virtual_path),
                )

    @unittest.expectedFailure
    def test_actual_path_to_gvfs_path_recurring_prefix_segment(self):
        """Documents a confirmed defect for issue #5869.

        ``actual_path_to_gvfs_path`` uses ``str.replace(actual_prefix, ...)``,
        which replaces *every* occurrence of the prefix string rather than only
        the leading one. When a later directory segment matches the prefix
        (here a sub-directory literally named ``data`` under a fileset rooted at
        ``file:/data/``), the conversion corrupts the path:

            got      'fileset/.../sub' + 'fileset/.../file'
            expected 'fileset/.../sub/data/file'

        A prefix-only replacement (slicing after the verified ``startswith``)
        would fix it. Left to a follow-up under the parent epic #5504.
        """
        gvfs_path = LOCA_HANDLER.actual_path_to_gvfs_path(
            "/data/sub/data/file", "file:/data/", _GVFS_PREFIX
        )
        self.assertEqual(f"{_GVFS_PREFIX}/sub/data/file", gvfs_path)


if __name__ == "__main__":
    unittest.main()
