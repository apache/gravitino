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

"""Tests for the tool filtering driven by --include-tool-tags."""

import asyncio
import unittest
from typing import Optional, Set

from mcp_server.client.factory import RESTClientFactory
from mcp_server.client.plain.plain_rest_client_operation import (
    PlainRESTClientOperation,
)
from mcp_server.core.setting import Setting
from mcp_server.server import GravitinoMCPServer
from mcp_server.tools import SUPPORTED_TOOL_TAGS
from tests.unit.tools import MockOperation


class TestIncludeToolTags(unittest.TestCase):
    """--include-tool-tags acts as an allowlist over the registered tools."""

    def setUp(self):
        RESTClientFactory.set_rest_client(MockOperation)

    def tearDown(self):
        RESTClientFactory.set_rest_client(PlainRESTClientOperation)

    def _tools(self, tags: Optional[Set[str]] = None) -> list:
        setting = Setting(metalake="ml", tags=tags or set())
        server = GravitinoMCPServer(setting)
        return asyncio.run(server.mcp.list_tools())

    def _names(self, tags: Optional[Set[str]] = None) -> Set[str]:
        return {tool.name for tool in self._tools(tags)}

    def _registered_tags(self) -> Set[str]:
        tags = set()
        for tool in self._tools():
            tags.update(tool.tags)
        return tags

    def test_no_tags_disables_filtering(self):
        unfiltered = self._names()
        by_tag = {tag: self._names({tag}) for tag in SUPPORTED_TOOL_TAGS}
        self.assertEqual(set().union(*by_tag.values()), unfiltered)
        # Equality alone holds if one tag no-ops, so require each to narrow.
        for tag, subset in by_tag.items():
            self.assertLess(subset, unfiltered, tag)

    def test_single_tag_exposes_only_matching_tools(self):
        tools = self._tools({"view"})
        self.assertTrue(tools)
        for tool in tools:
            self.assertIn("view", tool.tags)
        self.assertLess(len(tools), len(self._tools()))

    def test_multiple_tags_are_unioned(self):
        combined = self._names({"view", "schema"})
        self.assertEqual(
            combined, self._names({"view"}) | self._names({"schema"})
        )

    def test_unknown_tag_exposes_no_tools(self):
        self.assertEqual(self._names({"no_such_tag"}), set())

    def test_supported_tags_constant_matches_registered_tags(self):
        # --help renders this constant, so this keeps the docs from drifting.
        self.assertEqual(SUPPORTED_TOOL_TAGS, self._registered_tags())
