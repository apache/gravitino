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

from gravitino.api.tag.supports_tags import SupportsTags
from gravitino.client.generic_schema import GenericSchema
from gravitino.utils.http_client import HTTPClient
from tests.unittests.mock_base import build_schema_dto


class TestGenericSchema(unittest.TestCase):
    _rest_client = HTTPClient("http://localhost:8080")

    def test_equal_and_hash(self) -> None:
        schema_dto1 = build_schema_dto()
        schema_dto2 = build_schema_dto()

        generic_schema1 = GenericSchema(
            schema_dto1,
            TestGenericSchema._rest_client,
            "demo_metalake",
            "demo_catalog",
        )
        generic_schema2 = GenericSchema(
            schema_dto2,
            TestGenericSchema._rest_client,
            "demo_metalake",
            "demo_catalog",
        )

        self.assertEqual(generic_schema1, generic_schema2)
        self.assertEqual(hash(generic_schema1), hash(generic_schema2))

    def test_extends_supports_tags_class(self) -> None:
        generic_schema = GenericSchema(
            build_schema_dto(),
            TestGenericSchema._rest_client,
            "demo_metalake",
            "demo_catalog",
        )

        self.assertTrue(
            issubclass(
                GenericSchema,
                SupportsTags,
            )
        )
        expected_methods = ["list_tags", "list_tags_info", "get_tag", "associate_tags"]

        self.assertTrue(
            all(
                callable(getattr(generic_schema, method, None))
                for method in expected_methods
            )
        )
