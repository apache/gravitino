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
from gravitino.client.generic_model import GenericModel
from gravitino.name_identifier import NameIdentifier
from gravitino.namespace import Namespace
from gravitino.utils.http_client import HTTPClient
from tests.unittests.mock_base import build_model_dto


class TestGenericModel(unittest.TestCase):
    _rest_client = HTTPClient("http://localhost:8080")
    _model_ident: NameIdentifier = NameIdentifier.of(
        "demo_metalake", "demo_catalog", "demo_schema", "demo_model"
    )

    def test_equal_and_hash(self) -> None:
        generic_model = GenericModel(
            build_model_dto(),
            TestGenericModel._rest_client,
            TestGenericModel._model_ident.namespace(),
        )

        generic_model2 = GenericModel(
            build_model_dto(),
            TestGenericModel._rest_client,
            Namespace.of("demo_metalake", "demo_catalog", "demo_schema"),
        )

        self.assertEqual(generic_model, generic_model2)
        self.assertEqual(hash(generic_model), hash(generic_model2))

    def test_extends_supports_tags_class(self) -> None:
        genenric_model = GenericModel(
            build_model_dto(),
            TestGenericModel._rest_client,
            TestGenericModel._model_ident.namespace(),
        )

        self.assertTrue(
            issubclass(
                GenericModel,
                SupportsTags,
            )
        )
        expected_methods = ["list_tags", "list_tags_info", "get_tag", "associate_tags"]

        self.assertTrue(
            all(
                callable(getattr(genenric_model, method, None))
                for method in expected_methods
            )
        )
