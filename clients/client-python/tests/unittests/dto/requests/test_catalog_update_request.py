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

import json as _json
import unittest

from gravitino.api.catalog_change import CatalogChange
from gravitino.dto.requests.catalog_update_request import CatalogUpdateRequest


class TestCatalogUpdateRequest(unittest.TestCase):
    def test_rename_catalog_request_validate(self) -> None:
        invalid_request = CatalogUpdateRequest.RenameCatalogRequest("")

        with self.assertRaises(ValueError):
            invalid_request.validate()

    def test_rename_catalog_request_serialize(self) -> None:
        request = CatalogUpdateRequest.RenameCatalogRequest("newCatalog")
        json_str = _json.dumps(
            {
                "@type": "rename",
                "newName": "newCatalog",
            },
            ensure_ascii=False,
        )

        self.assertEqual(json_str, request.to_json())

    def test_update_catalog_comment_request_serialize(self) -> None:
        request = CatalogUpdateRequest.UpdateCatalogCommentRequest("new comment")
        json_str = _json.dumps(
            {
                "@type": "updateComment",
                "newComment": "new comment",
            },
            ensure_ascii=False,
        )

        self.assertEqual(json_str, request.to_json())

    def test_set_catalog_property_request_validate(self) -> None:
        invalid_request1 = CatalogUpdateRequest.SetCatalogPropertyRequest("", "value")
        invalid_request2 = CatalogUpdateRequest.SetCatalogPropertyRequest("key", "")

        with self.assertRaises(ValueError):
            invalid_request1.validate()

        with self.assertRaises(ValueError):
            invalid_request2.validate()

    def test_set_catalog_property_request_serialize(self) -> None:
        request = CatalogUpdateRequest.SetCatalogPropertyRequest("key", "value1")
        json_str = _json.dumps(
            {
                "@type": "setProperty",
                "property": "key",
                "value": "value1",
            },
            ensure_ascii=False,
        )

        self.assertEqual(json_str, request.to_json())

    def test_remove_catalog_property_request_validate(self) -> None:
        invalid_request = CatalogUpdateRequest.RemoveCatalogPropertyRequest("")

        with self.assertRaises(ValueError):
            invalid_request.validate()

        # A non-empty property must pass validation.
        valid_request = CatalogUpdateRequest.RemoveCatalogPropertyRequest("key")
        valid_request.validate()

    def test_remove_catalog_property_request_serialize(self) -> None:
        request = CatalogUpdateRequest.RemoveCatalogPropertyRequest("prop1")
        json_str = _json.dumps(
            {
                "@type": "removeProperty",
                "property": "prop1",
            },
            ensure_ascii=False,
        )

        self.assertEqual(json_str, request.to_json())

    def test_remove_catalog_property_request_catalog_change(self) -> None:
        request = CatalogUpdateRequest.RemoveCatalogPropertyRequest("prop1")

        self.assertIsInstance(request.catalog_change(), CatalogChange.RemoveProperty)


if __name__ == "__main__":
    unittest.main()
