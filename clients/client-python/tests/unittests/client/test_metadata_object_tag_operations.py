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

from gravitino.api.metadata_objects import MetadataObject, MetadataObjects
from gravitino.api.tag import Tag
from gravitino.client.generic_tag import GenericTag
from gravitino.client.metadata_object_tag_operations import MetadataObjectTagOperations
from gravitino.dto.requests.tag_associate_request import TagsAssociateRequest
from gravitino.dto.responses.tag_response import (
    TagListResponse,
    TagNamesListResponse,
    TagResponse,
)
from gravitino.exceptions.handlers.tag_error_handler import (
    TAG_ERROR_HANDLER,
)
from gravitino.rest.rest_utils import encode_string
from gravitino.utils import HTTPClient
from tests.unittests import mock_base


class TestMetadataObjectTagOperations(unittest.TestCase):
    REST_CLIENT = HTTPClient("http://localhost:8090")
    METALAKE_NAME = "demo_metalake"

    def test_list_tag_request_path(self) -> None:
        metadata_object = MetadataObjects.of(
            ["catalog", "schema", "table"], MetadataObject.Type.TABLE
        )
        table_tag_request_path = MetadataObjectTagOperations.TAG_REQUEST_PATH.format(
            encode_string(TestMetadataObjectTagOperations.METALAKE_NAME),
            metadata_object.type().name.lower(),
            encode_string(metadata_object.full_name()),
        )
        self.assertEqual(
            "api/metalakes/demo_metalake/objects/table/catalog.schema.table/tags",
            table_tag_request_path,
        )

    def test_get_tag_request_path(self) -> None:
        metadata_object = MetadataObjects.of(
            ["catalog", "schema", "table"], MetadataObject.Type.TABLE
        )
        table_tag_request_path = MetadataObjectTagOperations.TAG_REQUEST_PATH.format(
            encode_string(TestMetadataObjectTagOperations.METALAKE_NAME),
            metadata_object.type().name.lower(),
            encode_string(metadata_object.full_name()),
        )
        self.assertEqual(
            "api/metalakes/demo_metalake/objects/table/catalog.schema.table/tags/tag_name",
            f"{table_tag_request_path}/tag_name",
        )

    def test_get_tag_from_metadata_object(self) -> None:
        tag_operations = MetadataObjectTagOperations(
            TestMetadataObjectTagOperations.METALAKE_NAME,
            MetadataObjects.of(
                ["catalog", "schema"],
                MetadataObject.Type.SCHEMA,
            ),
            TestMetadataObjectTagOperations.REST_CLIENT,
        )

        tag = mock_base.build_tag_dto()
        resp = TagResponse(0, tag)
        json_str = resp.to_json()
        mock_resp = mock_base.mock_http_response(json_str)

        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            return_value=mock_resp,
        ) as mock_get:
            retrieved_tag = tag_operations.get_tag(tag.name())
            self.check_tag_equal(tag, retrieved_tag)
            mock_get.assert_called_once_with(
                "api/metalakes/demo_metalake/objects/schema/catalog.schema/tags/tagA",
                params={},
                error_handler=TAG_ERROR_HANDLER,
            )

    def test_list_tags(self) -> None:
        tag_operations = MetadataObjectTagOperations(
            TestMetadataObjectTagOperations.METALAKE_NAME,
            MetadataObjects.of(
                ["catalog", "schema", "table"],
                MetadataObject.Type.TABLE,
            ),
            TestMetadataObjectTagOperations.REST_CLIENT,
        )
        json_str = TagNamesListResponse(0, ["tagA", "tagB"]).to_json()
        mock_resp = mock_base.mock_http_response(json_str)

        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            return_value=mock_resp,
        ) as mock_get:
            tags = tag_operations.list_tags()

            self.assertEqual(["tagA", "tagB"], tags)
            mock_get.assert_called_once_with(
                "api/metalakes/demo_metalake/objects/table/catalog.schema.table/tags",
                params={},
                error_handler=TAG_ERROR_HANDLER,
            )

    def test_list_tags_info(self) -> None:
        tag_operations = MetadataObjectTagOperations(
            TestMetadataObjectTagOperations.METALAKE_NAME,
            MetadataObjects.of(
                ["catalog", "schema", "table"],
                MetadataObject.Type.TABLE,
            ),
            TestMetadataObjectTagOperations.REST_CLIENT,
        )
        tag1 = mock_base.build_tag_dto()
        tag2 = mock_base.build_tag_dto(name="tagB", comment="commentB")
        json_str = TagListResponse(0, [tag1, tag2]).to_json()
        mock_resp = mock_base.mock_http_response(json_str)

        with patch(
            "gravitino.utils.http_client.HTTPClient.get",
            return_value=mock_resp,
        ) as mock_get:
            tags = tag_operations.list_tags_info()

            self.assertEqual(2, len(tags))
            self.assertTrue(all(isinstance(tag, GenericTag) for tag in tags))
            mock_get.assert_called_once_with(
                "api/metalakes/demo_metalake/objects/table/catalog.schema.table/tags",
                params={"details": "true"},
                error_handler=TAG_ERROR_HANDLER,
            )

    def test_associate_tags(self) -> None:
        tag_operations = MetadataObjectTagOperations(
            TestMetadataObjectTagOperations.METALAKE_NAME,
            MetadataObjects.of(
                ["catalog", "schema", "table"],
                MetadataObject.Type.TABLE,
            ),
            TestMetadataObjectTagOperations.REST_CLIENT,
        )
        json_str = TagNamesListResponse(0, ["tagA", "tagC"]).to_json()
        mock_resp = mock_base.mock_http_response(json_str)

        with patch(
            "gravitino.utils.http_client.HTTPClient.post",
            return_value=mock_resp,
        ) as mock_post:
            tags = tag_operations.associate_tags(["tagA"], ["tagB"])

            self.assertEqual(["tagA", "tagC"], tags)
            mock_post.assert_called_once()
            call_args = mock_post.call_args
            self.assertEqual(
                "api/metalakes/demo_metalake/objects/table/catalog.schema.table/tags",
                call_args.args[0],
            )

            param = TagsAssociateRequest(["tagA"], ["tagB"])

            mock_post.assert_called_once_with(
                "api/metalakes/demo_metalake/objects/table/catalog.schema.table/tags",
                json=param,
                error_handler=TAG_ERROR_HANDLER,
            )

    def check_tag_equal(self, left: Tag, right: Tag) -> None:
        self.assertEqual(left.name(), right.name())
        self.assertEqual(left.comment(), right.comment())
        self.assertEqual(left.properties(), right.properties())
