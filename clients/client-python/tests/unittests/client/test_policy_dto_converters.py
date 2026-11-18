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

from gravitino.api.authorization.privileges import Privilege
from gravitino.api.metadata_object import MetadataObject
from gravitino.api.policy.policy_change import PolicyChange
from gravitino.api.policy.policy_content import PolicyContent
from gravitino.api.policy.policy_contents import PolicyContents
from gravitino.client.dto_converters import DTOConverters
from gravitino.dto.policy_dto import (
    AccessControlContentDTO,
    IcebergCompactionContentDTO,
    RowFilterContentDTO,
)
from gravitino.dto.requests.policy_update_request import PolicyUpdateRequest
from gravitino.exceptions.base import IllegalArgumentException


class TestPolicyDTOConverters(unittest.TestCase):
    def test_to_policy_update_request(self) -> None:
        rename = DTOConverters.to_policy_update_request(PolicyChange.rename("new_name"))
        self.assertIsInstance(rename, PolicyUpdateRequest.RenamePolicyRequest)
        self.assertEqual("new_name", rename.new_name)

        comment = DTOConverters.to_policy_update_request(
            PolicyChange.update_comment("new comment")
        )
        self.assertIsInstance(comment, PolicyUpdateRequest.UpdatePolicyCommentRequest)
        self.assertEqual("new comment", comment.new_comment)

        content = DTOConverters.to_policy_update_request(
            PolicyChange.update_content(
                "system_row_filter", PolicyContents.row_filter("x > 1")
            )
        )
        self.assertIsInstance(content, PolicyUpdateRequest.UpdatePolicyContentRequest)
        self.assertEqual("system_row_filter", content.policy_type)
        self.assertIsInstance(content.new_content, RowFilterContentDTO)
        self.assertEqual("x > 1", content.new_content.expression())

    def test_to_policy_update_request_unknown_change(self) -> None:
        class UnknownChange(PolicyChange):
            pass

        with self.assertRaises(IllegalArgumentException):
            DTOConverters.to_policy_update_request(UnknownChange())

    def test_content_round_trips(self) -> None:
        contents = [
            PolicyContents.custom({"rule": 1}, {MetadataObject.Type.TABLE}, {"k": "v"}),
            PolicyContents.iceberg_data_compaction(
                min_data_file_mse=10,
                rewrite_strategy="sort",
                sort_order="id",
                rewrite_options={"a": "b"},
                time_trigger="0 0 * * *",
                min_interval_ms=5,
            ),
            PolicyContents.iceberg_orphan_file_removal(
                older_than_days=7, location="s3://b/t", dry_run=True
            ),
            PolicyContents.iceberg_rewrite_manifests(
                manifest_count_critical=900, spec_id=1, use_caching=False
            ),
            PolicyContents.iceberg_snapshot_expiration(
                older_than_days=30, retain_last=3, stream_results=True
            ),
            PolicyContents.row_filter("age > 18"),
            PolicyContents.column_mask("mask(ssn)"),
            PolicyContents.access_control(
                [Privilege.Name.SELECT_TABLE, Privilege.Name.USE_MODEL],
                ["analyst", "admin"],
            ),
        ]
        for content in contents:
            dto = DTOConverters.to_policy_content_dto(content)
            self.assertEqual(
                content,
                DTOConverters.from_policy_content_dto(dto),
                f"round trip failed for {type(content).__name__}",
            )

    def test_to_policy_content_dto_types(self) -> None:
        dto = DTOConverters.to_policy_content_dto(
            PolicyContents.iceberg_data_compaction()
        )
        self.assertIsInstance(dto, IcebergCompactionContentDTO)
        self.assertEqual(405323966463344, dto.min_data_file_mse())

        dto = DTOConverters.to_policy_content_dto(
            PolicyContents.access_control([Privilege.Name.SELECT_TABLE], ["analyst"])
        )
        self.assertIsInstance(dto, AccessControlContentDTO)
        self.assertEqual([Privilege.Name.SELECT_TABLE], dto.privileges())

    def test_to_policy_content_dto_unknown(self) -> None:
        class UnknownContent(PolicyContent):
            def supported_object_types(self):
                return {MetadataObject.Type.TABLE}

            def properties(self):
                return {}

        with self.assertRaises(IllegalArgumentException):
            DTOConverters.to_policy_content_dto(UnknownContent())


if __name__ == "__main__":
    unittest.main()
