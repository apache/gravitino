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

import json
import unittest

from gravitino.api.authorization.privileges import Privilege
from gravitino.api.metadata_object import MetadataObject
from gravitino.api.policy.policy_association_selector import (
    AllValuesSelector,
    TagValueSelector,
)
from gravitino.api.policy.policy_content import PolicyExecutionSpec
from gravitino.api.policy.policy_contents import PolicyContents
from gravitino.dto.policy_dto import (
    AccessControlContentDTO,
    ColumnMaskContentDTO,
    CustomContentDTO,
    IcebergCompactionContentDTO,
    IcebergOrphanFileRemovalContentDTO,
    IcebergRewriteManifestsContentDTO,
    IcebergSnapshotExpirationContentDTO,
    PolicyAssociationSelectorDTO,
    PolicyContentDTO,
    PolicyDTO,
    PolicyForTagAssociationDTO,
    RowFilterContentDTO,
    TagForPolicyAssociationDTO,
)
from gravitino.dto.tag_dto import TagDTO
from gravitino.exceptions.base import IllegalArgumentException


def _policy_json(policy_type: str, content: dict) -> str:
    return json.dumps(
        {
            "name": "policy1",
            "comment": "a comment",
            "policyType": policy_type,
            "enabled": True,
            "content": content,
        }
    )


class TestPolicyDTO(unittest.TestCase):
    def test_access_control_round_trip(self) -> None:
        policy = PolicyDTO.from_json(
            _policy_json(
                "system_access_control",
                {
                    "privileges": ["select_table", "MODIFY_TABLE"],
                    "applicableRoles": ["analyst"],
                },
            )
        )
        self.assertEqual("policy1", policy.name())
        self.assertEqual("system_access_control", policy.policy_type())
        self.assertEqual("a comment", policy.comment())
        self.assertTrue(policy.enabled())
        content = policy.content()
        self.assertIsInstance(content, AccessControlContentDTO)
        self.assertEqual(
            [Privilege.Name.SELECT_TABLE, Privilege.Name.MODIFY_TABLE],
            content.privileges(),
        )
        self.assertEqual(["analyst"], content.applicable_roles())
        # Privilege names are written in lower case, mirroring PrivilegeDTO.
        self.assertEqual(
            ["modify_table", "select_table"],
            sorted(policy.to_dict()["content"]["privileges"]),
        )
        self.assertEqual(policy, PolicyDTO.from_json(policy.to_json()))

    def test_access_control_probe_table_like_round_trip(self) -> None:
        # The Java server permits PROBE_TABLE_LIKE in access-control
        # policies (see TestPolicyDTO.testAccessControlPolicySerDe), so
        # decoding such a policy must not raise KeyError.
        policy = PolicyDTO.from_json(
            _policy_json(
                "system_access_control",
                {
                    "privileges": ["SELECT_TABLE", "PROBE_TABLE_LIKE"],
                    "applicableRoles": ["analyst", "auditor"],
                },
            )
        )
        content = policy.content()
        self.assertIsInstance(content, AccessControlContentDTO)
        self.assertEqual(
            [Privilege.Name.SELECT_TABLE, Privilege.Name.PROBE_TABLE_LIKE],
            content.privileges(),
        )
        self.assertEqual(["analyst", "auditor"], content.applicable_roles())
        self.assertEqual(policy, PolicyDTO.from_json(policy.to_json()))

    def test_access_control_domain_conversion(self) -> None:
        dto = AccessControlContentDTO.from_dict(
            {"privileges": ["select_table"], "applicableRoles": ["r1"]},
            infer_missing=True,
        )
        domain = dto.to_domain_content()
        self.assertEqual(
            PolicyContents.access_control([Privilege.Name.SELECT_TABLE], ["r1"]),
            domain,
        )
        dto.validate()
        with self.assertRaises(IllegalArgumentException):
            AccessControlContentDTO.from_dict({}, infer_missing=True).validate()

    def test_compaction_round_trip_and_defaults(self) -> None:
        policy = PolicyDTO.from_json(
            _policy_json("system_iceberg_compaction", {"minDataFileMse": 7})
        )
        content = policy.content()
        self.assertIsInstance(content, IcebergCompactionContentDTO)
        self.assertEqual(7, content.min_data_file_mse())
        self.assertEqual(1, content.min_delete_file_number())
        self.assertEqual(100, content.delete_file_number_weight())
        self.assertEqual("binpack", content.rewrite_strategy())
        self.assertEqual(PolicyExecutionSpec.empty(), content.execution_spec())
        # Unset fields are omitted from the wire form.
        self.assertEqual({"minDataFileMse": 7}, policy.to_dict()["content"])
        self.assertEqual(policy, PolicyDTO.from_json(policy.to_json()))

    def test_compaction_execution_peers_round_trip(self) -> None:
        policy = PolicyDTO.from_json(
            _policy_json(
                "system_iceberg_snapshot_expiration",
                {
                    "olderThanDays": 9,
                    "timeTrigger": "0 0 * * *",
                    "commitTrigger": False,
                    "minIntervalMs": 60000,
                    "jobOptions": {"k": "v"},
                },
            )
        )
        content = policy.content()
        self.assertIsInstance(content, IcebergSnapshotExpirationContentDTO)
        self.assertEqual(9, content.older_than_days())
        self.assertEqual(1, content.retain_last())
        spec = content.execution_spec()
        self.assertEqual("0 0 * * *", spec.time_trigger())
        self.assertEqual(False, spec.commit_trigger())
        self.assertEqual(60000, spec.min_interval_ms())
        self.assertEqual({"k": "v"}, spec.job_options())
        domain = content.to_domain_content()
        self.assertEqual(
            PolicyContents.iceberg_snapshot_expiration(
                older_than_days=9,
                time_trigger="0 0 * * *",
                commit_trigger=False,
                min_interval_ms=60000,
                job_options={"k": "v"},
            ),
            domain,
        )
        self.assertEqual(policy, PolicyDTO.from_json(policy.to_json()))

    def test_orphan_file_removal_round_trip(self) -> None:
        policy = PolicyDTO.from_json(
            _policy_json(
                "system_iceberg_orphan_file_removal",
                {"olderThanDays": 10, "location": "s3://b/t", "dryRun": True},
            )
        )
        content = policy.content()
        self.assertIsInstance(content, IcebergOrphanFileRemovalContentDTO)
        self.assertEqual(10, content.older_than_days())
        self.assertEqual("s3://b/t", content.location())
        self.assertTrue(content.dry_run())
        self.assertEqual(policy, PolicyDTO.from_json(policy.to_json()))

    def test_rewrite_manifests_snake_case_wire_names(self) -> None:
        policy = PolicyDTO.from_json(
            _policy_json(
                "system_iceberg_rewrite_manifests",
                {
                    "manifest_count_critical": 900,
                    "manifest_count_warning": 200,
                    "avg_manifest_size_threshold_bytes": 1024,
                    "spec_id": 3,
                    "use_caching": True,
                },
            )
        )
        content = policy.content()
        self.assertIsInstance(content, IcebergRewriteManifestsContentDTO)
        self.assertEqual(900, content.manifest_count_critical())
        self.assertEqual(200, content.manifest_count_warning())
        self.assertEqual(1024, content.avg_manifest_size_threshold_bytes())
        self.assertEqual(3, content.spec_id())
        self.assertTrue(content.use_caching())
        wire = policy.to_dict()["content"]
        self.assertEqual(900, wire["manifest_count_critical"])
        self.assertEqual(3, wire["spec_id"])
        self.assertEqual(policy, PolicyDTO.from_json(policy.to_json()))

    def test_rewrite_manifests_defaults(self) -> None:
        content = IcebergRewriteManifestsContentDTO.from_dict({}, infer_missing=True)
        self.assertEqual(500, content.manifest_count_critical())
        self.assertEqual(100, content.manifest_count_warning())
        self.assertEqual(8388608, content.avg_manifest_size_threshold_bytes())
        self.assertEqual({}, content.to_dict())

    def test_row_filter_and_column_mask_round_trip(self) -> None:
        policy = PolicyDTO.from_json(
            _policy_json("system_row_filter", {"expression": "age > 18"})
        )
        self.assertIsInstance(policy.content(), RowFilterContentDTO)
        self.assertEqual("age > 18", policy.content().expression())
        self.assertEqual(
            PolicyContents.row_filter("age > 18"),
            policy.content().to_domain_content(),
        )
        self.assertEqual(policy, PolicyDTO.from_json(policy.to_json()))

        policy = PolicyDTO.from_json(
            _policy_json("system_column_mask", {"expression": "mask(x)"})
        )
        self.assertIsInstance(policy.content(), ColumnMaskContentDTO)
        self.assertEqual(
            {MetadataObject.Type.TABLE, MetadataObject.Type.COLUMN},
            policy.content().supported_object_types(),
        )

    def test_custom_content_and_unknown_type_fallback(self) -> None:
        content_dict = {
            "customRules": {"rule1": 456},
            "supportedObjectTypes": ["CATALOG", "TABLE"],
            "properties": {"k": "v"},
        }
        policy = PolicyDTO.from_json(_policy_json("custom", content_dict))
        self.assertIsInstance(policy.content(), CustomContentDTO)
        self.assertEqual({"rule1": 456}, policy.content().rules())
        self.assertEqual(
            {MetadataObject.Type.CATALOG, MetadataObject.Type.TABLE},
            policy.content().supported_object_types(),
        )
        self.assertEqual(policy, PolicyDTO.from_json(policy.to_json()))

        # Any non-built-in policy type also uses the custom content shape.
        policy = PolicyDTO.from_json(_policy_json("my_custom_type", content_dict))
        self.assertIsInstance(policy.content(), CustomContentDTO)

    def test_unknown_system_type_rejected(self) -> None:
        with self.assertRaises(IllegalArgumentException):
            PolicyDTO.from_json(_policy_json("system_unknown", {}))

    def test_builder_validation(self) -> None:
        content = RowFilterContentDTO.from_dict(
            {"expression": "x > 1"}, infer_missing=True
        )
        policy = (
            PolicyDTO.builder()
            .name("p")
            .policy_type("system_row_filter")
            .content(content)
            .build()
        )
        self.assertEqual("p", policy.name())
        self.assertTrue(policy.enabled())
        with self.assertRaises(IllegalArgumentException):
            PolicyDTO.builder().policy_type("system_row_filter").content(
                content
            ).build()
        with self.assertRaises(IllegalArgumentException):
            PolicyDTO.builder().name("p").policy_type("system_row_filter").build()
        with self.assertRaises(IllegalArgumentException):
            PolicyDTO.builder().name("p").policy_type("system_row_filter").content(
                RowFilterContentDTO.from_dict({"expression": " "}, infer_missing=True)
            ).build()

    def test_equality_ignores_inherited(self) -> None:
        def build(inherited):
            return PolicyDTO(
                _name="p",
                _policy_type="system_row_filter",
                _content=RowFilterContentDTO.from_dict(
                    {"expression": "x > 1"}, infer_missing=True
                ),
                _inherited=inherited,
            )

        self.assertEqual(build(True), build(None))
        self.assertEqual(hash(build(True)), hash(build(None)))
        self.assertNotEqual(
            build(True),
            PolicyDTO.from_json(
                _policy_json("system_row_filter", {"expression": "y > 2"})
            ),
        )


class TestPolicyAssociationSelectorDTO(unittest.TestCase):
    def test_all_values(self) -> None:
        dto = PolicyAssociationSelectorDTO.from_selector(AllValuesSelector.get())
        self.assertEqual("ALL_VALUES", dto.type())
        self.assertIsNone(dto.value())
        self.assertEqual({"type": "ALL_VALUES"}, dto.to_dict())
        self.assertEqual(AllValuesSelector.get(), dto.to_selector())
        dto.validate()

    def test_tag_value(self) -> None:
        dto = PolicyAssociationSelectorDTO.from_selector(TagValueSelector.of("gold"))
        self.assertEqual("TAG_VALUE", dto.type())
        self.assertEqual("gold", dto.value())
        self.assertEqual(TagValueSelector.of("gold"), dto.to_selector())
        round_tripped = PolicyAssociationSelectorDTO.from_json(
            dto.to_json(), infer_missing=True
        )
        self.assertEqual(dto, round_tripped)

    def test_from_selector_none(self) -> None:
        self.assertIsNone(PolicyAssociationSelectorDTO.from_selector(None))

    def test_validation_failures(self) -> None:
        with self.assertRaises(IllegalArgumentException):
            PolicyAssociationSelectorDTO("ALL_VALUES", "gold").validate()
        with self.assertRaises(IllegalArgumentException):
            PolicyAssociationSelectorDTO("TAG_VALUE", " ").validate()
        with self.assertRaises(IllegalArgumentException):
            PolicyAssociationSelectorDTO("TAG_VALUE", "x" * 257).validate()
        PolicyAssociationSelectorDTO("TAG_VALUE", "x" * 256).validate()
        with self.assertRaises(IllegalArgumentException):
            PolicyAssociationSelectorDTO("OTHER", None).validate()


class TestAssociationDTOs(unittest.TestCase):
    def test_policy_for_tag_association(self) -> None:
        association = PolicyForTagAssociationDTO.from_dict(
            {
                "policy": json.loads(
                    _policy_json("system_row_filter", {"expression": "x > 1"})
                ),
                "selector": {"type": "TAG_VALUE", "value": "gold"},
            },
            infer_missing=True,
        )
        self.assertEqual("policy1", association.policy().name())
        self.assertEqual("gold", association.selector().value())
        round_tripped = PolicyForTagAssociationDTO.from_json(
            association.to_json(), infer_missing=True
        )
        self.assertEqual(association.policy(), round_tripped.policy())
        self.assertEqual(association.selector(), round_tripped.selector())

    def test_tag_for_policy_association(self) -> None:
        tag = TagDTO.builder().name("tag1").build()
        association = TagForPolicyAssociationDTO(
            _tag=tag,
            _selector=PolicyAssociationSelectorDTO("ALL_VALUES", None),
        )
        round_tripped = TagForPolicyAssociationDTO.from_json(
            association.to_json(), infer_missing=True
        )
        self.assertEqual("tag1", round_tripped.tag().name())
        self.assertEqual("ALL_VALUES", round_tripped.selector().type())


class TestPolicyContentDTODispatch(unittest.TestCase):
    def test_dispatch_by_type(self) -> None:
        self.assertIsInstance(
            PolicyContentDTO.from_dict_by_type({}, "system_iceberg_compaction"),
            IcebergCompactionContentDTO,
        )
        self.assertIsInstance(
            PolicyContentDTO.from_dict_by_type({}, "custom"), CustomContentDTO
        )
        with self.assertRaises(IllegalArgumentException):
            PolicyContentDTO.from_dict_by_type({}, "system_nope")


if __name__ == "__main__":
    unittest.main()
