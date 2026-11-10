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
from gravitino.api.policy.policy import Policy
from gravitino.api.policy.policy_association_selector import (
    AllValuesSelector,
    TagValueSelector,
)
from gravitino.api.policy.policy_content import (
    AccessControlContent,
    IcebergDataCompactionContent,
    PolicyExecutionSpec,
)
from gravitino.api.policy.policy_contents import PolicyContents
from gravitino.api.policy.policy_list import PolicyList
from gravitino.api.policy.supports_policies import SupportsPolicies
from gravitino.exceptions.base import IllegalArgumentException


class TestPolicyExecutionSpec(unittest.TestCase):
    def test_empty(self) -> None:
        spec = PolicyExecutionSpec.of()
        self.assertFalse(spec.has_any())
        self.assertEqual(PolicyExecutionSpec.empty(), spec)
        self.assertIsNone(spec.time_trigger())
        self.assertIsNone(spec.commit_trigger())
        self.assertIsNone(spec.min_interval_ms())
        self.assertEqual({}, spec.job_options())
        self.assertEqual({}, spec.updater_options())

    def test_blank_time_trigger_normalized_to_none(self) -> None:
        spec = PolicyExecutionSpec.of(time_trigger="  ")
        self.assertIsNone(spec.time_trigger())
        self.assertFalse(spec.has_any())

    def test_commit_trigger_false_is_retained(self) -> None:
        spec = PolicyExecutionSpec.of(commit_trigger=False)
        self.assertEqual(False, spec.commit_trigger())
        self.assertTrue(spec.has_any())

    def test_negative_min_interval_rejected(self) -> None:
        with self.assertRaises(IllegalArgumentException):
            PolicyExecutionSpec.of(min_interval_ms=-1)

    def test_blank_option_key_or_value_rejected(self) -> None:
        with self.assertRaises(IllegalArgumentException):
            PolicyExecutionSpec.of(job_options={" ": "v"})
        with self.assertRaises(IllegalArgumentException):
            PolicyExecutionSpec.of(updater_options={"k": ""})

    def test_equality_and_hash(self) -> None:
        left = PolicyExecutionSpec.of(time_trigger="0 0 * * *", job_options={"k": "v"})
        right = PolicyExecutionSpec.of(time_trigger="0 0 * * *", job_options={"k": "v"})
        self.assertEqual(left, right)
        self.assertEqual(hash(left), hash(right))
        self.assertNotEqual(left, PolicyExecutionSpec.empty())


class TestReadRestrictionContents(unittest.TestCase):
    def test_row_filter(self) -> None:
        content = PolicyContents.row_filter("age > 18")
        self.assertEqual("age > 18", content.expression())
        self.assertEqual({MetadataObject.Type.TABLE}, content.supported_object_types())
        self.assertEqual({}, content.properties())
        self.assertEqual({"expression": "age > 18"}, content.rules())
        content.validate()

    def test_row_filter_blank_expression_rejected(self) -> None:
        with self.assertRaises(IllegalArgumentException):
            PolicyContents.row_filter("  ")
        with self.assertRaises(IllegalArgumentException):
            PolicyContents.row_filter(None)

    def test_row_filter_oversized_expression_rejected(self) -> None:
        # 9000 two-byte characters exceed the 16 * 1024 UTF-8 byte limit.
        with self.assertRaises(IllegalArgumentException):
            PolicyContents.row_filter("é" * 9000)

    def test_column_mask(self) -> None:
        content = PolicyContents.column_mask("mask(ssn)")
        self.assertEqual(
            {MetadataObject.Type.TABLE, MetadataObject.Type.COLUMN},
            content.supported_object_types(),
        )
        self.assertEqual({"expression": "mask(ssn)"}, content.rules())
        with self.assertRaises(IllegalArgumentException):
            PolicyContents.column_mask("")


class TestAccessControlContent(unittest.TestCase):
    def test_rules_and_types(self) -> None:
        content = PolicyContents.access_control(
            [Privilege.Name.SELECT_TABLE, Privilege.Name.MODIFY_TABLE],
            ["analyst"],
        )
        self.assertEqual(
            {
                "privileges": ["SELECT_TABLE", "MODIFY_TABLE"],
                "applicableRoles": ["analyst"],
            },
            content.rules(),
        )
        self.assertIn(MetadataObject.Type.TABLE, content.supported_object_types())
        self.assertIn(MetadataObject.Type.VIEW, content.supported_object_types())
        self.assertEqual({}, content.properties())
        content.validate()

    def test_empty_privileges_rejected(self) -> None:
        content = AccessControlContent([], ["analyst"])
        with self.assertRaises(IllegalArgumentException):
            content.validate()

    def test_null_privilege_rejected(self) -> None:
        content = AccessControlContent([None], ["analyst"])
        with self.assertRaises(IllegalArgumentException):
            content.validate()

    def test_empty_or_blank_roles_rejected(self) -> None:
        with self.assertRaises(IllegalArgumentException):
            AccessControlContent([Privilege.Name.SELECT_TABLE], []).validate()
        with self.assertRaises(IllegalArgumentException):
            AccessControlContent([Privilege.Name.SELECT_TABLE], [" "]).validate()

    def test_null_lists_become_empty(self) -> None:
        content = AccessControlContent(None, None)
        self.assertEqual([], content.privileges())
        self.assertEqual([], content.applicable_roles())


class TestIcebergDataCompactionContent(unittest.TestCase):
    def test_defaults(self) -> None:
        content = PolicyContents.iceberg_data_compaction()
        self.assertEqual(405323966463344, content.min_data_file_mse())
        self.assertEqual(1, content.min_delete_file_number())
        self.assertEqual(1, content.data_file_mse_weight())
        self.assertEqual(100, content.delete_file_number_weight())
        self.assertEqual(50, content.max_partition_num())
        self.assertEqual("binpack", content.rewrite_strategy())
        self.assertEqual("", content.sort_order())
        self.assertEqual({}, content.rewrite_options())
        self.assertEqual(PolicyExecutionSpec.empty(), content.execution_spec())
        content.validate()

    def test_properties_and_rules(self) -> None:
        content = PolicyContents.iceberg_data_compaction(
            rewrite_options={"target-file-size-bytes": "123"}
        )
        self.assertEqual(
            {
                "strategy.type": "iceberg-data-compaction",
                "job.template-name": "builtin-iceberg-rewrite-data-files",
            },
            content.properties(),
        )
        rules = content.rules()
        self.assertEqual(
            IcebergDataCompactionContent.TRIGGER_EXPR, rules["trigger-expr"]
        )
        self.assertEqual(IcebergDataCompactionContent.SCORE_EXPR, rules["score-expr"])
        self.assertEqual("binpack", rules["rewriteStrategy"])
        self.assertEqual("123", rules["job.options.target-file-size-bytes"])
        self.assertEqual(
            {
                MetadataObject.Type.CATALOG,
                MetadataObject.Type.SCHEMA,
                MetadataObject.Type.TABLE,
            },
            content.supported_object_types(),
        )

    def test_execution_peers_passed_through(self) -> None:
        content = PolicyContents.iceberg_data_compaction(
            time_trigger="0 0 * * *", min_interval_ms=1000
        )
        self.assertEqual("0 0 * * *", content.execution_spec().time_trigger())
        self.assertEqual(1000, content.execution_spec().min_interval_ms())

    def test_validation_failures(self) -> None:
        with self.assertRaises(IllegalArgumentException):
            PolicyContents.iceberg_data_compaction(rewrite_strategy="sort").validate()
        with self.assertRaises(IllegalArgumentException):
            PolicyContents.iceberg_data_compaction(sort_order="x").validate()
        PolicyContents.iceberg_data_compaction(
            rewrite_strategy="sort", sort_order="id"
        ).validate()
        with self.assertRaises(IllegalArgumentException):
            PolicyContents.iceberg_data_compaction(rewrite_strategy="zipper").validate()
        with self.assertRaises(IllegalArgumentException):
            PolicyContents.iceberg_data_compaction(min_data_file_mse=-1).validate()
        with self.assertRaises(IllegalArgumentException):
            PolicyContents.iceberg_data_compaction(max_partition_num=0).validate()

    def test_rewrite_strategy_is_normalized(self) -> None:
        content = PolicyContents.iceberg_data_compaction(
            rewrite_strategy=" SORT ", sort_order="id"
        )
        self.assertEqual("sort", content.rewrite_strategy())
        content.validate()
        self.assertEqual(
            PolicyContents.iceberg_data_compaction(rewrite_strategy="binpack"),
            PolicyContents.iceberg_data_compaction(rewrite_strategy=" BinPack "),
        )

    def test_valid_rewrite_options(self) -> None:
        PolicyContents.iceberg_data_compaction(
            rewrite_options={
                "target-file-size-bytes": "1048576",
                "min-input-files": "5",
                "partial-progress.max-failed-commits": "0",
                "rewrite-all": "true",
                "partial-progress.enabled": "FALSE",
                "rewrite-job-order": "Bytes-Asc",
                "delete-ratio-threshold": "0.5",
            }
        ).validate()

    def test_invalid_rewrite_options(self) -> None:
        invalid_options = [
            {"target-file-size-bytes": "-1"},
            {"target-file-size-bytes": "0"},
            {"target-file-size-bytes": "1.5"},
            {"min-input-files": "0"},
            {"partial-progress.max-failed-commits": "-1"},
            {"rewrite-all": "yes"},
            {"rewrite-job-order": "random"},
            {"delete-ratio-threshold": "1.5"},
            {"delete-ratio-threshold": "abc"},
            {"unknown-option": "1"},
            {"job.options.target-file-size-bytes": "1"},
            {"bad key!": "1"},
            {"target-file-size-bytes": ""},
        ]
        for options in invalid_options:
            with self.subTest(options=options):
                with self.assertRaises(IllegalArgumentException):
                    PolicyContents.iceberg_data_compaction(
                        rewrite_options=options
                    ).validate()


class TestIcebergOrphanFileRemovalContent(unittest.TestCase):
    def test_defaults_and_rules(self) -> None:
        content = PolicyContents.iceberg_orphan_file_removal()
        self.assertEqual(3, content.older_than_days())
        self.assertIsNone(content.location())
        self.assertEqual(False, content.dry_run())
        rules = content.rules()
        self.assertEqual("true", rules["trigger-expr"])
        self.assertEqual("1", rules["score-expr"])
        self.assertEqual(3, rules["job.options.olderThanDays"])
        self.assertEqual(False, rules["job.options.dryRun"])
        self.assertNotIn("job.options.location", rules)
        content.validate()

    def test_location_in_rules_when_set(self) -> None:
        content = PolicyContents.iceberg_orphan_file_removal(
            location="s3://bucket/table"
        )
        self.assertEqual("s3://bucket/table", content.rules()["job.options.location"])

    def test_validation_failures(self) -> None:
        with self.assertRaises(IllegalArgumentException):
            PolicyContents.iceberg_orphan_file_removal(commit_trigger=True).validate()
        with self.assertRaises(IllegalArgumentException):
            PolicyContents.iceberg_orphan_file_removal(older_than_days=0).validate()
        with self.assertRaises(IllegalArgumentException):
            PolicyContents.iceberg_orphan_file_removal(older_than_days=36501).validate()
        with self.assertRaises(IllegalArgumentException):
            PolicyContents.iceberg_orphan_file_removal(
                location="s3://bucket/../table"
            ).validate()
        with self.assertRaises(IllegalArgumentException):
            PolicyContents.iceberg_orphan_file_removal(
                location=" s3://bucket/table"
            ).validate()


class TestIcebergRewriteManifestsContent(unittest.TestCase):
    def test_defaults_and_rules(self) -> None:
        content = PolicyContents.iceberg_rewrite_manifests()
        self.assertEqual(500, content.manifest_count_critical())
        self.assertEqual(100, content.manifest_count_warning())
        self.assertEqual(8388608, content.avg_manifest_size_threshold_bytes())
        self.assertIsNone(content.spec_id())
        self.assertIsNone(content.use_caching())
        rules = content.rules()
        self.assertEqual(500, rules["manifest_count_critical"])
        self.assertEqual(100, rules["manifest_count_warning"])
        self.assertEqual(8388608, rules["avg_manifest_size_threshold_bytes"])
        self.assertNotIn("spec_id", rules)
        self.assertNotIn("use_caching", rules)
        content.validate()

    def test_optional_rules_included_when_set(self) -> None:
        content = PolicyContents.iceberg_rewrite_manifests(spec_id=2, use_caching=True)
        self.assertEqual(2, content.rules()["spec_id"])
        self.assertEqual(True, content.rules()["use_caching"])

    def test_validation_failures(self) -> None:
        with self.assertRaises(IllegalArgumentException):
            PolicyContents.iceberg_rewrite_manifests(
                manifest_count_warning=0
            ).validate()
        with self.assertRaises(IllegalArgumentException):
            PolicyContents.iceberg_rewrite_manifests(
                manifest_count_critical=50, manifest_count_warning=100
            ).validate()
        with self.assertRaises(IllegalArgumentException):
            PolicyContents.iceberg_rewrite_manifests(
                avg_manifest_size_threshold_bytes=0
            ).validate()
        with self.assertRaises(IllegalArgumentException):
            PolicyContents.iceberg_rewrite_manifests(spec_id=-1).validate()


class TestIcebergSnapshotExpirationContent(unittest.TestCase):
    def test_defaults_and_rules(self) -> None:
        content = PolicyContents.iceberg_snapshot_expiration()
        self.assertEqual(5, content.older_than_days())
        self.assertEqual(1, content.retain_last())
        self.assertEqual(False, content.stream_results())
        rules = content.rules()
        self.assertEqual(5, rules["job.options.olderThanDays"])
        self.assertEqual(1, rules["job.options.retainLast"])
        self.assertEqual(False, rules["job.options.streamResults"])
        content.validate()

    def test_validation_failures(self) -> None:
        with self.assertRaises(IllegalArgumentException):
            PolicyContents.iceberg_snapshot_expiration(older_than_days=0).validate()
        with self.assertRaises(IllegalArgumentException):
            PolicyContents.iceberg_snapshot_expiration(retain_last=0).validate()


class TestCustomContent(unittest.TestCase):
    def test_custom_content(self) -> None:
        content = PolicyContents.custom(
            {"rule1": 123},
            {MetadataObject.Type.TABLE},
            {"key": "value"},
        )
        self.assertEqual({"rule1": 123}, content.rules())
        self.assertEqual({MetadataObject.Type.TABLE}, content.supported_object_types())
        self.assertEqual({"key": "value"}, content.properties())
        content.validate()
        self.assertEqual(
            content,
            PolicyContents.custom(
                {"rule1": 123}, {MetadataObject.Type.TABLE}, {"key": "value"}
            ),
        )

    def test_custom_content_hash_with_nested_rule_values(self) -> None:
        rules = {"rule": ["a", "b"], "nested": {"x": [1, 2], "y": {"z": 3}}}
        content = PolicyContents.custom(rules, {MetadataObject.Type.TABLE}, {})
        equal_content = PolicyContents.custom(
            {"nested": {"y": {"z": 3}, "x": [1, 2]}, "rule": ["a", "b"]},
            {MetadataObject.Type.TABLE},
            {},
        )
        self.assertEqual(content, equal_content)
        self.assertEqual(hash(content), hash(equal_content))
        self.assertIn(content, {equal_content})


class TestBuiltInType(unittest.TestCase):
    def test_from_policy_type(self) -> None:
        self.assertEqual(
            Policy.BuiltInType.ICEBERG_COMPACTION,
            Policy.BuiltInType.from_policy_type("system_iceberg_compaction"),
        )
        self.assertEqual(
            Policy.BuiltInType.ACCESS_CONTROL,
            Policy.BuiltInType.from_policy_type("SYSTEM_ACCESS_CONTROL"),
        )
        self.assertEqual(
            Policy.BuiltInType.CUSTOM, Policy.BuiltInType.from_policy_type("custom")
        )

    def test_from_policy_type_failures(self) -> None:
        with self.assertRaises(IllegalArgumentException):
            Policy.BuiltInType.from_policy_type(None)
        with self.assertRaises(IllegalArgumentException):
            Policy.BuiltInType.from_policy_type("system_unknown")
        with self.assertRaises(IllegalArgumentException):
            Policy.BuiltInType.from_policy_type("something_else")

    def test_content_classes(self) -> None:
        self.assertEqual(
            IcebergDataCompactionContent,
            Policy.BuiltInType.ICEBERG_COMPACTION.content_class(),
        )
        self.assertEqual(
            AccessControlContent,
            Policy.BuiltInType.ACCESS_CONTROL.content_class(),
        )
        self.assertEqual(
            PolicyContents.CustomContent, Policy.BuiltInType.CUSTOM.content_class()
        )


class TestSelectors(unittest.TestCase):
    def test_all_values_singleton(self) -> None:
        self.assertIs(AllValuesSelector.get(), AllValuesSelector.get())
        self.assertEqual("ALL_VALUES", AllValuesSelector.get().type())
        self.assertEqual(AllValuesSelector.get(), AllValuesSelector())

    def test_tag_value_selector(self) -> None:
        selector = TagValueSelector.of("gold")
        self.assertEqual("TAG_VALUE", selector.type())
        self.assertEqual("gold", selector.value())
        self.assertEqual(TagValueSelector.of("gold"), selector)
        with self.assertRaises(IllegalArgumentException):
            TagValueSelector.of(" ")


class TestPolicyListAndSupportsPolicies(unittest.TestCase):
    def test_policy_list_copy(self) -> None:
        policy_list = PolicyList(["a", "b"], True)
        policies = policy_list.policies()
        policies.append("c")
        self.assertEqual(["a", "b"], policy_list.policies())
        self.assertTrue(policy_list.has_inaccessible_policies())

    def test_visibility_defaults(self) -> None:
        class _Stub(SupportsPolicies):
            def list_policies(self):
                return ["p1"]

            def list_policy_infos(self):
                return []

        stub = _Stub()
        listed = stub.list_policies_with_visibility()
        self.assertEqual(["p1"], listed.policies())
        self.assertFalse(listed.has_inaccessible_policies())
        self.assertEqual([], stub.list_policy_infos_with_visibility().policies())


if __name__ == "__main__":
    unittest.main()
