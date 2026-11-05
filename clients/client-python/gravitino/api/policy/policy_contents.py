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

from __future__ import annotations

from gravitino.api.authorization.privileges import Privilege
from gravitino.api.metadata_object import MetadataObject
from gravitino.api.policy.policy_content import (
    AccessControlContent,
    ColumnMaskContent,
    IcebergDataCompactionContent,
    IcebergOrphanFileRemovalContent,
    IcebergRewriteManifestsContent,
    IcebergSnapshotExpirationContent,
    PolicyContent,
    PolicyExecutionSpec,
    RowFilterContent,
)


def _freeze_value(value: object) -> object:
    """Recursively converts nested collections to hashable equivalents."""
    if isinstance(value, dict):
        return frozenset((key, _freeze_value(item)) for key, item in value.items())
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_value(item) for item in value)
    if isinstance(value, (set, frozenset)):
        return frozenset(_freeze_value(item) for item in value)
    return value


class PolicyContents:
    """Convenience factories for creating policy contents."""

    class CustomContent(PolicyContent):
        """The content of a custom policy: user-defined rules plus the object
        types and properties that describe them."""

        def __init__(
            self,
            custom_rules: dict[str, object],
            supported_object_types: set[MetadataObject.Type] | None = None,
            properties: dict[str, str] | None = None,
        ):
            self._custom_rules = custom_rules
            self._supported_object_types = (
                set(supported_object_types)
                if supported_object_types is not None
                else set()
            )
            self._properties = dict(properties) if properties is not None else {}

        def rules(self) -> dict[str, object]:
            return self._custom_rules

        def supported_object_types(self) -> set[MetadataObject.Type]:
            return set(self._supported_object_types)

        def properties(self) -> dict[str, str]:
            return dict(self._properties)

        def __eq__(self, other: object) -> bool:
            if not isinstance(other, PolicyContents.CustomContent):
                return False
            return (
                self._custom_rules == other._custom_rules
                and self._supported_object_types == other._supported_object_types
                and self._properties == other._properties
            )

        def __hash__(self) -> int:
            return hash(
                (
                    frozenset(
                        (key, _freeze_value(value))
                        for key, value in self._custom_rules.items()
                    ),
                    frozenset(self._supported_object_types),
                    frozenset(self._properties.items()),
                )
            )

    @staticmethod
    def custom(
        rules: dict[str, object],
        supported_object_types: set[MetadataObject.Type] | None = None,
        properties: dict[str, str] | None = None,
    ) -> PolicyContents.CustomContent:
        """Creates the content of a custom policy.

        Args:
            rules: The user-defined rules of the policy.
            supported_object_types: The metadata object types the policy can
                be applied to.
            properties: The additional properties of the policy.

        Returns:
            PolicyContents.CustomContent: The custom policy content.
        """
        return PolicyContents.CustomContent(rules, supported_object_types, properties)

    @staticmethod
    def iceberg_data_compaction(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        min_data_file_mse: int | None = None,
        min_delete_file_number: int | None = None,
        data_file_mse_weight: int | None = None,
        delete_file_number_weight: int | None = None,
        max_partition_num: int | None = None,
        rewrite_strategy: str | None = None,
        sort_order: str | None = None,
        rewrite_options: dict[str, str] | None = None,
        time_trigger: str | None = None,
        commit_trigger: bool | None = None,
        min_interval_ms: int | None = None,
        job_options: dict[str, str] | None = None,
        updater_options: dict[str, str] | None = None,
    ) -> IcebergDataCompactionContent:
        """Creates the content of an Iceberg data-compaction policy; unset
        arguments take the content class's documented defaults."""
        return IcebergDataCompactionContent(
            min_data_file_mse,
            min_delete_file_number,
            data_file_mse_weight,
            delete_file_number_weight,
            max_partition_num,
            rewrite_strategy,
            sort_order,
            rewrite_options,
            PolicyExecutionSpec.of(
                time_trigger,
                commit_trigger,
                min_interval_ms,
                job_options,
                updater_options,
            ),
        )

    @staticmethod
    def iceberg_orphan_file_removal(
        older_than_days: int | None = None,
        location: str | None = None,
        dry_run: bool | None = None,
        time_trigger: str | None = None,
        commit_trigger: bool | None = None,
        min_interval_ms: int | None = None,
        job_options: dict[str, str] | None = None,
        updater_options: dict[str, str] | None = None,
    ) -> IcebergOrphanFileRemovalContent:
        """Creates the content of an Iceberg orphan-file-removal policy;
        unset arguments take the content class's documented defaults."""
        return IcebergOrphanFileRemovalContent(
            older_than_days,
            location,
            dry_run,
            PolicyExecutionSpec.of(
                time_trigger,
                commit_trigger,
                min_interval_ms,
                job_options,
                updater_options,
            ),
        )

    @staticmethod
    def iceberg_rewrite_manifests(
        manifest_count_critical: int | None = None,
        manifest_count_warning: int | None = None,
        avg_manifest_size_threshold_bytes: int | None = None,
        spec_id: int | None = None,
        use_caching: bool | None = None,
        time_trigger: str | None = None,
        commit_trigger: bool | None = None,
        min_interval_ms: int | None = None,
        job_options: dict[str, str] | None = None,
        updater_options: dict[str, str] | None = None,
    ) -> IcebergRewriteManifestsContent:
        """Creates the content of an Iceberg rewrite-manifests policy; unset
        arguments take the content class's documented defaults."""
        return IcebergRewriteManifestsContent(
            manifest_count_critical,
            manifest_count_warning,
            avg_manifest_size_threshold_bytes,
            spec_id,
            use_caching,
            PolicyExecutionSpec.of(
                time_trigger,
                commit_trigger,
                min_interval_ms,
                job_options,
                updater_options,
            ),
        )

    @staticmethod
    def iceberg_snapshot_expiration(
        older_than_days: int | None = None,
        retain_last: int | None = None,
        stream_results: bool | None = None,
        time_trigger: str | None = None,
        commit_trigger: bool | None = None,
        min_interval_ms: int | None = None,
        job_options: dict[str, str] | None = None,
        updater_options: dict[str, str] | None = None,
    ) -> IcebergSnapshotExpirationContent:
        """Creates the content of an Iceberg snapshot-expiration policy;
        unset arguments take the content class's documented defaults."""
        return IcebergSnapshotExpirationContent(
            older_than_days,
            retain_last,
            stream_results,
            PolicyExecutionSpec.of(
                time_trigger,
                commit_trigger,
                min_interval_ms,
                job_options,
                updater_options,
            ),
        )

    @staticmethod
    def row_filter(expression: str) -> RowFilterContent:
        """Creates the content of a row-filter policy.

        Args:
            expression: The row-filter expression.

        Returns:
            RowFilterContent: The row-filter policy content.
        """
        return RowFilterContent(expression)

    @staticmethod
    def column_mask(expression: str) -> ColumnMaskContent:
        """Creates the content of a column-mask policy.

        Args:
            expression: The column-mask expression.

        Returns:
            ColumnMaskContent: The column-mask policy content.
        """
        return ColumnMaskContent(expression)

    @staticmethod
    def access_control(
        privileges: list[Privilege.Name],
        applicable_roles: list[str],
    ) -> AccessControlContent:
        """Creates the content of an access-control policy.

        Args:
            privileges: The privileges the policy confers.
            applicable_roles: The roles the policy applies to.

        Returns:
            AccessControlContent: The access-control policy content.
        """
        return AccessControlContent(privileges, applicable_roles)
