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

import json
from abc import abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field

from dataclasses_json import DataClassJsonMixin, Undefined, config, dataclass_json

from gravitino.api.audit import Audit
from gravitino.api.authorization.privileges import Privilege
from gravitino.api.metadata_object import MetadataObject
from gravitino.api.policy.policy import Policy
from gravitino.api.policy.policy_association_selector import (
    AllValuesSelector,
    PolicyAssociationSelector,
    TagValueSelector,
)
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
from gravitino.api.policy.policy_contents import PolicyContents
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.tag_dto import TagDTO
from gravitino.exceptions.base import IllegalArgumentException
from gravitino.utils.precondition import Precondition


def _encode_object_types(types: set) -> list:
    # The Java server writes MetadataObject.Type enum names (for example
    # "TABLE"); it also reads them case-insensitively.
    return sorted(object_type.name for object_type in types)


def _decode_object_types(values: list) -> set:
    return {MetadataObject.Type[str(value).upper()] for value in values}


def _encode_privilege_names(names: list) -> list:
    # Mirrors PrivilegeDTO: privilege names are written in lower case.
    return [name.name.lower() for name in names]


def _decode_privilege_names(values: list) -> list:
    return [Privilege.Name[str(value).upper()] for value in values]


_POLICY_CONTENT_DTO_TYPES: dict = {}


def _exclude_none_fields(cls: type) -> type:
    """Wraps a DTO class's ``to_dict`` so fields with None values are
    omitted, mirroring the Java DTOs' NON_NULL serialization. (The
    dataclasses_json Undefined.EXCLUDE setting does not strip None values
    on the to_dict/to_json path.)"""
    original_to_dict = cls.to_dict

    def to_dict(self, encode_json: bool = False) -> dict:
        result = original_to_dict(self, encode_json=encode_json)
        return {key: value for key, value in result.items() if value is not None}

    cls.to_dict = to_dict
    return cls


def _register_policy_content_dto(
    policy_type: str,
) -> Callable[[type], type]:
    def decorator(cls: type) -> type:
        _exclude_none_fields(cls)
        _POLICY_CONTENT_DTO_TYPES[policy_type] = cls
        return cls

    return decorator


class PolicyContentDTO(PolicyContent):
    """Base class of the data transfer objects for policy contents.

    The concrete wire type of a policy's content is selected by the sibling
    ``policyType`` field of the enclosing policy object, mirroring the Java
    DTOs' external-property polymorphic mapping.
    """

    @classmethod
    def from_dict_by_type(cls, data: dict, policy_type: str) -> PolicyContentDTO:
        """Deserializes a policy content DTO, dispatching on the policy type.

        Args:
            data: The serialized content.
            policy_type: The policy type of the enclosing policy.

        Returns:
            PolicyContentDTO: The deserialized content DTO.

        Raises:
            IllegalArgumentException: If the policy type is an unknown
                ``system_``-prefixed built-in type.
        """
        content_type = _POLICY_CONTENT_DTO_TYPES.get(policy_type)
        if content_type is None:
            if policy_type is not None and policy_type.lower().startswith(
                Policy.BUILT_IN_TYPE_PREFIX
            ):
                raise IllegalArgumentException(
                    f"Unknown built-in policy type: {policy_type}"
                )
            # Any non-built-in policy type uses the custom content shape.
            content_type = CustomContentDTO
        return content_type.from_dict(data, infer_missing=True)

    @abstractmethod
    def to_domain_content(self) -> PolicyContent:
        """Converts this DTO to the domain policy content."""
        raise NotImplementedError()


class ExecutionPeersDTO:
    """Shared accessors for the flat maintenance execution peer fields
    carried by the executable policy content DTOs."""

    def execution_spec(self) -> PolicyExecutionSpec:
        """Assembles the execution peers into a PolicyExecutionSpec."""
        return PolicyExecutionSpec.of(
            self._time_trigger,
            self._commit_trigger,
            self._min_interval_ms,
            self._job_options,
            self._updater_options,
        )

    def time_trigger(self) -> str | None:
        return self._time_trigger

    def commit_trigger(self) -> bool | None:
        return self._commit_trigger

    def min_interval_ms(self) -> int | None:
        return self._min_interval_ms

    def job_options(self) -> dict:
        return dict(self._job_options) if self._job_options else {}

    def updater_options(self) -> dict:
        return dict(self._updater_options) if self._updater_options else {}


@_register_policy_content_dto("custom")
@dataclass_json(undefined=Undefined.EXCLUDE)
@dataclass
class CustomContentDTO(PolicyContentDTO):
    """Data transfer object for the content of a custom policy."""

    _custom_rules: dict | None = field(
        default=None, metadata=config(field_name="customRules")
    )
    _properties: dict | None = field(
        default=None, metadata=config(field_name="properties")
    )
    _supported_object_types: set | None = field(
        default=None,
        metadata=config(
            field_name="supportedObjectTypes",
            encoder=_encode_object_types,
            decoder=_decode_object_types,
        ),
    )

    def custom_rules(self) -> dict:
        return dict(self._custom_rules) if self._custom_rules else {}

    def properties(self) -> dict:
        return dict(self._properties) if self._properties else {}

    def supported_object_types(self) -> set:
        return (
            set(self._supported_object_types) if self._supported_object_types else set()
        )

    def rules(self) -> dict:
        return self.custom_rules()

    def to_domain_content(self) -> PolicyContents.CustomContent:
        return PolicyContents.CustomContent(
            self.custom_rules(),
            self.supported_object_types(),
            self.properties(),
        )


@_register_policy_content_dto("system_iceberg_compaction")
@dataclass_json(undefined=Undefined.EXCLUDE)
@dataclass
class IcebergCompactionContentDTO(
    PolicyContentDTO, ExecutionPeersDTO
):  # pylint: disable=too-many-instance-attributes
    """Data transfer object for the content of an Iceberg data-compaction
    policy."""

    _min_data_file_mse: int | None = field(
        default=None, metadata=config(field_name="minDataFileMse")
    )
    _min_delete_file_number: int | None = field(
        default=None, metadata=config(field_name="minDeleteFileNumber")
    )
    _data_file_mse_weight: int | None = field(
        default=None, metadata=config(field_name="dataFileMseWeight")
    )
    _delete_file_number_weight: int | None = field(
        default=None, metadata=config(field_name="deleteFileNumberWeight")
    )
    _max_partition_num: int | None = field(
        default=None, metadata=config(field_name="maxPartitionNum")
    )
    _rewrite_strategy: str | None = field(
        default=None, metadata=config(field_name="rewriteStrategy")
    )
    _sort_order: str | None = field(
        default=None, metadata=config(field_name="sortOrder")
    )
    _rewrite_options: dict | None = field(
        default=None, metadata=config(field_name="rewriteOptions")
    )
    _time_trigger: str | None = field(
        default=None, metadata=config(field_name="timeTrigger")
    )
    _commit_trigger: bool | None = field(
        default=None, metadata=config(field_name="commitTrigger")
    )
    _min_interval_ms: int | None = field(
        default=None, metadata=config(field_name="minIntervalMs")
    )
    _job_options: dict | None = field(
        default=None, metadata=config(field_name="jobOptions")
    )
    _updater_options: dict | None = field(
        default=None, metadata=config(field_name="updaterOptions")
    )

    def min_data_file_mse(self) -> int:
        return (
            self._min_data_file_mse
            if self._min_data_file_mse is not None
            else IcebergDataCompactionContent.DEFAULT_MIN_DATA_FILE_MSE
        )

    def min_delete_file_number(self) -> int:
        return (
            self._min_delete_file_number
            if self._min_delete_file_number is not None
            else IcebergDataCompactionContent.DEFAULT_MIN_DELETE_FILE_NUMBER
        )

    def data_file_mse_weight(self) -> int:
        return (
            self._data_file_mse_weight
            if self._data_file_mse_weight is not None
            else IcebergDataCompactionContent.DEFAULT_DATA_FILE_MSE_WEIGHT
        )

    def delete_file_number_weight(self) -> int:
        return (
            self._delete_file_number_weight
            if self._delete_file_number_weight is not None
            else IcebergDataCompactionContent.DEFAULT_DELETE_FILE_NUMBER_WEIGHT
        )

    def max_partition_num(self) -> int:
        return (
            self._max_partition_num
            if self._max_partition_num is not None
            else IcebergDataCompactionContent.DEFAULT_MAX_PARTITION_NUM
        )

    def rewrite_strategy(self) -> str:
        return (
            self._rewrite_strategy
            if self._rewrite_strategy is not None
            else IcebergDataCompactionContent.DEFAULT_REWRITE_STRATEGY
        )

    def sort_order(self) -> str:
        return (
            self._sort_order
            if self._sort_order is not None
            else IcebergDataCompactionContent.DEFAULT_SORT_ORDER
        )

    def rewrite_options(self) -> dict:
        return dict(self._rewrite_options) if self._rewrite_options else {}

    def supported_object_types(self) -> set:
        return {
            MetadataObject.Type.CATALOG,
            MetadataObject.Type.SCHEMA,
            MetadataObject.Type.TABLE,
        }

    def properties(self) -> dict:
        return {
            IcebergDataCompactionContent.STRATEGY_TYPE_KEY: (
                IcebergDataCompactionContent.STRATEGY_TYPE_VALUE
            ),
            IcebergDataCompactionContent.JOB_TEMPLATE_NAME_KEY: (
                IcebergDataCompactionContent.JOB_TEMPLATE_NAME_VALUE
            ),
        }

    def rules(self) -> dict:
        return self.to_domain_content().rules()

    def validate(self) -> None:
        self.to_domain_content().validate()

    def to_domain_content(self) -> IcebergDataCompactionContent:
        return IcebergDataCompactionContent(
            self.min_data_file_mse(),
            self.min_delete_file_number(),
            self.data_file_mse_weight(),
            self.delete_file_number_weight(),
            self.max_partition_num(),
            self.rewrite_strategy(),
            self.sort_order(),
            self.rewrite_options(),
            self.execution_spec(),
        )


@_register_policy_content_dto("system_iceberg_orphan_file_removal")
@dataclass_json(undefined=Undefined.EXCLUDE)
@dataclass
class IcebergOrphanFileRemovalContentDTO(
    PolicyContentDTO, ExecutionPeersDTO
):  # pylint: disable=too-many-instance-attributes
    """Data transfer object for the content of an Iceberg orphan-file-removal
    policy."""

    _older_than_days: int | None = field(
        default=None, metadata=config(field_name="olderThanDays")
    )
    _location: str | None = field(default=None, metadata=config(field_name="location"))
    _dry_run: bool | None = field(default=None, metadata=config(field_name="dryRun"))
    _time_trigger: str | None = field(
        default=None, metadata=config(field_name="timeTrigger")
    )
    _commit_trigger: bool | None = field(
        default=None, metadata=config(field_name="commitTrigger")
    )
    _min_interval_ms: int | None = field(
        default=None, metadata=config(field_name="minIntervalMs")
    )
    _job_options: dict | None = field(
        default=None, metadata=config(field_name="jobOptions")
    )
    _updater_options: dict | None = field(
        default=None, metadata=config(field_name="updaterOptions")
    )

    def older_than_days(self) -> int:
        return (
            self._older_than_days
            if self._older_than_days is not None
            else IcebergOrphanFileRemovalContent.DEFAULT_OLDER_THAN_DAYS
        )

    def location(self) -> str | None:
        return self._location

    def dry_run(self) -> bool:
        return (
            self._dry_run
            if self._dry_run is not None
            else IcebergOrphanFileRemovalContent.DEFAULT_DRY_RUN
        )

    def supported_object_types(self) -> set:
        return {
            MetadataObject.Type.CATALOG,
            MetadataObject.Type.SCHEMA,
            MetadataObject.Type.TABLE,
        }

    def properties(self) -> dict:
        return {
            IcebergOrphanFileRemovalContent.STRATEGY_TYPE_KEY: (
                IcebergOrphanFileRemovalContent.STRATEGY_TYPE_VALUE
            ),
            IcebergOrphanFileRemovalContent.JOB_TEMPLATE_NAME_KEY: (
                IcebergOrphanFileRemovalContent.JOB_TEMPLATE_NAME_VALUE
            ),
        }

    def rules(self) -> dict:
        return self.to_domain_content().rules()

    def validate(self) -> None:
        self.to_domain_content().validate()

    def to_domain_content(self) -> IcebergOrphanFileRemovalContent:
        return IcebergOrphanFileRemovalContent(
            self.older_than_days(),
            self.location(),
            self.dry_run(),
            self.execution_spec(),
        )


@_register_policy_content_dto("system_iceberg_rewrite_manifests")
@dataclass_json(undefined=Undefined.EXCLUDE)
@dataclass
class IcebergRewriteManifestsContentDTO(
    PolicyContentDTO, ExecutionPeersDTO
):  # pylint: disable=too-many-instance-attributes
    """Data transfer object for the content of an Iceberg rewrite-manifests
    policy. Note that the threshold fields use snake_case wire names, unlike
    the camelCase execution peer fields."""

    _manifest_count_critical: int | None = field(
        default=None, metadata=config(field_name="manifest_count_critical")
    )
    _manifest_count_warning: int | None = field(
        default=None, metadata=config(field_name="manifest_count_warning")
    )
    _avg_manifest_size_threshold_bytes: int | None = field(
        default=None,
        metadata=config(field_name="avg_manifest_size_threshold_bytes"),
    )
    _spec_id: int | None = field(default=None, metadata=config(field_name="spec_id"))
    _use_caching: bool | None = field(
        default=None, metadata=config(field_name="use_caching")
    )
    _time_trigger: str | None = field(
        default=None, metadata=config(field_name="timeTrigger")
    )
    _commit_trigger: bool | None = field(
        default=None, metadata=config(field_name="commitTrigger")
    )
    _min_interval_ms: int | None = field(
        default=None, metadata=config(field_name="minIntervalMs")
    )
    _job_options: dict | None = field(
        default=None, metadata=config(field_name="jobOptions")
    )
    _updater_options: dict | None = field(
        default=None, metadata=config(field_name="updaterOptions")
    )

    def manifest_count_critical(self) -> int:
        return (
            self._manifest_count_critical
            if self._manifest_count_critical is not None
            else IcebergRewriteManifestsContent.DEFAULT_MANIFEST_COUNT_CRITICAL
        )

    def manifest_count_warning(self) -> int:
        return (
            self._manifest_count_warning
            if self._manifest_count_warning is not None
            else IcebergRewriteManifestsContent.DEFAULT_MANIFEST_COUNT_WARNING
        )

    def avg_manifest_size_threshold_bytes(self) -> int:
        return (
            self._avg_manifest_size_threshold_bytes
            if self._avg_manifest_size_threshold_bytes is not None
            else (
                IcebergRewriteManifestsContent.DEFAULT_AVG_MANIFEST_SIZE_THRESHOLD_BYTES
            )
        )

    def spec_id(self) -> int | None:
        return self._spec_id

    def use_caching(self) -> bool | None:
        return self._use_caching

    def supported_object_types(self) -> set:
        return {
            MetadataObject.Type.CATALOG,
            MetadataObject.Type.SCHEMA,
            MetadataObject.Type.TABLE,
        }

    def properties(self) -> dict:
        return {
            IcebergRewriteManifestsContent.STRATEGY_TYPE_KEY: (
                IcebergRewriteManifestsContent.STRATEGY_TYPE_VALUE
            ),
            IcebergRewriteManifestsContent.JOB_TEMPLATE_NAME_KEY: (
                IcebergRewriteManifestsContent.JOB_TEMPLATE_NAME_VALUE
            ),
        }

    def rules(self) -> dict:
        return self.to_domain_content().rules()

    def validate(self) -> None:
        self.to_domain_content().validate()

    def to_domain_content(self) -> IcebergRewriteManifestsContent:
        return IcebergRewriteManifestsContent(
            self.manifest_count_critical(),
            self.manifest_count_warning(),
            self.avg_manifest_size_threshold_bytes(),
            self.spec_id(),
            self.use_caching(),
            self.execution_spec(),
        )


@_register_policy_content_dto("system_iceberg_snapshot_expiration")
@dataclass_json(undefined=Undefined.EXCLUDE)
@dataclass
class IcebergSnapshotExpirationContentDTO(
    PolicyContentDTO, ExecutionPeersDTO
):  # pylint: disable=too-many-instance-attributes
    """Data transfer object for the content of an Iceberg
    snapshot-expiration policy."""

    _older_than_days: int | None = field(
        default=None, metadata=config(field_name="olderThanDays")
    )
    _retain_last: int | None = field(
        default=None, metadata=config(field_name="retainLast")
    )
    _stream_results: bool | None = field(
        default=None, metadata=config(field_name="streamResults")
    )
    _time_trigger: str | None = field(
        default=None, metadata=config(field_name="timeTrigger")
    )
    _commit_trigger: bool | None = field(
        default=None, metadata=config(field_name="commitTrigger")
    )
    _min_interval_ms: int | None = field(
        default=None, metadata=config(field_name="minIntervalMs")
    )
    _job_options: dict | None = field(
        default=None, metadata=config(field_name="jobOptions")
    )
    _updater_options: dict | None = field(
        default=None, metadata=config(field_name="updaterOptions")
    )

    def older_than_days(self) -> int:
        return (
            self._older_than_days
            if self._older_than_days is not None
            else IcebergSnapshotExpirationContent.DEFAULT_OLDER_THAN_DAYS
        )

    def retain_last(self) -> int:
        return (
            self._retain_last
            if self._retain_last is not None
            else IcebergSnapshotExpirationContent.DEFAULT_RETAIN_LAST
        )

    def stream_results(self) -> bool:
        return (
            self._stream_results
            if self._stream_results is not None
            else IcebergSnapshotExpirationContent.DEFAULT_STREAM_RESULTS
        )

    def supported_object_types(self) -> set:
        return {
            MetadataObject.Type.CATALOG,
            MetadataObject.Type.SCHEMA,
            MetadataObject.Type.TABLE,
        }

    def properties(self) -> dict:
        return {
            IcebergSnapshotExpirationContent.STRATEGY_TYPE_KEY: (
                IcebergSnapshotExpirationContent.STRATEGY_TYPE_VALUE
            ),
            IcebergSnapshotExpirationContent.JOB_TEMPLATE_NAME_KEY: (
                IcebergSnapshotExpirationContent.JOB_TEMPLATE_NAME_VALUE
            ),
        }

    def rules(self) -> dict:
        return self.to_domain_content().rules()

    def validate(self) -> None:
        self.to_domain_content().validate()

    def to_domain_content(self) -> IcebergSnapshotExpirationContent:
        return IcebergSnapshotExpirationContent(
            self.older_than_days(),
            self.retain_last(),
            self.stream_results(),
            self.execution_spec(),
        )


@_register_policy_content_dto("system_row_filter")
@dataclass_json(undefined=Undefined.EXCLUDE)
@dataclass
class RowFilterContentDTO(PolicyContentDTO):
    """Data transfer object for the content of a row-filter policy."""

    _expression: str | None = field(
        default=None, metadata=config(field_name="expression")
    )

    def expression(self) -> str | None:
        return self._expression

    def supported_object_types(self) -> set:
        return {MetadataObject.Type.TABLE}

    def properties(self) -> dict:
        return {}

    def rules(self) -> dict:
        return {"expression": self._expression}

    def validate(self) -> None:
        PolicyContent.validate(self)
        RowFilterContent.validate_source(self._expression, "row-filter expression")

    def to_domain_content(self) -> RowFilterContent:
        return RowFilterContent(self._expression)


@_register_policy_content_dto("system_column_mask")
@dataclass_json(undefined=Undefined.EXCLUDE)
@dataclass
class ColumnMaskContentDTO(PolicyContentDTO):
    """Data transfer object for the content of a column-mask policy."""

    _expression: str | None = field(
        default=None, metadata=config(field_name="expression")
    )

    def expression(self) -> str | None:
        return self._expression

    def supported_object_types(self) -> set:
        return {MetadataObject.Type.TABLE, MetadataObject.Type.COLUMN}

    def properties(self) -> dict:
        return {}

    def rules(self) -> dict:
        return {"expression": self._expression}

    def validate(self) -> None:
        PolicyContent.validate(self)
        ColumnMaskContent.validate_source(self._expression, "column-mask expression")

    def to_domain_content(self) -> ColumnMaskContent:
        return ColumnMaskContent(self._expression)


@_register_policy_content_dto("system_access_control")
@dataclass_json(undefined=Undefined.EXCLUDE)
@dataclass
class AccessControlContentDTO(PolicyContentDTO):
    """Data transfer object for the content of an access-control policy."""

    _privileges: list | None = field(
        default=None,
        metadata=config(
            field_name="privileges",
            encoder=_encode_privilege_names,
            decoder=_decode_privilege_names,
        ),
    )
    _applicable_roles: list | None = field(
        default=None, metadata=config(field_name="applicableRoles")
    )

    def privileges(self) -> list:
        return list(self._privileges) if self._privileges else []

    def applicable_roles(self) -> list:
        return list(self._applicable_roles) if self._applicable_roles else []

    def supported_object_types(self) -> set:
        return {
            MetadataObject.Type.CATALOG,
            MetadataObject.Type.SCHEMA,
            MetadataObject.Type.TABLE,
            MetadataObject.Type.VIEW,
            MetadataObject.Type.FILESET,
            MetadataObject.Type.TOPIC,
            MetadataObject.Type.MODEL,
            MetadataObject.Type.FUNCTION,
            MetadataObject.Type.SEMANTIC_MODEL,
        }

    def properties(self) -> dict:
        return {}

    def rules(self) -> dict:
        return self.to_domain_content().rules()

    def validate(self) -> None:
        self.to_domain_content().validate()

    def to_domain_content(self) -> AccessControlContent:
        return AccessControlContent(self.privileges(), self.applicable_roles())


@_exclude_none_fields
@dataclass_json(undefined=Undefined.EXCLUDE)
@dataclass
class PolicyAssociationSelectorDTO(DataClassJsonMixin):
    """Data transfer object for a policy association selector."""

    MAX_VALUE_LENGTH = 256

    _type: str | None = field(default=None, metadata=config(field_name="type"))
    _value: str | None = field(default=None, metadata=config(field_name="value"))

    @staticmethod
    def from_selector(
        selector: PolicyAssociationSelector | None,
    ) -> PolicyAssociationSelectorDTO | None:
        """Converts an API selector to its DTO form."""
        if selector is None:
            return None
        if isinstance(selector, AllValuesSelector):
            return PolicyAssociationSelectorDTO(AllValuesSelector.TYPE, None)
        if isinstance(selector, TagValueSelector):
            return PolicyAssociationSelectorDTO(TagValueSelector.TYPE, selector.value())
        raise IllegalArgumentException(
            f"Unknown policy association selector type: {type(selector)}"
        )

    def type(self) -> str | None:
        return self._type

    def value(self) -> str | None:
        return self._value

    def to_selector(self) -> PolicyAssociationSelector:
        """Converts this DTO to the API selector it represents."""
        self.validate()
        if self._type == AllValuesSelector.TYPE:
            return AllValuesSelector.get()
        return TagValueSelector.of(self._value)

    def validate(self) -> None:
        if self._type == AllValuesSelector.TYPE:
            Precondition.check_argument(
                self._value is None,
                f"{AllValuesSelector.TYPE} selector must not carry a value",
            )
        elif self._type == TagValueSelector.TYPE:
            Precondition.check_string_not_empty(
                self._value, "tag value cannot be blank"
            )
            Precondition.check_argument(
                len(self._value) <= self.MAX_VALUE_LENGTH,
                f"tag value cannot exceed {self.MAX_VALUE_LENGTH} characters",
            )
        else:
            raise IllegalArgumentException(
                f"Unknown policy association selector type: {self._type}"
            )


@dataclass
class PolicyDTO(Policy):
    """Data transfer object representing a policy.

    Serialization is implemented explicitly (rather than through
    dataclasses_json) because the concrete type of the ``content`` field is
    selected by the sibling ``policyType`` field, which dataclasses_json
    cannot dispatch on.
    """

    _name: str
    _policy_type: str
    _content: PolicyContentDTO
    _comment: str | None = None
    _enabled: bool = True
    _inherited: bool | None = None
    _audit: AuditDTO | None = None

    def name(self) -> str:
        return self._name

    def policy_type(self) -> str:
        return self._policy_type

    def comment(self) -> str | None:
        return self._comment

    def enabled(self) -> bool:
        return self._enabled

    def content(self) -> PolicyContentDTO:
        return self._content

    def inherited(self) -> bool | None:
        return self._inherited

    def audit_info(self) -> Audit:
        return self._audit

    def to_dict(self) -> dict:
        result = {
            "name": self._name,
            "policyType": self._policy_type,
            "enabled": self._enabled,
            "content": (self._content.to_dict() if self._content is not None else None),
        }
        if self._comment is not None:
            result["comment"] = self._comment
        if self._inherited is not None:
            result["inherited"] = self._inherited
        if self._audit is not None:
            result["audit"] = self._audit.to_dict()
        return result

    @classmethod
    def from_dict(cls, data: dict) -> PolicyDTO:
        content_data = data.get("content")
        audit_data = data.get("audit")
        return cls(
            _name=data.get("name"),
            _policy_type=data.get("policyType"),
            _content=(
                PolicyContentDTO.from_dict_by_type(content_data, data.get("policyType"))
                if content_data is not None
                else None
            ),
            _comment=data.get("comment"),
            _enabled=data.get("enabled", True),
            _inherited=data.get("inherited"),
            _audit=(
                AuditDTO.from_dict(audit_data, infer_missing=True)
                if audit_data is not None
                else None
            ),
        )

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str: str) -> PolicyDTO:
        return cls.from_dict(json.loads(json_str))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, PolicyDTO):
            return False
        return (
            self._name == other._name
            and self._comment == other._comment
            and self._policy_type == other._policy_type
            and self._enabled == other._enabled
            and self._content == other._content
            and self._audit == other._audit
        )

    def __hash__(self) -> int:
        return hash(
            (
                self._name,
                self._comment,
                self._policy_type,
                self._enabled,
                self._audit,
            )
        )

    @staticmethod
    def builder() -> PolicyDTO.Builder:
        """Returns a builder for creating a PolicyDTO."""
        return PolicyDTO.Builder()

    class Builder:
        """Builder for PolicyDTO; mirrors the Java PolicyDTO builder,
        including its validation."""

        def __init__(self):
            self._name: str | None = None
            self._comment: str | None = None
            self._policy_type: str | None = None
            self._enabled: bool = True
            self._content: PolicyContentDTO | None = None
            self._inherited: bool | None = None
            self._audit: AuditDTO | None = None

        def name(self, name: str) -> PolicyDTO.Builder:
            self._name = name
            return self

        def comment(self, comment: str | None) -> PolicyDTO.Builder:
            self._comment = comment
            return self

        def policy_type(self, policy_type: str) -> PolicyDTO.Builder:
            self._policy_type = policy_type
            return self

        def enabled(self, enabled: bool) -> PolicyDTO.Builder:
            self._enabled = enabled
            return self

        def content(self, content: PolicyContentDTO) -> PolicyDTO.Builder:
            self._content = content
            return self

        def inherited(self, inherited: bool | None) -> PolicyDTO.Builder:
            self._inherited = inherited
            return self

        def audit_info(self, audit: AuditDTO) -> PolicyDTO.Builder:
            self._audit = audit
            return self

        def build(self) -> PolicyDTO:
            Precondition.check_string_not_empty(
                self._name, "Policy name cannot be empty"
            )
            Precondition.check_string_not_empty(
                self._policy_type, "Policy type cannot be empty"
            )
            Precondition.check_argument(
                self._content is not None, "Policy content cannot be null"
            )
            self._content.validate()
            return PolicyDTO(
                _name=self._name,
                _policy_type=self._policy_type,
                _content=self._content,
                _comment=self._comment,
                _enabled=self._enabled,
                _inherited=self._inherited,
                _audit=self._audit,
            )


@dataclass_json(undefined=Undefined.EXCLUDE)
@dataclass
class PolicyForTagAssociationDTO(DataClassJsonMixin):
    """Data transfer object for a policy associated with a tag."""

    _policy: PolicyDTO | None = field(
        default=None,
        metadata=config(
            field_name="policy",
            encoder=lambda policy: policy.to_dict() if policy is not None else None,
            decoder=lambda data: (
                PolicyDTO.from_dict(data) if isinstance(data, dict) else data
            ),
        ),
    )
    _selector: PolicyAssociationSelectorDTO | None = field(
        default=None, metadata=config(field_name="selector")
    )

    def policy(self) -> PolicyDTO | None:
        return self._policy

    def selector(self) -> PolicyAssociationSelectorDTO | None:
        return self._selector


@dataclass_json(undefined=Undefined.EXCLUDE)
@dataclass
class TagForPolicyAssociationDTO(DataClassJsonMixin):
    """Data transfer object for a tag associated with a policy."""

    _tag: TagDTO | None = field(default=None, metadata=config(field_name="tag"))
    _selector: PolicyAssociationSelectorDTO | None = field(
        default=None, metadata=config(field_name="selector")
    )

    def tag(self) -> TagDTO | None:
        return self._tag

    def selector(self) -> PolicyAssociationSelectorDTO | None:
        return self._selector
