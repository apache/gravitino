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

# pylint: disable=too-many-lines

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from typing import ClassVar

from gravitino.api.authorization.privileges import Privilege
from gravitino.api.metadata_object import MetadataObject
from gravitino.exceptions.base import (
    IllegalArgumentException,
    UnsupportedOperationException,
)


class PolicyExecutionSpec:
    """Immutable maintenance execution peers for executable policy contents.

    Every peer is optional: a spec with every peer unset is the empty spec.
    """

    def __init__(
        self,
        time_trigger: str | None = None,
        commit_trigger: bool | None = None,
        min_interval_ms: int | None = None,
        job_options: dict[str, str] | None = None,
        updater_options: dict[str, str] | None = None,
    ):
        normalized_time_trigger = (
            time_trigger
            if time_trigger is not None and time_trigger.strip() != ""
            else None
        )
        normalized_job_options = dict(job_options) if job_options else {}
        normalized_updater_options = dict(updater_options) if updater_options else {}
        if min_interval_ms is not None and min_interval_ms < 0:
            raise IllegalArgumentException("minIntervalMs must be >= 0")
        self._validate_option_map(normalized_job_options, "jobOptions")
        self._validate_option_map(normalized_updater_options, "updaterOptions")
        self._time_trigger = normalized_time_trigger
        self._commit_trigger = commit_trigger
        self._min_interval_ms = min_interval_ms
        self._job_options = normalized_job_options
        self._updater_options = normalized_updater_options

    @staticmethod
    def of(
        time_trigger: str | None = None,
        commit_trigger: bool | None = None,
        min_interval_ms: int | None = None,
        job_options: dict[str, str] | None = None,
        updater_options: dict[str, str] | None = None,
    ) -> PolicyExecutionSpec:
        """Creates an immutable execution spec.

        Args:
            time_trigger: Optional cron expression.
            commit_trigger: Optional commit-trigger flag; any non-None value
                (including False) is retained.
            min_interval_ms: Optional cooldown in milliseconds.
            job_options: Optional job options.
            updater_options: Optional updater options.

        Returns:
            PolicyExecutionSpec: The execution spec, or the empty spec when
                every peer is unset.

        Raises:
            IllegalArgumentException: If min_interval_ms is negative or an
                option map has a blank key or value.
        """
        spec = PolicyExecutionSpec(
            time_trigger,
            commit_trigger,
            min_interval_ms,
            job_options,
            updater_options,
        )
        return spec if spec.has_any() else PolicyExecutionSpec.empty()

    @staticmethod
    def empty() -> PolicyExecutionSpec:
        """Returns the spec with every peer unset."""
        return _EMPTY_SPEC

    def time_trigger(self) -> str | None:
        """Returns the cron expression, or None when unset."""
        return self._time_trigger

    def commit_trigger(self) -> bool | None:
        """Returns the commit-trigger flag when explicitly supplied (including
        False), or None when unset."""
        return self._commit_trigger

    def min_interval_ms(self) -> int | None:
        """Returns the minimum interval in milliseconds, or None when unset."""
        return self._min_interval_ms

    def job_options(self) -> dict[str, str]:
        """Returns the job options; never None."""
        return dict(self._job_options)

    def updater_options(self) -> dict[str, str]:
        """Returns the updater options; never None."""
        return dict(self._updater_options)

    def has_any(self) -> bool:
        """Returns True when any peer is set."""
        return (
            (self._time_trigger is not None and self._time_trigger.strip() != "")
            or self._commit_trigger is not None
            or self._min_interval_ms is not None
            or len(self._job_options) > 0
            or len(self._updater_options) > 0
        )

    @staticmethod
    def _validate_option_map(options: dict[str, str], map_name: str) -> None:
        for key, value in options.items():
            if key is None or key.strip() == "":
                raise IllegalArgumentException(f"{map_name} key is blank")
            if value is None or value.strip() == "":
                raise IllegalArgumentException(
                    f"{map_name} '{key}' must have non-empty value"
                )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, PolicyExecutionSpec):
            return False
        return (
            self._time_trigger == other._time_trigger
            and self._commit_trigger == other._commit_trigger
            and self._min_interval_ms == other._min_interval_ms
            and self._job_options == other._job_options
            and self._updater_options == other._updater_options
        )

    def __hash__(self) -> int:
        return hash(
            (
                self._time_trigger,
                self._commit_trigger,
                self._min_interval_ms,
                frozenset(self._job_options.items()),
                frozenset(self._updater_options.items()),
            )
        )

    def __repr__(self) -> str:
        # Never render option map values; credentials may appear in them.
        return (
            f"PolicyExecutionSpec{{timeTrigger='{self._time_trigger}', "
            f"commitTrigger={self._commit_trigger}, "
            f"minIntervalMs={self._min_interval_ms}, "
            f"jobOptionsKeys={sorted(self._job_options.keys())}, "
            f"updaterOptionsKeys={sorted(self._updater_options.keys())}}}"
        )


_EMPTY_SPEC = PolicyExecutionSpec()


class PolicyContent(ABC):
    """The content of a policy: the rules plus the object types and properties
    that describe them."""

    @abstractmethod
    def supported_object_types(self) -> set[MetadataObject.Type]:
        """Returns the set of metadata object types that the policy can be
        applied to."""
        raise NotImplementedError()

    @abstractmethod
    def properties(self) -> dict[str, str]:
        """Returns the additional properties of the policy."""
        raise NotImplementedError()

    def rules(self) -> dict[str, object]:
        """Returns all rules in the policy content, as a map of rule names to
        their corresponding rule objects."""
        # backward compatibility
        raise UnsupportedOperationException("Does support get all rules.")

    def validate(self) -> None:
        """Validates the policy content.

        Raises:
            IllegalArgumentException: If the content is invalid.
        """
        if not self.supported_object_types():
            raise IllegalArgumentException("supportedObjectTypes cannot be empty")


class ExecutablePolicyContent(PolicyContent):
    """A policy content that carries maintenance execution peers."""

    @abstractmethod
    def execution_spec(self) -> PolicyExecutionSpec:
        """Returns the immutable execution peers; never None."""
        raise NotImplementedError()


class AbstractExecutablePolicyContent(ExecutablePolicyContent):
    """Base class for executable policy contents."""

    def __init__(self, execution_spec: PolicyExecutionSpec | None = None):
        self._execution_spec = (
            execution_spec
            if execution_spec is not None
            else PolicyExecutionSpec.empty()
        )

    def execution_spec(self) -> PolicyExecutionSpec:
        return self._execution_spec

    def _execution_fields_equal(self, that: AbstractExecutablePolicyContent) -> bool:
        return self.execution_spec() == that.execution_spec()

    def _execution_fields_hash_code(self) -> int:
        return hash(self.execution_spec())


class ReadRestrictionContent(PolicyContent):
    """Base class for contents that carry an authored read-restriction
    expression (row filter, column mask)."""

    MAX_SOURCE_LENGTH_BYTES: ClassVar[int] = 16 * 1024

    def properties(self) -> dict[str, str]:
        return {}

    @staticmethod
    def validate_source(source: str | None, field_name: str) -> None:
        if source is None or source.strip() == "":
            raise IllegalArgumentException(f"{field_name} cannot be blank")
        if len(source.encode("utf-8")) > ReadRestrictionContent.MAX_SOURCE_LENGTH_BYTES:
            raise IllegalArgumentException(
                f"{field_name} cannot exceed "
                f"{ReadRestrictionContent.MAX_SOURCE_LENGTH_BYTES} UTF-8 bytes"
            )


class RowFilterContent(ReadRestrictionContent):
    """Content of a row-filter policy: an expression evaluated per row of a
    tagged table."""

    _SUPPORTED_OBJECT_TYPES: ClassVar[set] = {MetadataObject.Type.TABLE}

    def __init__(self, expression: str | None):
        self._expression = expression
        self.validate()

    def expression(self) -> str | None:
        """Returns the authored row-filter expression."""
        return self._expression

    def supported_object_types(self) -> set[MetadataObject.Type]:
        return set(self._SUPPORTED_OBJECT_TYPES)

    def rules(self) -> dict[str, object]:
        return {"expression": self._expression}

    def validate(self) -> None:
        super().validate()
        self.validate_source(self._expression, "row-filter expression")

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, RowFilterContent):
            return False
        return self._expression == other._expression

    def __hash__(self) -> int:
        return hash(self._expression)

    def __repr__(self) -> str:
        return f"RowFilterContent{{expression='{self._expression}'}}"


class ColumnMaskContent(ReadRestrictionContent):
    """Content of a column-mask policy: an expression that transforms the
    values of a tagged table or column."""

    _SUPPORTED_OBJECT_TYPES: ClassVar[set] = {
        MetadataObject.Type.TABLE,
        MetadataObject.Type.COLUMN,
    }

    def __init__(self, expression: str | None):
        self._expression = expression
        self.validate()

    def expression(self) -> str | None:
        """Returns the authored column-mask expression."""
        return self._expression

    def supported_object_types(self) -> set[MetadataObject.Type]:
        return set(self._SUPPORTED_OBJECT_TYPES)

    def rules(self) -> dict[str, object]:
        return {"expression": self._expression}

    def validate(self) -> None:
        super().validate()
        self.validate_source(self._expression, "column-mask expression")

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ColumnMaskContent):
            return False
        return self._expression == other._expression

    def __hash__(self) -> int:
        return hash(self._expression)

    def __repr__(self) -> str:
        return f"ColumnMaskContent{{expression='{self._expression}'}}"


class AccessControlContent(PolicyContent):
    """Content of an access-control policy: the privileges a tag confers on
    the tagged object, and the roles that satisfy the rule's condition."""

    PRIVILEGES_KEY: ClassVar[str] = "privileges"
    APPLICABLE_ROLES_KEY: ClassVar[str] = "applicableRoles"

    # The privileges a tag is allowed to confer. An allowlist rather than a
    # denylist, so the boundary fails closed: a privilege added to
    # Privilege.Name later confers nothing through a tag until it is added
    # here deliberately.
    PERMITTED_PRIVILEGES: ClassVar[frozenset] = frozenset(
        {
            Privilege.Name.SELECT_TABLE,
            Privilege.Name.MODIFY_TABLE,
            Privilege.Name.PROBE_TABLE_LIKE,
            Privilege.Name.SELECT_VIEW,
            Privilege.Name.READ_FILESET,
            Privilege.Name.WRITE_FILESET,
            Privilege.Name.CONSUME_TOPIC,
            Privilege.Name.PRODUCE_TOPIC,
            Privilege.Name.USE_MODEL,
            Privilege.Name.EXECUTE_FUNCTION,
            Privilege.Name.USE_SEMANTIC_MODEL,
        }
    )

    _SUPPORTED_OBJECT_TYPES: ClassVar[set] = {
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

    def __init__(
        self,
        privileges: list[Privilege.Name] | None,
        applicable_roles: list[str] | None,
    ):
        # Copied without rejecting nulls so that validate() is the single
        # place that reports a bad entry, with the offending value named.
        self._privileges = list(privileges) if privileges is not None else []
        self._applicable_roles = (
            list(applicable_roles) if applicable_roles is not None else []
        )

    def privileges(self) -> list[Privilege.Name]:
        """Returns the privileges this rule confers on the tagged object."""
        return list(self._privileges)

    def applicable_roles(self) -> list[str]:
        """Returns the roles that satisfy this rule's condition."""
        return list(self._applicable_roles)

    def supported_object_types(self) -> set[MetadataObject.Type]:
        return set(self._SUPPORTED_OBJECT_TYPES)

    def properties(self) -> dict[str, str]:
        return {}

    def rules(self) -> dict[str, object]:
        return {
            self.PRIVILEGES_KEY: [
                privilege.name if privilege is not None else None
                for privilege in self._privileges
            ],
            self.APPLICABLE_ROLES_KEY: list(self._applicable_roles),
        }

    def validate(self) -> None:
        super().validate()
        if not self._privileges:
            raise IllegalArgumentException("privileges cannot be empty")
        for privilege in self._privileges:
            if privilege not in self.PERMITTED_PRIVILEGES:
                raise IllegalArgumentException(
                    f"privilege {privilege} cannot be conferred by a tag, "
                    f"permitted privileges are {sorted(p.name for p in self.PERMITTED_PRIVILEGES)}"
                )
        if not self._applicable_roles:
            raise IllegalArgumentException("applicableRoles cannot be empty")
        for role in self._applicable_roles:
            if role is None or role.strip() == "":
                raise IllegalArgumentException("applicable role name cannot be blank")

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, AccessControlContent):
            return False
        return (
            self._privileges == other._privileges
            and self._applicable_roles == other._applicable_roles
        )

    def __hash__(self) -> int:
        return hash((tuple(self._privileges), tuple(self._applicable_roles)))

    def __repr__(self) -> str:
        return (
            f"AccessControlContent{{privileges={self._privileges}, "
            f"applicableRoles={self._applicable_roles}}}"
        )


class IcebergDataCompactionContent(  # pylint: disable=too-many-instance-attributes
    AbstractExecutablePolicyContent
):
    """Content of an Iceberg data-compaction policy."""

    STRATEGY_TYPE_KEY: ClassVar[str] = "strategy.type"
    JOB_TEMPLATE_NAME_KEY: ClassVar[str] = "job.template-name"
    JOB_OPTIONS_PREFIX: ClassVar[str] = "job.options."
    REWRITE_STRATEGY_KEY: ClassVar[str] = "rewriteStrategy"
    SORT_ORDER_KEY: ClassVar[str] = "sortOrder"
    TRIGGER_EXPR_KEY: ClassVar[str] = "trigger-expr"
    SCORE_EXPR_KEY: ClassVar[str] = "score-expr"
    MIN_DATA_FILE_MSE_KEY: ClassVar[str] = "minDataFileMse"
    MIN_DELETE_FILE_NUMBER_KEY: ClassVar[str] = "minDeleteFileNumber"
    DATA_FILE_MSE_WEIGHT_KEY: ClassVar[str] = "dataFileMseWeight"
    DELETE_FILE_NUMBER_WEIGHT_KEY: ClassVar[str] = "deleteFileNumberWeight"
    MAX_PARTITION_NUM_KEY: ClassVar[str] = "max-partition-num"
    REWRITE_STRATEGY_BINPACK: ClassVar[str] = "binpack"
    REWRITE_STRATEGY_SORT: ClassVar[str] = "sort"
    DATA_FILE_MSE_METRIC: ClassVar[str] = "custom-data-file-mse"
    DELETE_FILE_NUMBER_METRIC: ClassVar[str] = "custom-delete-file-number"

    DEFAULT_MIN_DATA_FILE_MSE: ClassVar[int] = 405323966463344
    DEFAULT_MIN_DELETE_FILE_NUMBER: ClassVar[int] = 1
    DEFAULT_DATA_FILE_MSE_WEIGHT: ClassVar[int] = 1
    DEFAULT_DELETE_FILE_NUMBER_WEIGHT: ClassVar[int] = 100
    DEFAULT_MAX_PARTITION_NUM: ClassVar[int] = 50
    DEFAULT_REWRITE_STRATEGY: ClassVar[str] = REWRITE_STRATEGY_BINPACK
    DEFAULT_SORT_ORDER: ClassVar[str] = ""
    DEFAULT_REWRITE_OPTIONS: ClassVar[dict] = {}

    STRATEGY_TYPE_VALUE: ClassVar[str] = "iceberg-data-compaction"
    JOB_TEMPLATE_NAME_VALUE: ClassVar[str] = "builtin-iceberg-rewrite-data-files"

    _SUPPORTED_REWRITE_STRATEGIES: ClassVar[set] = {
        REWRITE_STRATEGY_BINPACK,
        REWRITE_STRATEGY_SORT,
    }
    # Names and value kinds mirror Iceberg RewriteDataFiles /
    # SizeBasedFileRewriter, as in the Java content's supported-option map.
    _SUPPORTED_REWRITE_OPTIONS: ClassVar[dict] = {
        "target-file-size-bytes": "positive_long",
        "min-file-size-bytes": "positive_long",
        "max-file-size-bytes": "positive_long",
        "max-file-group-size-bytes": "positive_long",
        "min-input-files": "positive_int",
        "max-concurrent-file-group-rewrites": "positive_int",
        "partial-progress.max-commits": "positive_int",
        "partial-progress.max-failed-commits": "non_negative_int",
        "output-spec-id": "non_negative_int",
        "delete-file-threshold": "non_negative_int",
        "rewrite-all": "boolean",
        "partial-progress.enabled": "boolean",
        "use-starting-sequence-number": "boolean",
        "remove-dangling-deletes": "boolean",
        "rewrite-job-order": "rewrite_job_order",
        "delete-ratio-threshold": "ratio",
    }
    _REWRITE_JOB_ORDERS: ClassVar[set] = {
        "bytes-asc",
        "bytes-desc",
        "files-asc",
        "files-desc",
        "none",
    }
    _OPTION_KEY_PATTERN: ClassVar[re.Pattern] = re.compile(r"[A-Za-z0-9._-]+")
    _INTEGER_PATTERN: ClassVar[re.Pattern] = re.compile(r"[+-]?[0-9]+")
    _SUPPORTED_OBJECT_TYPES: ClassVar[set] = {
        MetadataObject.Type.CATALOG,
        MetadataObject.Type.SCHEMA,
        MetadataObject.Type.TABLE,
    }

    TRIGGER_EXPR: ClassVar[str] = (
        DATA_FILE_MSE_METRIC
        + " >= "
        + MIN_DATA_FILE_MSE_KEY
        + " || "
        + DELETE_FILE_NUMBER_METRIC
        + " >= "
        + MIN_DELETE_FILE_NUMBER_KEY
    )
    SCORE_EXPR: ClassVar[str] = (
        DATA_FILE_MSE_METRIC
        + " * "
        + DATA_FILE_MSE_WEIGHT_KEY
        + " + "
        + DELETE_FILE_NUMBER_METRIC
        + " * "
        + DELETE_FILE_NUMBER_WEIGHT_KEY
    )

    def __init__(
        self,
        min_data_file_mse: int | None = None,
        min_delete_file_number: int | None = None,
        data_file_mse_weight: int | None = None,
        delete_file_number_weight: int | None = None,
        max_partition_num: int | None = None,
        rewrite_strategy: str | None = None,
        sort_order: str | None = None,
        rewrite_options: dict[str, str] | None = None,
        execution_spec: PolicyExecutionSpec | None = None,
    ):
        super().__init__(execution_spec)
        self._min_data_file_mse = (
            min_data_file_mse
            if min_data_file_mse is not None
            else self.DEFAULT_MIN_DATA_FILE_MSE
        )
        self._min_delete_file_number = (
            min_delete_file_number
            if min_delete_file_number is not None
            else self.DEFAULT_MIN_DELETE_FILE_NUMBER
        )
        self._data_file_mse_weight = (
            data_file_mse_weight
            if data_file_mse_weight is not None
            else self.DEFAULT_DATA_FILE_MSE_WEIGHT
        )
        self._delete_file_number_weight = (
            delete_file_number_weight
            if delete_file_number_weight is not None
            else self.DEFAULT_DELETE_FILE_NUMBER_WEIGHT
        )
        self._max_partition_num = (
            max_partition_num
            if max_partition_num is not None
            else self.DEFAULT_MAX_PARTITION_NUM
        )
        self._rewrite_strategy = (
            rewrite_strategy.strip().lower()
            if rewrite_strategy is not None
            else self.DEFAULT_REWRITE_STRATEGY
        )
        self._sort_order = (
            sort_order.strip() if sort_order is not None else self.DEFAULT_SORT_ORDER
        )
        self._rewrite_options = (
            dict(rewrite_options)
            if rewrite_options is not None
            else dict(self.DEFAULT_REWRITE_OPTIONS)
        )

    def min_data_file_mse(self) -> int:
        return self._min_data_file_mse

    def min_delete_file_number(self) -> int:
        return self._min_delete_file_number

    def data_file_mse_weight(self) -> int:
        return self._data_file_mse_weight

    def delete_file_number_weight(self) -> int:
        return self._delete_file_number_weight

    def max_partition_num(self) -> int:
        return self._max_partition_num

    def rewrite_strategy(self) -> str:
        return self._rewrite_strategy

    def sort_order(self) -> str:
        return self._sort_order

    def rewrite_options(self) -> dict[str, str]:
        return dict(self._rewrite_options)

    def supported_object_types(self) -> set[MetadataObject.Type]:
        return set(self._SUPPORTED_OBJECT_TYPES)

    def properties(self) -> dict[str, str]:
        # Properties keep stable strategy/job identity; thresholds and
        # scoring knobs belong to rules.
        return {
            self.STRATEGY_TYPE_KEY: self.STRATEGY_TYPE_VALUE,
            self.JOB_TEMPLATE_NAME_KEY: self.JOB_TEMPLATE_NAME_VALUE,
        }

    def rules(self) -> dict[str, object]:
        rules: dict[str, object] = {
            self.MIN_DATA_FILE_MSE_KEY: self._min_data_file_mse,
            self.MIN_DELETE_FILE_NUMBER_KEY: self._min_delete_file_number,
            self.DATA_FILE_MSE_WEIGHT_KEY: self._data_file_mse_weight,
            self.DELETE_FILE_NUMBER_WEIGHT_KEY: self._delete_file_number_weight,
            self.MAX_PARTITION_NUM_KEY: self._max_partition_num,
            self.TRIGGER_EXPR_KEY: self.TRIGGER_EXPR,
            self.SCORE_EXPR_KEY: self.SCORE_EXPR,
            self.REWRITE_STRATEGY_KEY: self._rewrite_strategy,
            self.SORT_ORDER_KEY: self._sort_order,
        }
        for key, value in self._rewrite_options.items():
            rules[self.JOB_OPTIONS_PREFIX + key] = value
        return rules

    def validate(self) -> None:
        super().validate()
        # All fields are defaulted in the constructor, so only range checks
        # are needed here.
        if self._min_data_file_mse < 0:
            raise IllegalArgumentException("minDataFileMse must be >= 0")
        if self._min_delete_file_number < 0:
            raise IllegalArgumentException("minDeleteFileNumber must be >= 0")
        if self._data_file_mse_weight < 0:
            raise IllegalArgumentException("dataFileMseWeight must be >= 0")
        if self._delete_file_number_weight < 0:
            raise IllegalArgumentException("deleteFileNumberWeight must be >= 0")
        if self._max_partition_num <= 0:
            raise IllegalArgumentException("maxPartitionNum must be > 0")
        if self._rewrite_strategy not in self._SUPPORTED_REWRITE_STRATEGIES:
            raise IllegalArgumentException(
                f"rewriteStrategy must be one of "
                f"{sorted(self._SUPPORTED_REWRITE_STRATEGIES)}, but got "
                f"'{self._rewrite_strategy}'"
            )
        if self._rewrite_strategy == self.REWRITE_STRATEGY_SORT:
            if self._sort_order.strip() == "":
                raise IllegalArgumentException(
                    f"sortOrder must be set when rewriteStrategy is "
                    f"'{self.REWRITE_STRATEGY_SORT}'"
                )
        elif self._sort_order.strip() != "":
            raise IllegalArgumentException(
                f"sortOrder must be empty when rewriteStrategy is "
                f"'{self.REWRITE_STRATEGY_BINPACK}'"
            )
        for key, value in self._rewrite_options.items():
            self._validate_rewrite_option(key, value)

    @staticmethod
    def _validate_rewrite_option(key: str, value: str) -> None:
        # Mirrors the Java content's validateRewriteOption contract.
        if key is None or key.strip() == "":
            raise IllegalArgumentException("rewrite option key is blank")
        if not IcebergDataCompactionContent._OPTION_KEY_PATTERN.fullmatch(key):
            raise IllegalArgumentException(
                f"rewrite option key '{key}' contains illegal characters"
            )
        if key.startswith(IcebergDataCompactionContent.JOB_OPTIONS_PREFIX):
            raise IllegalArgumentException(
                f"rewrite option key '{key}' must not start with "
                f"'{IcebergDataCompactionContent.JOB_OPTIONS_PREFIX}'"
            )
        if value is None or value.strip() == "":
            raise IllegalArgumentException(
                f"rewrite option '{key}' must have non-empty value"
            )
        kind = IcebergDataCompactionContent._SUPPORTED_REWRITE_OPTIONS.get(key)
        if kind is None:
            raise IllegalArgumentException(
                f"rewrite option '{key}' is not supported by "
                f"rewrite_data_files; supported options are "
                f"{sorted(IcebergDataCompactionContent._SUPPORTED_REWRITE_OPTIONS)}"
            )
        IcebergDataCompactionContent._check_rewrite_option_value(key, value, kind)

    @staticmethod
    def _check_rewrite_option_value(key: str, value: str, kind: str) -> None:
        if kind in ("positive_long", "positive_int", "non_negative_int"):
            IcebergDataCompactionContent._check_integer_option(key, value, kind)
        elif kind == "boolean":
            if value.lower() not in ("true", "false"):
                raise IllegalArgumentException(
                    f"rewrite option '{key}' must be true or false"
                )
        elif kind == "rewrite_job_order":
            if value.lower() not in IcebergDataCompactionContent._REWRITE_JOB_ORDERS:
                raise IllegalArgumentException(
                    f"rewrite option '{key}' must be one of "
                    f"{sorted(IcebergDataCompactionContent._REWRITE_JOB_ORDERS)}, "
                    f"but got '{value}'"
                )
        elif kind == "ratio":
            IcebergDataCompactionContent._check_ratio_option(key, value)

    @staticmethod
    def _check_integer_option(key: str, value: str, kind: str) -> None:
        bits = 64 if kind == "positive_long" else 32
        kind_name = "a long" if kind == "positive_long" else "an int"
        text = value.strip()
        if not IcebergDataCompactionContent._INTEGER_PATTERN.fullmatch(text):
            raise IllegalArgumentException(
                f"rewrite option '{key}' must be {kind_name}"
            )
        parsed = int(text)
        if not -(2 ** (bits - 1)) <= parsed <= 2 ** (bits - 1) - 1:
            raise IllegalArgumentException(
                f"rewrite option '{key}' must be {kind_name}"
            )
        if kind == "non_negative_int":
            if parsed < 0:
                raise IllegalArgumentException(
                    f"rewrite option '{key}' must be a non-negative int"
                )
        elif parsed <= 0:
            label = "a positive long" if kind == "positive_long" else "a positive int"
            raise IllegalArgumentException(f"rewrite option '{key}' must be {label}")

    @staticmethod
    def _check_ratio_option(key: str, value: str) -> None:
        try:
            ratio = float(value.strip())
        except ValueError:
            raise IllegalArgumentException(
                f"rewrite option '{key}' must be a number between 0 and 1"
            ) from None
        if not 0.0 <= ratio <= 1.0:
            raise IllegalArgumentException(
                f"rewrite option '{key}' must be a number between 0 and 1"
            )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, IcebergDataCompactionContent):
            return False
        return (
            self._execution_fields_equal(other)
            and self._min_data_file_mse == other._min_data_file_mse
            and self._min_delete_file_number == other._min_delete_file_number
            and self._data_file_mse_weight == other._data_file_mse_weight
            and self._delete_file_number_weight == other._delete_file_number_weight
            and self._max_partition_num == other._max_partition_num
            and self._rewrite_strategy == other._rewrite_strategy
            and self._sort_order == other._sort_order
            and self._rewrite_options == other._rewrite_options
        )

    def __hash__(self) -> int:
        return hash(
            (
                self._execution_fields_hash_code(),
                self._min_data_file_mse,
                self._min_delete_file_number,
                self._data_file_mse_weight,
                self._delete_file_number_weight,
                self._max_partition_num,
                self._rewrite_strategy,
                self._sort_order,
                frozenset(self._rewrite_options.items()),
            )
        )


class IcebergOrphanFileRemovalContent(AbstractExecutablePolicyContent):
    """Content of an Iceberg orphan-file-removal policy."""

    STRATEGY_TYPE_KEY: ClassVar[str] = "strategy.type"
    JOB_TEMPLATE_NAME_KEY: ClassVar[str] = "job.template-name"
    TRIGGER_EXPR_KEY: ClassVar[str] = "trigger-expr"
    SCORE_EXPR_KEY: ClassVar[str] = "score-expr"
    JOB_OPTIONS_PREFIX: ClassVar[str] = "job.options."
    OLDER_THAN_DAYS_KEY: ClassVar[str] = "olderThanDays"
    LOCATION_KEY: ClassVar[str] = "location"
    DRY_RUN_KEY: ClassVar[str] = "dryRun"

    DEFAULT_OLDER_THAN_DAYS: ClassVar[int] = 3
    DEFAULT_DRY_RUN: ClassVar[bool] = False
    MAX_OLDER_THAN_DAYS: ClassVar[int] = 36500

    STRATEGY_TYPE_VALUE: ClassVar[str] = "iceberg-orphan-file-removal"
    JOB_TEMPLATE_NAME_VALUE: ClassVar[str] = "builtin-iceberg-remove-orphan-files"

    _SUPPORTED_OBJECT_TYPES: ClassVar[set] = {
        MetadataObject.Type.CATALOG,
        MetadataObject.Type.SCHEMA,
        MetadataObject.Type.TABLE,
    }

    def __init__(
        self,
        older_than_days: int | None = None,
        location: str | None = None,
        dry_run: bool | None = None,
        execution_spec: PolicyExecutionSpec | None = None,
    ):
        super().__init__(execution_spec)
        self._older_than_days = (
            older_than_days
            if older_than_days is not None
            else self.DEFAULT_OLDER_THAN_DAYS
        )
        self._location = location
        self._dry_run = dry_run if dry_run is not None else self.DEFAULT_DRY_RUN

    def older_than_days(self) -> int:
        return self._older_than_days

    def location(self) -> str | None:
        return self._location

    def dry_run(self) -> bool:
        return self._dry_run

    def supported_object_types(self) -> set[MetadataObject.Type]:
        return set(self._SUPPORTED_OBJECT_TYPES)

    def properties(self) -> dict[str, str]:
        return {
            self.STRATEGY_TYPE_KEY: self.STRATEGY_TYPE_VALUE,
            self.JOB_TEMPLATE_NAME_KEY: self.JOB_TEMPLATE_NAME_VALUE,
        }

    def rules(self) -> dict[str, object]:
        rules: dict[str, object] = {
            self.TRIGGER_EXPR_KEY: "true",
            self.SCORE_EXPR_KEY: "1",
            self.JOB_OPTIONS_PREFIX + self.OLDER_THAN_DAYS_KEY: self._older_than_days,
            self.JOB_OPTIONS_PREFIX + self.DRY_RUN_KEY: self._dry_run,
        }
        if self._location is not None:
            rules[self.JOB_OPTIONS_PREFIX + self.LOCATION_KEY] = self._location
        return rules

    @staticmethod
    def validate_older_than_days(days: int) -> None:
        if not 1 <= days <= IcebergOrphanFileRemovalContent.MAX_OLDER_THAN_DAYS:
            raise IllegalArgumentException(
                f"olderThanDays must be between 1 and "
                f"{IcebergOrphanFileRemovalContent.MAX_OLDER_THAN_DAYS}"
            )

    def validate(self) -> None:
        super().validate()
        if self.execution_spec().commit_trigger() is True:
            raise IllegalArgumentException(
                "commitTrigger is not allowed for orphan file removal"
            )
        self.validate_older_than_days(self._older_than_days)
        if self._location is not None:
            if self._location.strip() == "":
                raise IllegalArgumentException("location must not be blank")
            if self._location != self._location.strip():
                raise IllegalArgumentException(
                    "location must not have leading or trailing whitespace"
                )
            if ".." in self._location:
                raise IllegalArgumentException(
                    f"location must not contain '..' path segments: {self._location}"
                )
            if "\\" in self._location:
                raise IllegalArgumentException(
                    f"location must not contain '\\': {self._location}"
                )
            if "%" in self._location:
                raise IllegalArgumentException(
                    f"location must not contain percent-encoding: {self._location}"
                )
            if "?" in self._location or "#" in self._location:
                raise IllegalArgumentException(
                    f"location must not contain query or fragment: {self._location}"
                )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, IcebergOrphanFileRemovalContent):
            return False
        return (
            self._execution_fields_equal(other)
            and self._older_than_days == other._older_than_days
            and self._location == other._location
            and self._dry_run == other._dry_run
        )

    def __hash__(self) -> int:
        return hash(
            (
                self._execution_fields_hash_code(),
                self._older_than_days,
                self._location,
                self._dry_run,
            )
        )


class IcebergRewriteManifestsContent(AbstractExecutablePolicyContent):
    """Content of an Iceberg rewrite-manifests policy."""

    STRATEGY_TYPE_KEY: ClassVar[str] = "strategy.type"
    JOB_TEMPLATE_NAME_KEY: ClassVar[str] = "job.template-name"
    TRIGGER_EXPR_KEY: ClassVar[str] = "trigger-expr"
    SCORE_EXPR_KEY: ClassVar[str] = "score-expr"
    MANIFEST_COUNT_CRITICAL: ClassVar[str] = "manifest_count_critical"
    MANIFEST_COUNT_WARNING: ClassVar[str] = "manifest_count_warning"
    AVG_MANIFEST_SIZE_THRESHOLD_BYTES: ClassVar[str] = (
        "avg_manifest_size_threshold_bytes"
    )
    SPEC_ID: ClassVar[str] = "spec_id"
    USE_CACHING: ClassVar[str] = "use_caching"

    DEFAULT_MANIFEST_COUNT_CRITICAL: ClassVar[int] = 500
    DEFAULT_MANIFEST_COUNT_WARNING: ClassVar[int] = 100
    DEFAULT_AVG_MANIFEST_SIZE_THRESHOLD_BYTES: ClassVar[int] = 8388608

    STRATEGY_TYPE_VALUE: ClassVar[str] = "iceberg-rewrite-manifests"
    JOB_TEMPLATE_NAME_VALUE: ClassVar[str] = "builtin-iceberg-rewrite-manifests"

    _SUPPORTED_OBJECT_TYPES: ClassVar[set] = {
        MetadataObject.Type.CATALOG,
        MetadataObject.Type.SCHEMA,
        MetadataObject.Type.TABLE,
    }

    def __init__(
        self,
        manifest_count_critical: int | None = None,
        manifest_count_warning: int | None = None,
        avg_manifest_size_threshold_bytes: int | None = None,
        spec_id: int | None = None,
        use_caching: bool | None = None,
        execution_spec: PolicyExecutionSpec | None = None,
    ):
        super().__init__(execution_spec)
        self._manifest_count_critical = (
            manifest_count_critical
            if manifest_count_critical is not None
            else self.DEFAULT_MANIFEST_COUNT_CRITICAL
        )
        self._manifest_count_warning = (
            manifest_count_warning
            if manifest_count_warning is not None
            else self.DEFAULT_MANIFEST_COUNT_WARNING
        )
        self._avg_manifest_size_threshold_bytes = (
            avg_manifest_size_threshold_bytes
            if avg_manifest_size_threshold_bytes is not None
            else self.DEFAULT_AVG_MANIFEST_SIZE_THRESHOLD_BYTES
        )
        self._spec_id = spec_id
        self._use_caching = use_caching

    def manifest_count_critical(self) -> int:
        return self._manifest_count_critical

    def manifest_count_warning(self) -> int:
        return self._manifest_count_warning

    def avg_manifest_size_threshold_bytes(self) -> int:
        return self._avg_manifest_size_threshold_bytes

    def spec_id(self) -> int | None:
        return self._spec_id

    def use_caching(self) -> bool | None:
        return self._use_caching

    def supported_object_types(self) -> set[MetadataObject.Type]:
        return set(self._SUPPORTED_OBJECT_TYPES)

    def properties(self) -> dict[str, str]:
        return {
            self.STRATEGY_TYPE_KEY: self.STRATEGY_TYPE_VALUE,
            self.JOB_TEMPLATE_NAME_KEY: self.JOB_TEMPLATE_NAME_VALUE,
        }

    def rules(self) -> dict[str, object]:
        rules: dict[str, object] = {
            self.MANIFEST_COUNT_CRITICAL: self._manifest_count_critical,
            self.MANIFEST_COUNT_WARNING: self._manifest_count_warning,
            self.AVG_MANIFEST_SIZE_THRESHOLD_BYTES: (
                self._avg_manifest_size_threshold_bytes
            ),
        }
        if self._spec_id is not None:
            rules[self.SPEC_ID] = self._spec_id
        if self._use_caching is not None:
            rules[self.USE_CACHING] = self._use_caching
        return rules

    def validate(self) -> None:
        super().validate()
        if self._manifest_count_warning <= 0:
            raise IllegalArgumentException("manifest_count_warning must be > 0")
        if self._manifest_count_critical < self._manifest_count_warning:
            raise IllegalArgumentException(
                "manifest_count_critical must be >= manifest_count_warning"
            )
        if self._avg_manifest_size_threshold_bytes <= 0:
            raise IllegalArgumentException(
                "avg_manifest_size_threshold_bytes must be > 0"
            )
        if self._spec_id is not None and self._spec_id < 0:
            raise IllegalArgumentException("spec_id must be >= 0")

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, IcebergRewriteManifestsContent):
            return False
        return (
            self._execution_fields_equal(other)
            and self._manifest_count_critical == other._manifest_count_critical
            and self._manifest_count_warning == other._manifest_count_warning
            and self._avg_manifest_size_threshold_bytes
            == other._avg_manifest_size_threshold_bytes
            and self._spec_id == other._spec_id
            and self._use_caching == other._use_caching
        )

    def __hash__(self) -> int:
        return hash(
            (
                self._execution_fields_hash_code(),
                self._manifest_count_critical,
                self._manifest_count_warning,
                self._avg_manifest_size_threshold_bytes,
                self._spec_id,
                self._use_caching,
            )
        )


class IcebergSnapshotExpirationContent(AbstractExecutablePolicyContent):
    """Content of an Iceberg snapshot-expiration policy."""

    STRATEGY_TYPE_KEY: ClassVar[str] = "strategy.type"
    JOB_TEMPLATE_NAME_KEY: ClassVar[str] = "job.template-name"
    TRIGGER_EXPR_KEY: ClassVar[str] = "trigger-expr"
    SCORE_EXPR_KEY: ClassVar[str] = "score-expr"
    JOB_OPTIONS_PREFIX: ClassVar[str] = "job.options."
    OLDER_THAN_DAYS_KEY: ClassVar[str] = "olderThanDays"
    RETAIN_LAST_KEY: ClassVar[str] = "retainLast"
    STREAM_RESULTS_KEY: ClassVar[str] = "streamResults"

    DEFAULT_OLDER_THAN_DAYS: ClassVar[int] = 5
    DEFAULT_RETAIN_LAST: ClassVar[int] = 1
    DEFAULT_STREAM_RESULTS: ClassVar[bool] = False
    MAX_OLDER_THAN_DAYS: ClassVar[int] = 36500

    STRATEGY_TYPE_VALUE: ClassVar[str] = "iceberg-snapshot-expiration"
    JOB_TEMPLATE_NAME_VALUE: ClassVar[str] = "builtin-iceberg-expire-snapshots"

    _SUPPORTED_OBJECT_TYPES: ClassVar[set] = {
        MetadataObject.Type.CATALOG,
        MetadataObject.Type.SCHEMA,
        MetadataObject.Type.TABLE,
    }

    def __init__(
        self,
        older_than_days: int | None = None,
        retain_last: int | None = None,
        stream_results: bool | None = None,
        execution_spec: PolicyExecutionSpec | None = None,
    ):
        super().__init__(execution_spec)
        self._older_than_days = (
            older_than_days
            if older_than_days is not None
            else self.DEFAULT_OLDER_THAN_DAYS
        )
        self._retain_last = (
            retain_last if retain_last is not None else self.DEFAULT_RETAIN_LAST
        )
        self._stream_results = (
            stream_results
            if stream_results is not None
            else self.DEFAULT_STREAM_RESULTS
        )

    def older_than_days(self) -> int:
        return self._older_than_days

    def retain_last(self) -> int:
        return self._retain_last

    def stream_results(self) -> bool:
        return self._stream_results

    def supported_object_types(self) -> set[MetadataObject.Type]:
        return set(self._SUPPORTED_OBJECT_TYPES)

    def properties(self) -> dict[str, str]:
        return {
            self.STRATEGY_TYPE_KEY: self.STRATEGY_TYPE_VALUE,
            self.JOB_TEMPLATE_NAME_KEY: self.JOB_TEMPLATE_NAME_VALUE,
        }

    def rules(self) -> dict[str, object]:
        return {
            self.TRIGGER_EXPR_KEY: "true",
            self.SCORE_EXPR_KEY: "1",
            self.JOB_OPTIONS_PREFIX + self.OLDER_THAN_DAYS_KEY: self._older_than_days,
            self.JOB_OPTIONS_PREFIX + self.RETAIN_LAST_KEY: self._retain_last,
            self.JOB_OPTIONS_PREFIX + self.STREAM_RESULTS_KEY: self._stream_results,
        }

    @staticmethod
    def validate_older_than_days(days: int) -> None:
        if not 1 <= days <= IcebergSnapshotExpirationContent.MAX_OLDER_THAN_DAYS:
            raise IllegalArgumentException(
                f"olderThanDays must be between 1 and "
                f"{IcebergSnapshotExpirationContent.MAX_OLDER_THAN_DAYS}"
            )

    @staticmethod
    def validate_retain_last(retain_last: int) -> None:
        if retain_last < 1:
            raise IllegalArgumentException("retainLast must be a positive long")

    def validate(self) -> None:
        super().validate()
        self.validate_older_than_days(self._older_than_days)
        self.validate_retain_last(self._retain_last)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, IcebergSnapshotExpirationContent):
            return False
        return (
            self._execution_fields_equal(other)
            and self._older_than_days == other._older_than_days
            and self._retain_last == other._retain_last
            and self._stream_results == other._stream_results
        )

    def __hash__(self) -> int:
        return hash(
            (
                self._execution_fields_hash_code(),
                self._older_than_days,
                self._retain_last,
                self._stream_results,
            )
        )
