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

import re
from types import MappingProxyType
from typing import Any, Final, Mapping

from gravitino.api.metadata_object import MetadataObject
from gravitino.api.policy.policy_content import PolicyContent
from gravitino.utils import StringUtils
from gravitino.utils.precondition import Precondition


class IcebergDataCompactionContent(PolicyContent):
    """
    Built-in policy content for Iceberg compaction strategy.
    """

    # Property key for strategy type.
    STRATEGY_TYPE_KEY: Final[str] = "strategy.type"

    # Strategy type value for iceberg data compaction.
    STRATEGY_TYPE_VALUE: Final[str] = "iceberg-data-compaction"

    # Property key for job template name.
    JOB_TEMPLATE_NAME_KEY: Final[str] = "job.template-name"

    # Built-in job template name for Iceberg rewrite data files.
    JOB_TEMPLATE_NAME_VALUE: Final[str] = "builtin-iceberg-rewrite-data-files"

    # Prefix for rewrite options propagated to job options.
    JOB_OPTIONS_PREFIX: Final[str] = "job.options."

    # Rule key for trigger expression.
    TRIGGER_EXPR_KEY: Final[str] = "trigger-expr"

    # Rule key for score expression.
    SCORE_EXPR_KEY: Final[str] = "score-expr"

    # Rule key for minimum data file MSE threshold.
    MIN_DATA_FILE_MSE_KEY: Final[str] = "minDataFileMse"

    # Rule key for minimum delete file count threshold.
    MIN_DELETE_FILE_NUMBER_KEY: Final[str] = "minDeleteFileNumber"

    # Rule key for data file MSE score weight.
    DATA_FILE_MSE_WEIGHT_KEY: Final[str] = "dataFileMseWeight"

    # Rule key for delete file number score weight.
    DELETE_FILE_NUMBER_WEIGHT_KEY: Final[str] = "deleteFileNumberWeight"

    # Rule key for max partition number selected for compaction.
    MAX_PARTITION_NUM_KEY: Final[str] = "max-partition-num"

    # Metric name for data file MSE.
    DATA_FILE_MSE_METRIC: Final[str] = "custom-data-file-mse"

    # Metric name for delete file number.
    DELETE_FILE_NUMBER_METRIC: Final[str] = "custom-delete-file-number"

    # Default minimum threshold for data file MSE metric, equals (128 MiB * 0.15)^2.
    DEFAULT_MIN_DATA_FILE_MSE: Final[int] = 405323966463344

    # Default minimum threshold for delete file number metric.
    DEFAULT_MIN_DELETE_FILE_NUMBER: Final[int] = 1

    # Default score weight for data file MSE.
    DEFAULT_DATA_FILE_MSE_WEIGHT: Final[int] = 1

    # Default score weight for delete file number.
    DEFAULT_DELETE_FILE_NUMBER_WEIGHT: Final[int] = 100

    # Default max partition number for compaction.
    DEFAULT_MAX_PARTITION_NUM: Final[int] = 50

    # Default rewrite options for Iceberg rewrite data files.
    DEFAULT_REWRITE_OPTIONS: Mapping[str, str] = MappingProxyType({})

    OPTION_KEY_PATTERN: Final[re.Pattern[str]] = re.compile(r"[A-Za-z0-9._-]+")

    SUPPORTED_OBJECT_TYPES: set[MetadataObject.Type] = {
        MetadataObject.Type.CATALOG,
        MetadataObject.Type.SCHEMA,
        MetadataObject.Type.TABLE,
    }

    TRIGGER_EXPR: Final[str] = (
        f"{DATA_FILE_MSE_METRIC} >= {MIN_DATA_FILE_MSE_KEY} "
        f"|| {DELETE_FILE_NUMBER_METRIC} >= {MIN_DELETE_FILE_NUMBER_KEY}"
    )

    SCORE_EXPR: Final[str] = (
        f"{DATA_FILE_MSE_METRIC} * {DATA_FILE_MSE_WEIGHT_KEY} "
        f"+ {DELETE_FILE_NUMBER_METRIC} * {DELETE_FILE_NUMBER_WEIGHT_KEY}"
    )

    def __init__(
        self,
        min_data_file_mse: int,
        min_delete_file_number: int,
        data_file_mse_weight: int,
        delete_file_number_weight: int,
        max_partition_num: int,
        rewrite_options: dict[str, str],
    ) -> None:
        super().__init__()

        self._min_data_file_mse = (
            min_data_file_mse
            if min_data_file_mse is not None
            else IcebergDataCompactionContent.DEFAULT_MIN_DATA_FILE_MSE
        )
        self._min_delete_file_number = (
            min_delete_file_number
            if min_delete_file_number is not None
            else IcebergDataCompactionContent.DEFAULT_MIN_DELETE_FILE_NUMBER
        )
        self._data_file_mse_weight = (
            data_file_mse_weight
            if data_file_mse_weight is not None
            else IcebergDataCompactionContent.DEFAULT_DATA_FILE_MSE_WEIGHT
        )
        self._delete_file_number_weight = (
            delete_file_number_weight
            if delete_file_number_weight is not None
            else IcebergDataCompactionContent.DEFAULT_DELETE_FILE_NUMBER_WEIGHT
        )
        self._max_partition_num = (
            max_partition_num
            if max_partition_num is not None
            else IcebergDataCompactionContent.DEFAULT_MAX_PARTITION_NUM
        )
        self._rewrite_options = (
            MappingProxyType(rewrite_options)
            if rewrite_options is not None
            else IcebergDataCompactionContent.DEFAULT_REWRITE_OPTIONS
        )

    def __eq__(self, value) -> bool:
        if not isinstance(value, IcebergDataCompactionContent):
            return False

        return (
            self.min_data_file_mse == value.min_data_file_mse
            and self.min_delete_file_number == value.min_delete_file_number
            and self.data_file_mse_weight == value.data_file_mse_weight
            and self.delete_file_number_weight == value.delete_file_number_weight
            and self.max_partition_num == value.max_partition_num
            and self.rewrite_options == value.rewrite_options
        )

    def __hash__(self):
        return hash(
            self._min_data_file_mse,
            self._min_delete_file_number,
            self._data_file_mse_weight,
            self._delete_file_number_weight,
            self._max_partition_num,
            tuple(sorted(self._rewrite_options.items())),
        )

    def __str__(self) -> str:
        return (
            "IcebergDataCompactionContent("
            f"min_data_file_mse={self.min_data_file_mse}, "
            f"min_delete_file_number={self.min_delete_file_number}, "
            f"data_file_mse_weight={self.data_file_mse_weight}, "
            f"delete_file_number_weight={self.delete_file_number_weight}, "
            f"max_partition_num={self.max_partition_num}, "
            f"rewrite_options={dict(self.rewrite_options)}"
            ")"
        )

    @property
    def min_data_file_mse(self) -> int:
        """
        Returns the minimum threshold for "custom-data-file-mse".

        Returns:
            int: minimum data file MSE threshold
        """
        return self._min_data_file_mse

    @property
    def min_delete_file_number(self) -> int:
        """
        Returns the minimum threshold for "custom-delete-file-number".

        Returns:
            int: minimum delete file number threshold
        """
        return self._min_delete_file_number

    @property
    def data_file_mse_weight(self) -> int:
        """
        Returns the weight used by "custom-data-file-mse" in score expression.

        Returns:
            int: data file MSE score weight
        """
        return self._data_file_mse_weight

    @property
    def delete_file_number_weight(self) -> int:
        """
        Returns the weight used by "custom-delete-file-number" in score expression.

        Returns:
            int: delete file number score weight
        """
        return self._delete_file_number_weight

    @property
    def max_partition_num(self) -> int:
        """
        Returns max partition number selected for compaction.

        Returns:
            int: max partition number
        """
        return self._max_partition_num

    @property
    def rewrite_options(self) -> Mapping[str, str]:
        """
        Returns rewrite options that are expanded to job.options.* rule entries.

        Returns:
            Mapping[str, str]: The rewrite options
        """
        return self._rewrite_options

    def supported_object_types(self) -> set[MetadataObject.Type]:
        return IcebergDataCompactionContent.SUPPORTED_OBJECT_TYPES

    def properties(self) -> dict[str, str]:
        # properties keep stable strategy/job identity; thresholds and scoring knobs belong to rules.
        return {
            IcebergDataCompactionContent.STRATEGY_TYPE_KEY: IcebergDataCompactionContent.STRATEGY_TYPE_VALUE,
            IcebergDataCompactionContent.JOB_TEMPLATE_NAME_KEY: IcebergDataCompactionContent.JOB_TEMPLATE_NAME_VALUE,
        }

    def rules(self) -> dict[str, Any]:
        rewrite_rules: dict[str, Any] = dict(self._rewrite_options.items())

        return {
            IcebergDataCompactionContent.MIN_DATA_FILE_MSE_KEY: self.min_data_file_mse,
            IcebergDataCompactionContent.MIN_DELETE_FILE_NUMBER_KEY: self.min_delete_file_number,
            IcebergDataCompactionContent.DATA_FILE_MSE_WEIGHT_KEY: self.data_file_mse_weight,
            IcebergDataCompactionContent.DELETE_FILE_NUMBER_WEIGHT_KEY: self.delete_file_number_weight,
            IcebergDataCompactionContent.MAX_PARTITION_NUM_KEY: self.max_partition_num,
            IcebergDataCompactionContent.TRIGGER_EXPR_KEY: self.TRIGGER_EXPR,
            IcebergDataCompactionContent.SCORE_EXPR_KEY: self.SCORE_EXPR,
            **rewrite_rules,
        }

    def validate(self) -> None:
        Precondition.check_argument(
            self.min_data_file_mse >= 0,
            "minDataFileMse must be >= 0",
        )
        Precondition.check_argument(
            self.min_delete_file_number >= 0,
            "minDeleteFileNumber must be >= 0",
        )
        Precondition.check_argument(
            self.data_file_mse_weight >= 0,
            "dataFileMseWeight must be >= 0",
        )
        Precondition.check_argument(
            self._max_partition_num > 0,
            "maxPartitionNum must be > 0",
        )

        for k, v in self._rewrite_options.items():
            Precondition.check_argument(
                StringUtils.is_not_blank(k),
                "rewrite option key is blank",
            )
            Precondition.check_argument(
                IcebergDataCompactionContent.OPTION_KEY_PATTERN.fullmatch(k),
                f"rewrite option key '{k}' contains illegal characters",
            )
            Precondition.check_argument(
                not k.startswith(IcebergDataCompactionContent.JOB_OPTIONS_PREFIX),
                f"rewrite option key '{k}' must not start with {IcebergDataCompactionContent.JOB_OPTIONS_PREFIX}",
            )
            Precondition.check_argument(
                StringUtils.is_not_blank(v),
                f"rewrite option '{k}' must have non-empty value",
            )
