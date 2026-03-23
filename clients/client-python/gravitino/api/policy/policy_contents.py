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

import typing as tp

from gravitino.api.metadata_object import MetadataObject
from gravitino.api.policy.iceberg_data_compaction_content import (
    IcebergDataCompactionContent,
)
from gravitino.api.policy.policy_content import PolicyContent


class PolicyContents:
    """
    Utility class for creating instances of PolicyContent.
    """

    @staticmethod
    def custom(
        rules: dict[str, tp.Any],
        supported_object_types: set[MetadataObject.Type],
        properties: dict[str, str],
    ) -> PolicyContent:
        """
        Creates a custom policy content with the given rules and properties.

        Args:
            rules (dict[str, tp.Any]): The custom rules of the policy.
            supported_object_types (set[MetadataObject.Type]): The set of metadata object types
            that the policy can be applied to.
            properties (dict[str, str]): The additional properties of the policy.

        Returns:
            PolicyContent: A new instance of PolicyContent with the specified rules and properties.
        """
        return CustomContent(rules, supported_object_types, properties)

    @staticmethod
    def iceberg_data_compaction(
        min_data_file_mse: int = IcebergDataCompactionContent.DEFAULT_MIN_DATA_FILE_MSE,
        min_delete_file_number: int = IcebergDataCompactionContent.DEFAULT_MIN_DELETE_FILE_NUMBER,
        data_file_mse_weight=IcebergDataCompactionContent.DEFAULT_DATA_FILE_MSE_WEIGHT,
        delete_file_number_weight=IcebergDataCompactionContent.DEFAULT_DELETE_FILE_NUMBER_WEIGHT,
        max_partition_num=IcebergDataCompactionContent.DEFAULT_MAX_PARTITION_NUM,
        rewrite_options: tp.Mapping[str, str] | None = None,
    ) -> IcebergDataCompactionContent:
        if rewrite_options is None:
            rewrite_options = dict(IcebergDataCompactionContent.DEFAULT_REWRITE_OPTIONS)

        return IcebergDataCompactionContent(
            min_data_file_mse,
            min_delete_file_number,
            data_file_mse_weight,
            delete_file_number_weight,
            max_partition_num,
            rewrite_options,
        )


class CustomContent(PolicyContent):
    """
    A custom content implementation of PolicyContent that holds custom rules and properties.
    """

    def __init__(
        self,
        custom_rules: dict[str, tp.Any],
        supported_object_types: set[MetadataObject.Type],
        properties: dict[str, str],
    ) -> None:
        super().__init__()
        self._custom_rules = custom_rules
        self._supported_object_types = set(supported_object_types)
        self._properties = properties

    def __hash__(self):
        return hash(
            self._custom_rules,
            self._properties,
            self._supported_object_types,
        )

    def __eq__(self, other) -> bool:
        if not isinstance(other, CustomContent):
            return False
        return (
            self._custom_rules == other._custom_rules
            and self._properties == other._properties
            and self._supported_object_types == other._supported_object_types
        )

    def __str__(self) -> str:
        return (
            "CustomContent{"
            + f"customRules={self._custom_rules}"
            + f", properties={self._properties}"
            + f", supportedObjectTypes={self._supported_object_types}"
            + "}"
        )

    @property
    def custom_rules(self) -> dict[str, tp.Any]:
        """
        Get the custom rules of the policy content.

        Returns:
            dict[str, tp.Any]: The custom rules of the policy content.
        """
        return self._custom_rules

    def supported_object_types(self) -> set[MetadataObject.Type]:
        return self._supported_object_types

    def properties(self) -> dict[str, str]:
        return self._properties

    def rules(self) -> dict[str, tp.Any]:
        return self._custom_rules
