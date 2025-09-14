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

from typing import Any, Optional

from gravitino.api.metadata_object import MetadataObject
from gravitino.api.policy.policy_content import PolicyContent


class PolicyContents:
    """Utility class for creating instances of `PolicyContent`"""

    @staticmethod
    def custom(
        rules: dict[str, Any],
        supported_object_types: set[MetadataObject.Type],
        properties: dict[str, str],
    ) -> PolicyContent:
        """Creates a custom policy content with the given rules and properties.

        Args:
            rules: The custom rules of the policy.
            supported_object_types: The set of metadata object types that the policy can be applied to.
            properties: The additional properties of the policy.

        Returns:
            A new instance of `PolicyContent` with the specified rules and properties.
        """
        return PolicyContents.CustomContent(rules, supported_object_types, properties)

    class CustomContent(PolicyContent):
        """A custom content implementation of `PolicyContent` that holds custom rules and properties."""

        def __init__(
            self,
            custom_rules: Optional[dict[str, Any]] = None,
            supported_object_types: Optional[set[MetadataObject.Type]] = None,
            properties: Optional[dict[str, str]] = None,
        ):
            """Constructor for `CustomContent`.

            Args:
                custom_rules: the custom rules of the policy
                supported_object_types: the set of metadata object types that the policy can be applied to
                properties: the additional properties of the policy
            """
            self._custom_rules = custom_rules
            self._supported_object_types = frozenset(supported_object_types or set())
            self._properties = properties

        def custom_rules(self) -> Optional[dict[str, Any]]:
            """Returns the custom rules of the policy.

            Returns:
                Optional[dict[str, Any]]: a map of custom rules.
            """
            return self._custom_rules

        def supported_object_types(self) -> set[MetadataObject.Type]:
            return set(self._supported_object_types)

        def properties(self) -> Optional[dict[str, str]]:
            return self._properties

        def __eq__(self, other) -> bool:
            if not isinstance(other, PolicyContents.CustomContent):
                return False
            return (
                self._custom_rules == other.custom_rules()
                and self._properties == other.properties()
                and self._supported_object_types == other.supported_object_types()
            )

        def __hash__(self) -> int:
            return hash(
                (
                    (
                        tuple(sorted(self._custom_rules.items()))
                        if self._custom_rules
                        else None
                    ),
                    (
                        tuple(sorted(self._properties.items()))
                        if self._properties
                        else None
                    ),
                    self._supported_object_types,
                )
            )

        def __str__(self) -> str:
            return (
                f"CustomContent("
                f"custom_rules={self._custom_rules}, "
                f"properties={self._properties}, "
                f"supported_object_types={set(self._supported_object_types)}"
                f")"
            )
