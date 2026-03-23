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
from dataclasses import dataclass, field

from dataclasses_json import config

from gravitino.api.policy.policy_content import PolicyContent


class PolicyChange:
    """
    Interface for supporting policy changes.
    This interface will be used to provide policy modification operations for each policy.
    """

    @staticmethod
    def rename(new_name: str) -> PolicyChange:
        """
        Creates a new policy change to rename the policy.

        Args:
            new_name (str): The new name for the policy.

        Returns:
            PolicyChange: The policy change.
        """
        return RenamePolicy(new_name)

    @staticmethod
    def update_comment(new_comment: str) -> PolicyChange:
        """
        Creates a new policy change to update the policy comment.

        Args:
            new_comment (str): The new comment for the policy.

        Returns:
            PolicyChange: The policy change.
        """
        return UpdatePolicyComment(new_comment)

    @staticmethod
    def update_content(policy_type: str, new_content: PolicyContent) -> PolicyChange:
        """
        Creates a new policy change to update the content of the policy.

        Args:
            policy_type (str): The type of the policy, used for validation.
            new_content (PolicyContent): The new content for the policy.

        Returns:
            UpdateContent: The policy change.
        """
        return UpdateContent(policy_type, new_content)


@tp.final
@dataclass(frozen=True)
class RenamePolicy(PolicyChange):
    _new_name: str = field(metadata=config(field_name="newName"))

    def __str__(self) -> str:
        return f"RENAME POLICY {self._new_name}"

    @property
    def new_name(self) -> str:
        return self._new_name


@tp.final
@dataclass(frozen=True)
class UpdatePolicyComment(PolicyChange):
    _new_comment: str = field(metadata=config(field_name="newComment"))

    def __str__(self) -> str:
        return f"UPDATE POLICY COMMENT {self._new_comment}"

    @property
    def new_comment(self) -> str:
        return self._new_comment


@tp.final
@dataclass(frozen=True)
class UpdateContent(PolicyChange):
    _policy_type: str = field(metadata=config(field_name="policyType"))
    _new_content: PolicyContent = field(metadata=config(field_name="newContent"))

    def __str__(self) -> str:
        return (
            "UPDATE POLICY CONTENT "
            + "policyType="
            + f"{self._policy_type}"
            + ", content="
            + f"{self._new_content}"
        )

    @property
    def new_content(self) -> PolicyContent:
        return self._new_content

    @property
    def policy_type(self) -> str:
        return self._policy_type
