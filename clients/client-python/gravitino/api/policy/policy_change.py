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

from abc import ABC
from dataclasses import dataclass

from gravitino.api.policy.policy_content import PolicyContent


class PolicyChange(ABC):
    """
    A policy change is a change to a policy. It can be a rename, a comment
    update, or a content update. A policy change is used to modify a policy
    in the metalake, together with other policy changes in a single request.
    """

    @staticmethod
    def rename(new_name: str) -> PolicyChange:
        """Creates a new policy change to rename a policy.

        Args:
            new_name: The new name of the policy.

        Returns:
            PolicyChange: The policy change.
        """
        return PolicyChange.RenamePolicy(new_name)

    @staticmethod
    def update_comment(new_comment: str) -> PolicyChange:
        """Creates a new policy change to update the comment of a policy.

        Args:
            new_comment: The new comment of the policy.

        Returns:
            PolicyChange: The policy change.
        """
        return PolicyChange.UpdatePolicyComment(new_comment)

    @staticmethod
    def update_content(policy_type: str, content: PolicyContent) -> PolicyChange:
        """Creates a new policy change to update the content of a policy.

        Args:
            policy_type: The type of the policy.
            content: The new content of the policy.

        Returns:
            PolicyChange: The policy change.
        """
        return PolicyChange.UpdateContent(policy_type, content)

    @dataclass(frozen=True, eq=True)
    class RenamePolicy:
        """A policy change to rename a policy."""

        _new_name: str

        @property
        def new_name(self) -> str:
            return self._new_name

        def __str__(self) -> str:
            return f"RENAME POLICY {self._new_name}"

    @dataclass(frozen=True, eq=True)
    class UpdatePolicyComment:
        """A policy change to update the comment of a policy."""

        _new_comment: str

        @property
        def new_comment(self) -> str:
            return self._new_comment

        def __str__(self) -> str:
            return f"UPDATE POLICY COMMENT {self._new_comment}"

    @dataclass(frozen=True, eq=True)
    class UpdateContent:
        """A policy change to update the type and content of a policy."""

        _policy_type: str
        _content: PolicyContent

        @property
        def policy_type(self) -> str:
            return self._policy_type

        @property
        def content(self) -> PolicyContent:
            return self._content

        def __str__(self) -> str:
            return (
                f"UPDATE POLICY CONTENT policyType={self._policy_type}, "
                f"content={self._content}"
            )
