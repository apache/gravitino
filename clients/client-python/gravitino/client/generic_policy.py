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

from typing import Optional

from gravitino.api.policy.policy import Policy
from gravitino.api.policy.policy_content import PolicyContent
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.policy_dto import PolicyDTO


class GenericPolicy(Policy):
    """Represents a generic policy, wrapping a PolicyDTO and exposing its
    content as the domain policy content."""

    def __init__(self, policy_dto: PolicyDTO) -> None:
        self._policy_dto = policy_dto
        content_dto = policy_dto.content()
        self._content = (
            content_dto.to_domain_content() if content_dto is not None else None
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, GenericPolicy):
            return False

        return self._policy_dto == other._policy_dto

    def __hash__(self) -> int:
        return hash(self._policy_dto)

    def name(self) -> str:
        """Get the name of the policy.

        Returns:
            str: The name of the policy.
        """
        return self._policy_dto.name()

    def policy_type(self) -> str:
        """Get the type of the policy.

        Returns:
            str: The type of the policy.
        """
        return self._policy_dto.policy_type()

    def comment(self) -> Optional[str]:
        """Get the comment of the policy.

        Returns:
            Optional[str]: The comment of the policy.
        """
        return self._policy_dto.comment()

    def enabled(self) -> bool:
        """Check whether the policy is enabled.

        Returns:
            bool: True if the policy is enabled, False otherwise.
        """
        return self._policy_dto.enabled()

    def content(self) -> PolicyContent:
        """Get the content of the policy, converted to the domain policy
        content type determined by the policy type.

        Returns:
            PolicyContent: The content of the policy.
        """
        return self._content

    def inherited(self) -> Optional[bool]:
        """Check if the policy is inherited from a parent object or not.

        Returns:
            Optional[bool]: True if the policy is inherited, False if it is
                owned by the object itself, None if the policy is not
                associated with any object.
        """
        return self._policy_dto.inherited()

    def audit_info(self) -> AuditDTO:
        """Retrieve the audit information of the entity.

        Returns:
            AuditDTO: The audit information of the entity.
        """
        return self._policy_dto.audit_info()
