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

from gravitino.api.policy.policy import Policy
from gravitino.api.policy.policy_association_selector import PolicyAssociationSelector
from gravitino.api.policy.policy_tag_association import PolicyTagAssociation
from gravitino.api.tag.tag import Tag


class GenericPolicyTagAssociation(PolicyTagAssociation):
    """Represents a generic association between a policy and a tag."""

    def __init__(
        self,
        policy: Policy,
        tag: Tag,
        selector: PolicyAssociationSelector,
    ) -> None:
        self._policy = policy
        self._tag = tag
        self._selector = selector

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, GenericPolicyTagAssociation):
            return False

        return (
            self._policy == other._policy
            and self._tag == other._tag
            and self._selector == other._selector
        )

    def __hash__(self) -> int:
        return hash((self._policy, self._tag, self._selector))

    def policy(self) -> Policy:
        """Get the associated policy.

        Returns:
            Policy: The associated policy.
        """
        return self._policy

    def tag(self) -> Tag:
        """Get the associated tag.

        Returns:
            Tag: The associated tag.
        """
        return self._tag

    def selector(self) -> PolicyAssociationSelector:
        """Get the selector scoping the association.

        Returns:
            PolicyAssociationSelector: The selector of the association.
        """
        return self._selector
