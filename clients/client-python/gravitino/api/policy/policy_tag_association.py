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

from abc import ABC, abstractmethod

from gravitino.api.policy.policy import Policy
from gravitino.api.policy.policy_association_selector import PolicyAssociationSelector
from gravitino.api.tag.tag import Tag


class PolicyTagAssociation(ABC):
    """An association between a policy and a tag, together with the selector
    that scopes which tag assignments the association applies to."""

    @abstractmethod
    def policy(self) -> Policy:
        """Returns the associated policy."""
        raise NotImplementedError()

    @abstractmethod
    def tag(self) -> Tag:
        """Returns the associated tag."""
        raise NotImplementedError()

    @abstractmethod
    def selector(self) -> PolicyAssociationSelector:
        """Returns the selector scoping the association."""
        raise NotImplementedError()
