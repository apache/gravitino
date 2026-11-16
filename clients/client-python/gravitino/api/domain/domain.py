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

from abc import abstractmethod
from enum import Enum
from typing import Optional

from gravitino.api.audit import Audit
from gravitino.api.auditable import Auditable
from gravitino.api.domain.domain_member import DomainMember
from gravitino.api.domain.domain_member_filter import DomainMemberFilter
from gravitino.api.domain.domain_relation import DomainRelation
from gravitino.api.tag.tag_value import TagValue


class Domain(Auditable):
    """A business grouping backed by tags, with at most one level of children.

    Draft domains are visible only to managers; hidden drafts raise
    NoSuchDomainException. Mutations operate on the current server state by this
    snapshot's name and return a new snapshot. If that name is reused after a
    rename, operations target the replacement domain. Membership grants no data
    access; inherited tag assignments and visible subdomains determine scope.
    """

    class Status(Enum):
        """Visibility status of a domain."""

        DRAFT = "draft"
        PUBLISHED = "published"

    @abstractmethod
    def name(self) -> str:
        """Return the domain name."""
        raise NotImplementedError

    @abstractmethod
    def comment(self) -> Optional[str]:
        """Return the optional comment."""
        raise NotImplementedError

    @abstractmethod
    def properties(self) -> dict[str, str]:
        """Return a copy of the properties."""
        raise NotImplementedError

    @abstractmethod
    def status(self) -> Domain.Status:
        """Return the draft or published status."""
        raise NotImplementedError

    @abstractmethod
    def parent(self) -> Optional[str]:
        """Return the parent name, including when the parent is hidden."""
        raise NotImplementedError

    @abstractmethod
    def backing_tag(self) -> Optional[TagValue]:
        """Return the backing tag, or None when removed or deleted."""
        raise NotImplementedError

    @abstractmethod
    def audit_info(self) -> Audit:
        """Return the audit information."""
        raise NotImplementedError

    @abstractmethod
    def relations(self) -> list[DomainRelation]:
        """Return relations when listed for a leaf metadata object."""
        raise NotImplementedError

    @abstractmethod
    def publish(self) -> Domain:
        """Publish the current server state and return a new snapshot."""
        raise NotImplementedError

    @abstractmethod
    def unpublish(self) -> Domain:
        """Unpublish the current server state and return a new snapshot."""
        raise NotImplementedError

    @abstractmethod
    def attach_to_parent(self, parent: str) -> Domain:
        """Attach or move to a parent and return a new snapshot."""
        raise NotImplementedError

    @abstractmethod
    def detach_from_parent(self) -> Domain:
        """Detach from the current parent and return a new snapshot."""
        raise NotImplementedError

    @abstractmethod
    def update_backing_tag(self, backing_tag: Optional[TagValue]) -> Domain:
        """Bind an existing tag, or remove with None, returning a new snapshot."""
        raise NotImplementedError

    @abstractmethod
    def create_subdomain(
        self,
        name: str,
        comment: Optional[str],
        properties: Optional[dict[str, str]],
        backing_tag: TagValue,
        create_backing_tag: bool,
    ) -> Domain:
        """Create a draft subdomain with an explicit backing tag."""
        raise NotImplementedError

    @abstractmethod
    def list_subdomains(self) -> list[str]:
        """List visible child names."""
        raise NotImplementedError

    @abstractmethod
    def list_subdomains_info(self) -> list[Domain]:
        """List visible child snapshots."""
        raise NotImplementedError

    @abstractmethod
    def list_members(self, member_filter: DomainMemberFilter) -> list[DomainMember]:
        """List accessible leaf objects and their relations in the selected scope."""
        raise NotImplementedError
