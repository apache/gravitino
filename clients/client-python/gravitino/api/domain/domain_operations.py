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

from abc import ABC, abstractmethod
from typing import Optional

from gravitino.api.domain.domain import Domain
from gravitino.api.domain.domain_change import DomainChange
from gravitino.api.tag.tag_value import TagValue


class DomainOperations(ABC):
    """Metalake domain management. Hidden drafts are omitted or return 404."""

    @abstractmethod
    def list_domains(self) -> list[str]:
        """List visible domain names, including subdomains."""
        raise NotImplementedError

    @abstractmethod
    def list_domains_info(self) -> list[Domain]:
        """List visible domain snapshots, including subdomains."""
        raise NotImplementedError

    @abstractmethod
    def get_domain(self, name: str) -> Domain:
        """Load by name; absent or hidden drafts raise NoSuchDomainException."""
        raise NotImplementedError

    @abstractmethod
    def create_domain(
        self,
        name: str,
        comment: Optional[str],
        properties: Optional[dict[str, str]],
        backing_tag: TagValue,
        create_backing_tag: bool,
    ) -> Domain:
        """Create a draft top-level domain, explicitly creating or binding its tag.

        A new tag cannot have a value. Existing tags are never adopted implicitly.
        Domain and new tag creation are atomic. Conflicts raise
        DomainAlreadyExistsException, TagAlreadyExistsException or
        BackingTagAlreadyBoundException; missing tags raise NoSuchTagException.
        """
        raise NotImplementedError

    @abstractmethod
    def alter_domain(self, name: str, *changes: DomainChange) -> Domain:
        """Alter name, comment or properties without renaming the backing tag."""
        raise NotImplementedError

    @abstractmethod
    def delete_domain(self, name: str) -> bool:
        """Delete, keeping tags; return False if absent, reject nonempty parents."""
        raise NotImplementedError
