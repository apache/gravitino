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

from dataclasses import dataclass
from collections.abc import Sequence
from typing import Optional

from gravitino.api.domain.domain_member import DomainMember
from gravitino.api.metadata_object import MetadataObject
from gravitino.utils.precondition import Precondition


@dataclass(frozen=True)
class DomainMemberFilter:
    """Narrow a domain's scope by types, case-insensitive name text and scopes.

    Empty options do not narrow the result. Domains select the union of the named
    scopes: the listed domain itself (including visible children), or its children.
    Unknown or invisible scope names are ignored by the server.
    """

    types: tuple[MetadataObject.Type, ...] = ()
    name_contains: Optional[str] = None
    domains: Sequence[str] = ()

    def _criteria(self) -> tuple:
        return frozenset(self.types), self.name_contains, frozenset(self.domains)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, DomainMemberFilter):
            return NotImplemented
        return self._criteria() == other._criteria()

    def __hash__(self) -> int:
        return hash(self._criteria())

    def __post_init__(self) -> None:
        Precondition.check_argument(self.types is not None, "types must not be None")
        Precondition.check_argument(
            self.domains is not None, "domains must not be None"
        )
        object.__setattr__(self, "types", tuple(dict.fromkeys(self.types)))
        domains = (self.domains,) if isinstance(self.domains, str) else self.domains
        object.__setattr__(self, "domains", tuple(dict.fromkeys(domains)))
        Precondition.check_argument(
            all(t in DomainMember.SUPPORTED_TYPES for t in self.types),
            "Only leaf metadata types can be domain members",
        )
        Precondition.check_argument(
            self.name_contains is None
            or (
                isinstance(self.name_contains, str) and bool(self.name_contains.strip())
            ),
            "name_contains must not be blank",
        )
        Precondition.check_argument(
            all(isinstance(d, str) and bool(d.strip()) for d in self.domains),
            "domains must not contain blank names",
        )
