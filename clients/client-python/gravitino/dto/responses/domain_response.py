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

from dataclasses import dataclass, field
from typing import Optional

from dataclasses_json import config, dataclass_json

from gravitino.dto.domain_dto import DomainDTO, DomainMemberDTO
from gravitino.dto.responses.base_response import BaseResponse
from gravitino.exceptions.base import IllegalArgumentException
from gravitino.utils.precondition import Precondition


def _decode_domain_names(value: object) -> Optional[list[str]]:
    if value is None:
        return None
    if not isinstance(value, list) or not all(isinstance(name, str) for name in value):
        raise IllegalArgumentException("names must be an array of strings")
    return value


@dataclass_json
@dataclass
class DomainNamesListResponse(BaseResponse):
    """Visible domain names."""

    _names: Optional[list[str]] = field(
        default=None, metadata=config(field_name="names", decoder=_decode_domain_names)
    )

    def names(self) -> list[str]:
        return self._names

    def validate(self) -> None:
        super().validate()
        Precondition.check_argument(
            self._names is not None, "Domain names must not be None"
        )
        for name in self._names:
            Precondition.check_argument(
                isinstance(name, str) and bool(name.strip()),
                "Domain name must not be blank",
            )


@dataclass_json
@dataclass
class DomainListResponse(BaseResponse):
    """Visible domain snapshots."""

    _domains: Optional[list[DomainDTO]] = field(
        default=None, metadata=config(field_name="domains")
    )

    def domains(self) -> list[DomainDTO]:
        return self._domains

    def validate(self) -> None:
        super().validate()
        Precondition.check_argument(
            self._domains is not None, "Domains must not be None"
        )
        for domain in self._domains:
            domain.validate()


@dataclass_json
@dataclass
class DomainResponse(BaseResponse):
    """A domain snapshot response."""

    _domain: Optional[DomainDTO] = field(
        default=None, metadata=config(field_name="domain")
    )

    def domain(self) -> DomainDTO:
        return self._domain

    def validate(self) -> None:
        super().validate()
        Precondition.check_argument(
            self._domain is not None, "Domain response must have a domain"
        )
        self._domain.validate()


@dataclass_json
@dataclass
class DomainMemberListResponse(BaseResponse):
    """Accessible members and their relations."""

    _members: Optional[list[DomainMemberDTO]] = field(
        default=None, metadata=config(field_name="members")
    )

    def members(self) -> list[DomainMemberDTO]:
        return self._members

    def validate(self) -> None:
        super().validate()
        Precondition.check_argument(
            self._members is not None, "Members must not be None"
        )
