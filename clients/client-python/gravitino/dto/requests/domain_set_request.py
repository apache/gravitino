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

from gravitino.api.domain.domain import Domain
from gravitino.dto.domain_dto import DomainBackingTagDTO
from gravitino.rest.rest_message import RESTRequest
from gravitino.utils.precondition import Precondition


@dataclass_json
@dataclass
class DomainSetRequest(RESTRequest):
    """Set domain visibility."""

    _status: Domain.Status = field(metadata=config(field_name="status"))

    def validate(self) -> None:
        Precondition.check_argument(
            isinstance(self._status, Domain.Status), "status must not be None"
        )


@dataclass_json
@dataclass
class DomainParentSetRequest(RESTRequest):
    """Attach or move a domain."""

    _parent: str = field(metadata=config(field_name="parent"))

    def validate(self) -> None:
        Precondition.check_argument(
            isinstance(self._parent, str) and bool(self._parent.strip()),
            "parent must not be blank",
        )


@dataclass_json
@dataclass
class DomainBackingTagSetRequest(RESTRequest):
    """Replace or remove the backing tag."""

    _backing_tag: Optional[DomainBackingTagDTO] = field(
        metadata=config(field_name="backingTag")
    )

    def validate(self) -> None:
        if self._backing_tag is not None:
            self._backing_tag.tag_value()
