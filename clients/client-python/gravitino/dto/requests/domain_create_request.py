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

from gravitino.dto.domain_dto import DomainBackingTagDTO
from gravitino.rest.rest_message import RESTRequest
from gravitino.rest.rest_utils import validate_domain_name
from gravitino.utils.precondition import Precondition


@dataclass_json
@dataclass
class BackingTagCreate(DomainBackingTagDTO):
    """Explicitly create a new valueless tag or bind an existing tag."""

    _create: bool = field(default=False, metadata=config(field_name="create"))

    def validate(self) -> None:
        tag = self.tag_value()
        Precondition.check_argument(
            not self._create or tag.value() is None,
            "A new backing tag cannot have a value",
        )


@dataclass_json
@dataclass
class DomainCreateRequest(RESTRequest):
    """Create a domain and optionally its backing tag atomically."""

    _name: str = field(metadata=config(field_name="name"))
    _comment: Optional[str] = field(metadata=config(field_name="comment"))
    _properties: Optional[dict[str, str]] = field(
        metadata=config(field_name="properties")
    )
    _parent: Optional[str] = field(metadata=config(field_name="parent"))
    _backing_tag: BackingTagCreate = field(metadata=config(field_name="backingTag"))

    def validate(self) -> None:
        validate_domain_name(self._name)
        Precondition.check_argument(
            self._parent is None
            or (isinstance(self._parent, str) and bool(self._parent.strip())),
            "parent must not be blank",
        )
        Precondition.check_argument(
            self._backing_tag is not None, "backing_tag must not be None"
        )
        if self._properties is not None:
            Precondition.check_argument(
                all(
                    k is not None and v is not None for k, v in self._properties.items()
                ),
                "properties must not contain None keys or values",
            )
        self._backing_tag.validate()
