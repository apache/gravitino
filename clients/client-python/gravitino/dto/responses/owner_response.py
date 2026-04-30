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

from dataclasses_json import config

from gravitino.dto.authorization.owner_dto import OwnerDTO
from gravitino.dto.responses.base_response import BaseResponse
from gravitino.exceptions.base import IllegalArgumentException


@dataclass
class OwnerResponse(BaseResponse):
    """Represents a response for the get owner operation."""

    _owner: Optional[OwnerDTO] = field(
        default=None, metadata=config(field_name="owner")
    )

    def owner(self) -> Optional[OwnerDTO]:
        return self._owner

    def validate(self) -> None:
        super().validate()
        if self._owner is not None:
            if not self._owner.name():
                raise IllegalArgumentException("owner name must not be empty")
            if self._owner.type() is None:
                raise IllegalArgumentException("owner type must not be None")
