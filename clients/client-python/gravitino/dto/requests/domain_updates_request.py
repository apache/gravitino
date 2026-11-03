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

from dataclasses_json import config, dataclass_json

from gravitino.api.domain.domain_change import DomainChange
from gravitino.dto.requests.domain_update_request import (
    DomainUpdateRequest,
    DomainUpdateRequestBase,
)
from gravitino.exceptions.base import IllegalArgumentException
from gravitino.rest.rest_message import RESTRequest
from gravitino.utils.precondition import Precondition


@dataclass_json
@dataclass
class DomainUpdatesRequest(RESTRequest):
    """A sequence of domain changes."""

    _updates: list[DomainUpdateRequestBase] = field(
        metadata=config(field_name="updates")
    )

    @staticmethod
    def from_changes(changes: tuple[DomainChange, ...]) -> "DomainUpdatesRequest":
        updates = []
        for change in changes:
            if isinstance(change, DomainChange.RenameDomain):
                updates.append(DomainUpdateRequest.RenameDomainRequest(change.new_name))
            elif isinstance(change, DomainChange.UpdateDomainComment):
                updates.append(
                    DomainUpdateRequest.UpdateDomainCommentRequest(change.new_comment)
                )
            elif isinstance(change, DomainChange.SetProperty):
                updates.append(
                    DomainUpdateRequest.SetDomainPropertyRequest(
                        change.name, change.value
                    )
                )
            elif isinstance(change, DomainChange.RemoveProperty):
                updates.append(
                    DomainUpdateRequest.RemoveDomainPropertyRequest(
                        change.removed_property
                    )
                )
            else:
                raise IllegalArgumentException(
                    f"Unsupported domain change: {type(change).__name__}"
                )
        return DomainUpdatesRequest(updates)

    def validate(self) -> None:
        Precondition.check_argument(
            self._updates is not None, "updates must not be None"
        )
        for update in self._updates:
            Precondition.check_argument(
                update is not None, "updates must not contain None"
            )
            update.validate()
