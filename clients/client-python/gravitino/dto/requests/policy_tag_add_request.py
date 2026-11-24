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

from dataclasses import dataclass, field

from dataclasses_json import config, dataclass_json

from gravitino.api.policy.policy_association_selector import PolicyAssociationSelector
from gravitino.dto.policy_dto import PolicyAssociationSelectorDTO
from gravitino.rest.rest_message import RESTRequest
from gravitino.utils.precondition import Precondition


@dataclass_json
@dataclass
class PolicyTagAddRequest(RESTRequest):
    """Represents a request to associate a policy with a tag."""

    _selector: PolicyAssociationSelectorDTO | None = field(
        default=None, metadata=config(field_name="selector")
    )

    def validate(self) -> None:
        """Validates the request.

        Raises:
            IllegalArgumentException: If the request is invalid.
        """
        Precondition.check_argument(
            self._selector is not None, "selector must not be null"
        )
        self._selector.validate()

    def selector(self) -> PolicyAssociationSelector:
        """Returns the selector as an API selector."""
        return self._selector.to_selector() if self._selector is not None else None

    @property
    def selector_dto(self) -> PolicyAssociationSelectorDTO | None:
        return self._selector
