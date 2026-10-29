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
from typing import List

from dataclasses_json import config

from gravitino.dto.requests.ontology_update_request import OntologyUpdateRequestBase
from gravitino.rest.rest_message import RESTRequest
from gravitino.utils.precondition import Precondition


@dataclass
class OntologyUpdatesRequest(RESTRequest):
    """Represents a request to update an ontology."""

    _updates: list[OntologyUpdateRequestBase] = field(
        metadata=config(field_name="updates"), default_factory=list
    )

    def __init__(self, updates: List[OntologyUpdateRequestBase]) -> None:
        """
        Creates a new OntologyUpdatesRequest.

        Args:
            updates (List[OntologyUpdateRequestBase]): The updates to apply to
                the ontology.
        """
        self._updates = updates

    def validate(self) -> None:
        """Validates the request.

        Raises:
            IllegalArgumentException: If the request has no update or an update
                is invalid.
        """
        Precondition.check_argument(
            self._updates is not None and len(self._updates) > 0,
            "updates must not be empty",
        )
        for update_request in self._updates:
            Precondition.check_argument(
                update_request is not None, "updates must not contain null"
            )
            update_request.validate()
