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
from typing import List

from dataclasses_json import config

from gravitino.dto.requests.function_update_request import FunctionUpdateRequest
from gravitino.rest.rest_message import RESTRequest


@dataclass
class FunctionUpdatesRequest(RESTRequest):
    """Represents a request with multiple function updates."""

    _updates: List[FunctionUpdateRequest] = field(metadata=config(field_name="updates"))

    def __init__(self, updates: List[FunctionUpdateRequest]):
        self._updates = updates

    def validate(self):
        """Validates the request.

        Raises:
            IllegalArgumentException: If the request is invalid.
        """
        if self._updates is None:
            raise ValueError("Updates cannot be null")
        for update in self._updates:
            update.validate()

    def updates(self) -> List[FunctionUpdateRequest]:
        """Returns the list of updates."""
        return self._updates
