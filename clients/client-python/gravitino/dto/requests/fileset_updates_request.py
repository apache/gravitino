"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""

from dataclasses import dataclass, field
from typing import List

from dataclasses_json import config

from gravitino.dto.requests.fileset_update_request import FilesetUpdateRequest
from gravitino.rest.rest_message import RESTRequest


@dataclass
class FilesetUpdatesRequest(RESTRequest):
    """Request to represent updates to a fileset."""

    _updates: List[FilesetUpdateRequest] = field(
        metadata=config(field_name="updates"), default_factory=list
    )

    def validate(self):
        if not self._updates:
            raise ValueError("Updates cannot be empty")
        for update_request in self._updates:
            update_request.validate()
