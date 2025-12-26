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
from dataclasses import field, dataclass
from typing import List

from dataclasses_json import config

from gravitino.dto.requests.job_template_update_request import JobTemplateUpdateRequest
from gravitino.rest.rest_message import RESTRequest


@dataclass
class JobTemplateUpdatesRequest(RESTRequest):
    """Represents a request to get job template updates."""

    _updates: List[JobTemplateUpdateRequest] = field(
        metadata=config(field_name="updates"), default_factory=list
    )

    def validate(self):
        if not self._updates:
            raise ValueError("Updates cannot be empty")
        for update_request in self._updates:
            update_request.validate()
