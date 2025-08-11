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
from gravitino.exceptions.base import IllegalArgumentException

from .base_response import BaseResponse
from ..job.job_dto import JobDTO


@dataclass
class JobListResponse(BaseResponse):
    """Represents a response containing a list of jobs."""

    _jobs: List[JobDTO] = field(metadata=config(field_name="jobs"))

    def validate(self):
        """Validates the response data and contained jobs."""
        super().validate()

        if self._jobs is None:
            raise IllegalArgumentException("jobs must not be None")

        for job in self._jobs:
            if job is not None:
                job.validate()

    def jobs(self) -> List[JobDTO]:
        """Returns the list of jobs from the response."""
        return self._jobs
