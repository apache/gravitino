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

from dataclasses_json import config
from gravitino.exceptions.base import IllegalArgumentException

from .base_response import BaseResponse
from ..job.job_dto import JobDTO


@dataclass
class JobResponse(BaseResponse):
    """Represents a response containing a single job."""

    _job: JobDTO = field(metadata=config(field_name="job"))

    def validate(self):
        """Validates the response data and contained job."""
        super().validate()

        if self._job is None:
            raise IllegalArgumentException('"job" must not be None')

        self._job.validate()

    def job(self) -> JobDTO:
        """Returns the job from the response."""
        return self._job
