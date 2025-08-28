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
import json
from dataclasses import dataclass, field

from dataclasses_json import config
from gravitino.exceptions.base import IllegalArgumentException

from .base_response import BaseResponse
from ..job.job_template_dto import JobTemplateDTO


@dataclass
class JobTemplateResponse(BaseResponse):
    """Represents a response containing a single job template."""

    _job_template: JobTemplateDTO = field(metadata=config(field_name="jobTemplate"))

    def validate(self):
        """Validates the response data.

        Raises:
            IllegalArgumentException: If the job template is not set or invalid.
        """
        super().validate()

        if self._job_template is None:
            raise IllegalArgumentException('"jobTemplate" must not be None')

        self._job_template.validate()

    def job_template(self) -> JobTemplateDTO:
        """Returns the job template from the response."""
        return self._job_template

    @classmethod
    def from_json(
        cls, s: str, infer_missing: bool = False, **kwargs
    ) -> "JobTemplateResponse":
        """Deserialize JSON string into a JobTemplateResponse object."""
        data = json.loads(s)
        job_template_data = JobTemplateDTO.from_json(
            json.dumps(data["jobTemplate"]), infer_missing=infer_missing, **kwargs
        )
        return cls(_job_template=job_template_data, _code=0)
