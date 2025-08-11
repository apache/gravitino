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
from dataclasses_json import config, DataClassJsonMixin

from gravitino.api.job.job_handle import JobHandle
from gravitino.dto.audit_dto import AuditDTO


@dataclass
class JobDTO(DataClassJsonMixin):
    """Data transfer object representing a Job."""

    _job_id: str = field(metadata=config(field_name="jobId"))
    _job_template_name: str = field(metadata=config(field_name="jobTemplateName"))
    _status: JobHandle.Status = field(
        metadata=config(
            field_name="status",
            encoder=JobHandle.Status.job_status_serialize,
            decoder=JobHandle.Status.job_status_deserialize,
        )
    )
    _audit: AuditDTO = field(metadata=config(field_name="audit"))

    def job_id(self) -> str:
        """Returns the job ID."""
        return self._job_id

    def job_template_name(self) -> str:
        """Returns the job template name."""
        return self._job_template_name

    def status(self) -> JobHandle.Status:
        """Returns the status of the job."""
        return self._status

    def audit(self) -> AuditDTO:
        """Returns the audit information of the job."""
        return self._audit

    def validate(self) -> None:
        """Validates the JobDTO, ensuring required fields are present and non-empty."""
        if self._job_id is None or not self._job_id.strip():
            raise ValueError('"jobId" is required and cannot be empty')
        if self._job_template_name is None or not self._job_template_name.strip():
            raise ValueError('"jobTemplateName" is required and cannot be empty')
        if self._status is None:
            raise ValueError('"status" must not be None')
        if self._audit is None:
            raise ValueError('"audit" must not be None')
