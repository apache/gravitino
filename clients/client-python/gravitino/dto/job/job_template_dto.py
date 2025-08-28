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
from abc import ABC
from typing import Dict, List, Optional, Type
from dataclasses import dataclass, field

from dataclasses_json import config, DataClassJsonMixin

from gravitino.api.job.job_template import JobType
from gravitino.dto.audit_dto import AuditDTO


@dataclass
class JobTemplateDTO(DataClassJsonMixin, ABC):
    """Represents a Job Template Data Transfer Object (DTO)."""

    # pylint: disable=R0902
    _job_type: JobType = field(
        metadata=config(
            field_name="jobType",
            encoder=JobType.job_type_serialize,
            decoder=JobType.job_type_deserialize,
        )
    )
    _name: str = field(metadata=config(field_name="name"))
    _executable: str = field(metadata=config(field_name="executable"))
    _comment: Optional[str] = field(default=None, metadata=config(field_name="comment"))
    _arguments: Optional[List[str]] = field(
        default=None, metadata=config(field_name="arguments")
    )
    _environments: Optional[Dict[str, str]] = field(
        default=None, metadata=config(field_name="environments")
    )
    _custom_fields: Optional[Dict[str, str]] = field(
        default=None, metadata=config(field_name="customFields")
    )
    _audit: Optional[AuditDTO] = field(
        default=None, metadata=config(field_name="audit")
    )

    def job_type(self) -> JobType:
        """Returns the type of the job."""
        return self._job_type

    def name(self) -> str:
        """Returns the name of the job template."""
        return self._name

    def comment(self) -> Optional[str]:
        """Returns the comment associated with the job template."""
        return self._comment

    def executable(self) -> str:
        """Returns the executable associated with the job template."""
        return self._executable

    def arguments(self) -> Optional[List[str]]:
        """Returns the list of arguments for the job template."""
        return self._arguments

    def environments(self) -> Optional[Dict[str, str]]:
        """Returns the environment variables for the job template."""
        return self._environments

    def custom_fields(self) -> Optional[Dict[str, str]]:
        """Returns custom fields for the job template."""
        return self._custom_fields

    def audit_info(self) -> Optional[AuditDTO]:
        """Returns the audit information for the job template."""
        return self._audit

    def validate(self) -> None:
        """Validates the JobTemplateDTO. Ensures that required fields are not null or empty."""
        if self._job_type is None:
            raise ValueError('"jobType" is required and cannot be None')

        if self._name is None or not self._name.strip():
            raise ValueError('"name" is required and cannot be empty')

        if self._executable is None or not self._executable.strip():
            raise ValueError('"executable" is required and cannot be empty')

    @classmethod
    def from_json(
        cls, s: str, infer_missing: bool = False, **kwargs
    ) -> "JobTemplateDTO":
        """Creates a JobTemplateDTO from a JSON string."""
        data = json.loads(s)
        job_type = JobType.job_type_deserialize(data.get("jobType"))
        subclass = JOB_TYPE_TEMPLATE_MAPPING.get(job_type)
        if not subclass:
            raise ValueError(f"Unsupported job type: {job_type}")
        return subclass.from_dict(data, infer_missing=infer_missing)


JOB_TYPE_TEMPLATE_MAPPING: Dict[JobType, Type["JobTemplateDTO"]] = {}


def register_job_template(job_type: JobType):
    """
    Decorator to register a subclass of JobTemplateDTO for a specific job type.

    Args:
        job_type (JobType): The job type to register the subclass for.

    Returns:
        Callable: The decorator function.
    """

    def decorator(subclass: Type["JobTemplateDTO"]):
        JOB_TYPE_TEMPLATE_MAPPING[job_type] = subclass
        return subclass

    return decorator
