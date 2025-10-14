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
from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from dataclasses_json import config

from gravitino.api.job.job_template_change import JobTemplateChange
from gravitino.dto.job.template_update_dto import TemplateUpdateDTO
from gravitino.rest.rest_message import RESTRequest


@dataclass
class JobTemplateUpdateRequest(RESTRequest, ABC):
    """Represents a request to update a job template."""

    _type: str = field(metadata=config(field_name="@type"))

    def __init__(self, action_type: str):
        self._type = action_type

    @abstractmethod
    def job_template_change(self) -> JobTemplateChange:
        """Converts the request to a JobTemplateChange object."""
        pass


@dataclass
class RenameJobTemplateRequest(JobTemplateUpdateRequest):
    """Request to rename a job template."""

    _new_name: str = field(metadata=config(field_name="newName"))
    """The new name for the job template."""

    def __init__(self, new_name: str):
        super().__init__("rename")
        self._new_name = new_name

    def validate(self):
        """Validates the fields of the request.

        Raises:
            ValueError if the new name is not set.
        """
        if not self._new_name:
            raise ValueError('"new_name" field is required and cannot be empty')

    def job_template_change(self) -> JobTemplateChange:
        return JobTemplateChange.rename(new_name=self._new_name)


@dataclass
class UpdateJobTemplateCommentRequest(JobTemplateUpdateRequest):
    """Request to update the comment of a job template."""

    _new_comment: str = field(metadata=config(field_name="newComment"))
    """The new comment for the job template."""

    def __init__(self, new_comment: str):
        super().__init__("updateComment")
        self._new_comment = new_comment

    def validate(self):
        """Validates the fields of the request.

        Raises:
            ValueError if the new comment is not set.
        """
        if self._new_comment is None:
            raise ValueError('"new_comment" field is required and cannot be None')

    def job_template_change(self) -> JobTemplateChange:
        return JobTemplateChange.update_comment(new_comment=self._new_comment)


@dataclass
class UpdateJobTemplateContentRequest(JobTemplateUpdateRequest):
    """Request to update the content of a job template."""

    _new_template: TemplateUpdateDTO = field(metadata=config(field_name="newTemplate"))
    """The template update details."""

    def __init__(self, new_template: TemplateUpdateDTO):
        super().__init__("updateTemplate")
        self._new_template = new_template

    def validate(self):
        """Validates the fields of the request.

        Raises:
            ValueError if the template update is not set.
        """
        if self._new_template is None:
            raise ValueError('"new_template" field is required and cannot be None')

    def job_template_change(self) -> JobTemplateChange:
        return JobTemplateChange.update_template(
            self._new_template.to_template_update()
        )
