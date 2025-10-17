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
from typing import Optional, List

from dataclasses_json import config

from gravitino.api.job.job_template_change import TemplateUpdate, ShellTemplateUpdate
from gravitino.dto.job.template_update_dto import TemplateUpdateDTO


@dataclass
class ShellTemplateUpdateDTO(TemplateUpdateDTO):
    """DTO for updating shell job templates."""

    _new_scripts: Optional[List[str]] = field(metadata=config(field_name="newScripts"))

    def __init__(
        self,
        new_executable: Optional[str] = None,
        new_arguments: Optional[List[str]] = None,
        new_environments: Optional[dict] = None,
        new_custom_fields: Optional[dict] = None,
        new_scripts: Optional[List[str]] = None,
    ):
        super().__init__(
            _type="shell",
            _new_executable=new_executable,
            _new_arguments=new_arguments,
            _new_environments=new_environments,
            _new_custom_fields=new_custom_fields,
        )
        self._new_scripts = new_scripts

    def to_template_update(self) -> TemplateUpdate:
        return ShellTemplateUpdate(
            new_executable=self._new_executable,
            new_arguments=self._new_arguments,
            new_environments=self._new_environments,
            new_custom_fields=self._new_custom_fields,
            new_scripts=self._new_scripts,
        )
