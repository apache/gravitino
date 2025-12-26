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
from typing import Optional, List, Dict

from dataclasses_json import config, DataClassJsonMixin

from gravitino.api.job.job_template_change import TemplateUpdate


@dataclass
class TemplateUpdateDTO(DataClassJsonMixin, ABC):
    """Represents a template update data transfer object (DTO)."""

    _type: str = field(metadata=config(field_name="@type"))
    _new_executable: Optional[str] = field(metadata=config(field_name="newExecutable"))
    _new_arguments: Optional[List[str]] = field(
        metadata=config(field_name="newArguments")
    )
    _new_environments: Optional[Dict[str, str]] = field(
        metadata=config(field_name="newEnvironments")
    )
    _new_custom_fields: Optional[Dict[str, str]] = field(
        metadata=config(field_name="newCustomFields")
    )

    @abstractmethod
    def to_template_update(self) -> TemplateUpdate:
        pass
