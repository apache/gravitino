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

from typing import Dict, List, Optional
from dataclasses import dataclass, field

from dataclasses_json import config

from .job_template_dto import JobTemplateDTO, register_job_template
from ...api.job.job_template import JobType


@register_job_template(JobType.SPARK)
@dataclass
class SparkJobTemplateDTO(JobTemplateDTO):
    """Represents a Spark Job Template Data Transfer Object (DTO)."""

    _class_name: str = field(default=None, metadata=config(field_name="className"))
    _jars: Optional[List[str]] = field(default=None, metadata=config(field_name="jars"))
    _files: Optional[List[str]] = field(
        default=None, metadata=config(field_name="files")
    )
    _archives: Optional[List[str]] = field(
        default=None, metadata=config(field_name="archives")
    )
    _configs: Optional[Dict[str, str]] = field(
        default=None, metadata=config(field_name="configs")
    )

    def class_name(self) -> str:
        """Returns the class name of the Spark job."""
        return self._class_name

    def jars(self) -> Optional[List[str]]:
        """Returns the list of JAR files associated with this Spark job template."""
        return self._jars

    def files(self) -> Optional[List[str]]:
        """Returns the list of files associated with this Spark job template."""
        return self._files

    def archives(self) -> Optional[List[str]]:
        """Returns the list of archives associated with this Spark job template."""
        return self._archives

    def configs(self) -> Optional[Dict[str, str]]:
        """Returns the configuration properties for the Spark job."""
        return self._configs

    def validate(self) -> None:
        """Validates the SparkJobTemplateDTO. Ensures that required fields are not null or empty."""
        super().validate()

        if self._class_name is None or not self._class_name.strip():
            raise ValueError('"className" is required and cannot be empty')
