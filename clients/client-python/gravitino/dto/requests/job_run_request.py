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
from typing import Optional, Dict

from dataclasses_json import config

from gravitino.rest.rest_message import RESTRequest


@dataclass
class JobRunRequest(RESTRequest):
    """Represents a request to run a job using a specified job template."""

    _job_template_name: str = field(metadata=config(field_name="jobTemplateName"))
    _job_conf: Optional[Dict[str, str]] = field(
        default=None, metadata=config(field_name="jobConf")
    )

    def job_template_name(self) -> str:
        """Returns the job template name to use for running the job."""
        return self._job_template_name

    def job_conf(self) -> Optional[Dict[str, str]]:
        """Returns the job configuration parameters."""
        return self._job_conf

    def validate(self) -> None:
        """Validates the request. Raises ValueError if invalid."""
        if self._job_template_name is None or not self._job_template_name.strip():
            raise ValueError('"jobTemplateName" is required and cannot be empty')
