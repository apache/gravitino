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
from typing import Optional, Dict, List

from dataclasses_json import DataClassJsonMixin, config

from gravitino.api.model.model_version import ModelVersion
from gravitino.dto.audit_dto import AuditDTO


@dataclass
class ModelVersionDTO(ModelVersion, DataClassJsonMixin):
    """Represents a Model Version DTO (Data Transfer Object)."""

    _version: int = field(metadata=config(field_name="version"))
    _comment: Optional[str] = field(metadata=config(field_name="comment"))
    _aliases: Optional[List[str]] = field(metadata=config(field_name="aliases"))
    _uri: str = field(metadata=config(field_name="uri"))
    _properties: Optional[Dict[str, str]] = field(
        metadata=config(field_name="properties")
    )
    _audit: AuditDTO = field(default=None, metadata=config(field_name="audit"))

    def version(self) -> int:
        return self._version

    def comment(self) -> Optional[str]:
        return self._comment

    def aliases(self) -> Optional[List[str]]:
        return self._aliases

    def uri(self) -> str:
        return self._uri

    def properties(self) -> Optional[Dict[str, str]]:
        return self._properties

    def audit_info(self) -> AuditDTO:
        return self._audit
