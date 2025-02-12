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

from dataclasses_json import config, DataClassJsonMixin

from gravitino.api.file.fileset import Fileset
from gravitino.dto.audit_dto import AuditDTO


@dataclass
class FilesetDTO(Fileset, DataClassJsonMixin):
    """Represents a Fileset DTO (Data Transfer Object)."""

    _name: str = field(metadata=config(field_name="name"))
    _comment: Optional[str] = field(metadata=config(field_name="comment"))
    _type: Fileset.Type = field(metadata=config(field_name="type"))
    _properties: Dict[str, str] = field(metadata=config(field_name="properties"))
    _storage_location: str = field(
        default=None, metadata=config(field_name="storageLocation")
    )
    _audit: AuditDTO = field(default=None, metadata=config(field_name="audit"))

    def name(self) -> str:
        return self._name

    def type(self) -> Fileset.Type:
        return self._type

    def storage_location(self) -> str:
        return self._storage_location

    def comment(self) -> Optional[str]:
        return self._comment

    def properties(self) -> Dict[str, str]:
        return self._properties

    def audit_info(self) -> AuditDTO:
        return self._audit
