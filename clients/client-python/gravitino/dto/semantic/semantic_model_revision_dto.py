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
from datetime import datetime
from typing import Optional

from dataclasses_json import config

from gravitino.api.semantic.semantic_model_revision import (
    SemanticModelRevision,
    SemanticModelStatus,
)
from gravitino.dto.audit_dto import (
    AuditDTO,
    _deserialize_datetime,
    _serialize_datetime,
)
from gravitino.dto.semantic.json_serdes.semantic_json_mixin import SemanticJsonMixin
from gravitino.dto.semantic.semantic_dto_utils import is_none


@dataclass
class SemanticModelRevisionDTO(SemanticModelRevision, SemanticJsonMixin):
    """The wire representation of a Semantic Model revision summary."""

    _revision: Optional[int] = field(
        default=None, metadata=config(field_name="revision", exclude=is_none)
    )
    _status: Optional[SemanticModelStatus] = field(
        default=None, metadata=config(field_name="status", exclude=is_none)
    )
    _published_by: Optional[str] = field(
        default=None, metadata=config(field_name="publishedBy", exclude=is_none)
    )
    _published_at: Optional[datetime] = field(
        default=None,
        metadata=config(
            field_name="publishedAt",
            encoder=_serialize_datetime,
            decoder=_deserialize_datetime,
            exclude=is_none,
        ),
    )
    _audit: Optional[AuditDTO] = field(
        default=None, metadata=config(field_name="audit", exclude=is_none)
    )

    def revision(self) -> int:
        return self._revision or 0

    def status(self) -> Optional[SemanticModelStatus]:
        return self._status

    def published_by(self) -> Optional[str]:
        return self._published_by

    def published_at(self) -> Optional[datetime]:
        return self._published_at

    def audit_info(self) -> Optional[AuditDTO]:
        return self._audit
