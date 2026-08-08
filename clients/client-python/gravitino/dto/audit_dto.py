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

from dataclasses_json import DataClassJsonMixin, config

from gravitino.api.audit import Audit


def _decode_datetime(date_str: Optional[str]) -> Optional[datetime]:
    if not date_str:
        return None
    return datetime.fromisoformat(date_str.replace("Z", "+00:00"))


def _encode_datetime(date_obj: Optional[datetime]) -> Optional[str]:
    if not date_obj:
        return None
    return date_obj.isoformat().replace("+00:00", "Z")


@dataclass
class AuditDTO(Audit, DataClassJsonMixin):
    """Data transfer object representing audit information."""

    _creator: Optional[str] = field(default=None, metadata=config(field_name="creator"))
    """The creator of the audit."""

    _create_time: Optional[datetime] = field(
        default=None,
        metadata=config(
            field_name="createTime", decoder=_decode_datetime, encoder=_encode_datetime
        ),
    )
    """The create time of the audit."""

    _last_modifier: Optional[str] = field(
        default=None, metadata=config(field_name="lastModifier")
    )
    """The last modifier of the audit."""

    _last_modified_time: Optional[datetime] = field(
        default=None,
        metadata=config(
            field_name="lastModifiedTime",
            decoder=_decode_datetime,
            encoder=_encode_datetime,
        ),
    )
    """The last modified time of the audit."""

    def __post_init__(self):
        if isinstance(self._create_time, str):
            self._create_time = _decode_datetime(self._create_time)
        if isinstance(self._last_modified_time, str):
            self._last_modified_time = _decode_datetime(self._last_modified_time)

    def __hash__(self):
        return hash(
            (
                self.creator(),
                self.create_time(),
                self.last_modifier(),
                self.last_modified_time(),
            )
        )

    def __eq__(self, other) -> bool:
        if not isinstance(other, AuditDTO):
            return False
        return (
            self.creator() == other.creator()
            and self.create_time() == other.create_time()
            and self.last_modifier() == other.last_modifier()
            and self.last_modified_time() == other.last_modified_time()
        )

    def creator(self) -> str:
        """The creator of the entity.

        Returns:
             the creator of the entity.
        """
        return self._creator

    def create_time(self) -> datetime:
        """The creation time of the entity.

        Returns:
             The creation time of the entity.
        """
        return self._create_time

    def last_modifier(self) -> str:
        """
        Returns:
             The last modifier of the entity.
        """
        return self._last_modifier

    def last_modified_time(self) -> datetime:
        """
        Returns:
             The last modified time of the entity.
        """
        return self._last_modified_time
