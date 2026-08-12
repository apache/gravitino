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

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from dataclasses_json import DataClassJsonMixin, config

from gravitino.api.audit import Audit

_FRACTIONAL_SECONDS_PATTERN = re.compile(r"(\.\d{6})\d+")


def _deserialize_datetime(value: Optional[datetime | str]) -> Optional[datetime]:
    if value is None or isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        raise TypeError(f"Audit time must be an ISO-8601 string, got {type(value)}")

    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    # Match Python 3.11+ by truncating nanoseconds to microsecond precision.
    normalized = _FRACTIONAL_SECONDS_PATTERN.sub(r"\1", normalized, count=1)
    parsed = datetime.fromisoformat(normalized)
    return parsed.astimezone(timezone.utc) if parsed.tzinfo is not None else parsed


def _serialize_datetime(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, datetime):
        raise TypeError(f"Audit time must be a datetime, got {type(value)}")

    normalized = value.astimezone(timezone.utc) if value.tzinfo is not None else value
    return normalized.isoformat().replace("+00:00", "Z")


@dataclass
class AuditDTO(Audit, DataClassJsonMixin):
    """Data transfer object representing audit information."""

    _creator: Optional[str] = field(default=None, metadata=config(field_name="creator"))
    """The creator of the audit."""

    _create_time: Optional[datetime] = field(
        default=None,
        metadata=config(
            field_name="createTime",
            encoder=_serialize_datetime,
            decoder=_deserialize_datetime,
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
            encoder=_serialize_datetime,
            decoder=_deserialize_datetime,
        ),
    )
    """The last modified time of the audit."""

    def __post_init__(self) -> None:
        self._create_time = _deserialize_datetime(self._create_time)
        self._last_modified_time = _deserialize_datetime(self._last_modified_time)

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

    def creator(self) -> Optional[str]:
        """The creator of the entity.

        Returns:
             The creator of the entity, or ``None`` if unavailable.
        """
        return self._creator

    def create_time(self) -> Optional[datetime]:
        """The creation time of the entity.

        Returns:
             The creation time of the entity, or ``None`` if unavailable.
        """
        return self._create_time

    def last_modifier(self) -> Optional[str]:
        """The last modifier of the entity.

        Returns:
             The last modifier of the entity, or ``None`` if unavailable.
        """
        return self._last_modifier

    def last_modified_time(self) -> Optional[datetime]:
        """The last modified time of the entity.

        Returns:
             The last modified time of the entity, or ``None`` if unavailable.
        """
        return self._last_modified_time
