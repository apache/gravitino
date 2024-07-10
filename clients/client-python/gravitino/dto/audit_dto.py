"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""

from dataclasses import dataclass, field
from typing import Optional

from dataclasses_json import DataClassJsonMixin, config

from gravitino.api.audit import Audit


@dataclass
class AuditDTO(Audit, DataClassJsonMixin):
    """Data transfer object representing audit information."""

    _creator: Optional[str] = field(default=None, metadata=config(field_name="creator"))
    """The creator of the audit."""

    _create_time: Optional[str] = field(
        default=None, metadata=config(field_name="createTime")
    )  # TODO: Can't deserialized datetime from JSON
    """The create time of the audit."""

    _last_modifier: Optional[str] = field(
        default=None, metadata=config(field_name="lastModifier")
    )
    """The last modifier of the audit."""

    _last_modified_time: Optional[str] = field(
        default=None, metadata=config(field_name="lastModifiedTime")
    )  # TODO: Can't deserialized datetime from JSON
    """The last modified time of the audit."""

    def creator(self) -> str:
        """The creator of the entity.

        Returns:
             the creator of the entity.
        """
        return self._creator

    def create_time(self) -> str:
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

    def last_modified_time(self) -> str:
        """
        Returns:
             The last modified time of the entity.
        """
        return self._last_modified_time
