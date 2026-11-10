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

from dataclasses_json import config

from gravitino.api.rel.representation import Representation
from gravitino.dto.rel.representation_dto import RepresentationDTO
from gravitino.utils.precondition import Precondition


@dataclass
class SQLRepresentationDTO(RepresentationDTO):
    """DTO for SQL view representations."""

    _dialect: str = field(metadata=config(field_name="dialect"))
    _sql: str = field(metadata=config(field_name="sql"))
    _type: str = field(
        default=Representation.TYPE_SQL, metadata=config(field_name="type")
    )

    def type(self) -> str:
        """Returns the representation type."""
        return self._type

    def dialect(self) -> str:
        """Returns the SQL dialect."""
        return self._dialect

    def sql(self) -> str:
        """Returns the SQL text."""
        return self._sql

    def validate(self) -> None:
        Precondition.check_string_not_empty(
            self._dialect, '"dialect" field is required and cannot be empty'
        )
        Precondition.check_string_not_empty(
            self._sql, '"sql" field is required and cannot be empty'
        )

    def __hash__(self) -> int:
        return hash((self._type, self._dialect, self._sql))
