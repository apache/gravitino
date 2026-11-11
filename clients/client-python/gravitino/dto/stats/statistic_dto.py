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
from typing import Any

from dataclasses_json import DataClassJsonMixin, config

from gravitino.api.audit import Audit
from gravitino.api.stats.json_serdes.statistic_value_serdes import StatisticValueSerdes
from gravitino.api.stats.statistic import Statistic
from gravitino.api.stats.statistic_value import StatisticValue
from gravitino.dto.audit_dto import AuditDTO
from gravitino.utils.precondition import Precondition


@dataclass
class StatisticDTO(Statistic, DataClassJsonMixin):
    """Data transfer object representing a statistic."""

    _name: str = field(metadata=config(field_name="name"))
    _reserved: bool = field(metadata=config(field_name="reserved"))
    _modifiable: bool = field(metadata=config(field_name="modifiable"))
    _audit: AuditDTO = field(metadata=config(field_name="audit"))
    _value: StatisticValue[Any] | None = field(
        default=None,
        metadata=config(
            field_name="value",
            encoder=StatisticValueSerdes.serialize,
            decoder=StatisticValueSerdes.deserialize,
            exclude=lambda v: v is None,
        ),
    )

    def name(self) -> str:
        return self._name

    def reserved(self) -> bool:
        return self._reserved

    def modifiable(self) -> bool:
        return self._modifiable

    def value(self) -> StatisticValue[Any] | None:
        return self._value

    def audit_info(self) -> Audit:
        return self._audit

    def validate(self) -> None:
        """Validates the StatisticDTO.

        Raises:
            IllegalArgumentException: If any of the required fields are invalid.
        """
        Precondition.check_string_not_empty(
            self._name, '"name" is required and cannot be empty'
        )
        Precondition.check_argument(
            self._audit is not None,
            '"audit" information is required and cannot be null',
        )

    def __hash__(self):
        return hash(
            (
                self._name,
                self._reserved,
                self._modifiable,
                hash(self._audit),
                hash(self._value),
            )
        )

    def __eq__(self, other) -> bool:
        if not isinstance(other, StatisticDTO):
            return False
        return (
            self._name == other._name
            and self._reserved == other._reserved
            and self._modifiable == other._modifiable
            and self._audit == other._audit
            and self._value == other._value
        )
