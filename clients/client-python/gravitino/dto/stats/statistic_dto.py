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

"""Data Transfer Object for representing a statistic."""

from typing import Optional

from gravitino.api.statistics.statistic import Statistic
from gravitino.api.statistics.statistic_value import StatisticValue
from gravitino.dto.stats.statistic_value_dto import StatisticValueDTO
from gravitino.rest.rest_message import RESTRequest


class StatisticDTO(Statistic, RESTRequest):  # pylint: disable=abstract-method
    """DTO representing a statistic."""

    _name: str
    _value: Optional[StatisticValueDTO]
    _reserved: bool
    _modifiable: bool

    def __init__(
        self,
        name: str = None,
        value: Optional[StatisticValueDTO] = None,
        reserved: bool = False,
        modifiable: bool = True,
    ):
        self._name = name
        self._value = value
        self._reserved = reserved
        self._modifiable = modifiable

    def name(self) -> str:
        return self._name

    def value(self) -> Optional[StatisticValue]:
        return self._value

    def reserved(self) -> bool:
        return self._reserved

    def modifiable(self) -> bool:
        return self._modifiable

    def validate(self):
        assert self._name is not None, "Statistic name must not be None"

    @classmethod
    # pylint: disable-next=arguments-differ,arguments-renamed
    def from_json(cls, json_dict: dict) -> "StatisticDTO":
        """Create a StatisticDTO from JSON representation."""
        value = None
        if "value" in json_dict and json_dict["value"] is not None:
            value = StatisticValueDTO.from_json(json_dict["value"])

        return cls(
            name=json_dict.get("name"),
            value=value,
            reserved=json_dict.get("reserved", False),
            modifiable=json_dict.get("modifiable", True),
        )

    def to_json(self) -> dict:  # pylint: disable=arguments-differ
        """Convert to JSON representation."""
        result = {
            "name": self._name,
            "reserved": self._reserved,
            "modifiable": self._modifiable,
        }
        if self._value is not None:
            result["value"] = self._value.to_json()
        return result
