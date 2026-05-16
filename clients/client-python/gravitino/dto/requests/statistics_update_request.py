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

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from dataclasses_json import config

from gravitino.api.stats.json_serdes.statistic_value_serdes import StatisticValueSerdes
from gravitino.api.stats.statistic_value import StatisticValue
from gravitino.rest.rest_message import RESTRequest
from gravitino.utils.precondition import Precondition


@dataclass
class StatisticsUpdateRequest(RESTRequest):
    """Represents a request to update statistics."""

    _updates: dict[str, StatisticValue[Any]] = field(
        metadata=config(
            field_name="updates",
            encoder=lambda mapping: {
                key: StatisticValueSerdes.serialize(value)
                for key, value in mapping.items()
            },
            decoder=lambda mapping: {
                key: StatisticValueSerdes.deserialize(value)
                for key, value in mapping.items()
            },
        )
    )

    @property
    def updates(self) -> dict[str, StatisticValue[Any]]:
        return self._updates

    def validate(self) -> None:
        Precondition.check_argument(
            self._updates is not None and len(self._updates) > 0,
            '"updates" must not be null or empty',
        )
        for name, value in self._updates.items():
            Precondition.check_string_not_empty(
                name, 'statistic "name" must not be null or empty'
            )
            Precondition.check_argument(
                value is not None, f"statistic \"value\" for '{name}' must not be null"
            )
