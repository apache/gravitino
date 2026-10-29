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

from dataclasses import dataclass

from gravitino.utils.precondition import Precondition


@dataclass(frozen=True)
class PartitionStatisticsDrop:
    """Named statistics to delete from a partition, without deleting its data."""

    partition_name: str
    statistic_names: list[str]

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        """Validate the current input, including mutable collection contents."""
        Precondition.check_argument(
            isinstance(self.partition_name, str) and bool(self.partition_name.strip()),
            "Partition name must not be blank",
        )
        Precondition.check_argument(
            isinstance(self.statistic_names, list) and len(self.statistic_names) > 0,
            "Statistic names must not be null or empty",
        )
        for name in self.statistic_names:
            Precondition.check_argument(
                isinstance(name, str) and bool(name.strip()),
                "Statistic name must not be blank",
            )
