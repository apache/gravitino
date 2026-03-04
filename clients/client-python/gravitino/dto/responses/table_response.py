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

from gravitino.dto.rel.table_dto import TableDTO
from gravitino.dto.responses.base_response import BaseResponse
from gravitino.utils.precondition import Precondition


@dataclass
class TableResponse(BaseResponse):
    """Represents a response for a table."""

    _table: TableDTO = field(metadata=config(field_name="table"))

    def table(self) -> TableDTO:
        return self._table

    def validate(self) -> None:
        Precondition.check_string_not_empty(
            self._table.name(), "table 'name' must not be null and empty"
        )
        Precondition.check_argument(
            self._table.audit_info() is not None, "table 'audit' must not be null"
        )
        Precondition.check_argument(
            self._table.partitioning() is not None,
            "table 'partitions' must not be null",
        )
