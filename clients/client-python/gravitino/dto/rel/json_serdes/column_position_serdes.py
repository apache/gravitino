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

from typing import Union

from dataclasses_json.core import Json

from gravitino.api.rel.table_change import After, Default, First, TableChange
from gravitino.api.rel.types.json_serdes.base import JsonSerializable


class ColumnPositionSerdes(JsonSerializable[TableChange.ColumnPosition]):
    """JSON serializer/deserializer for table column positions."""

    _POSITION_FIRST = "first"
    _POSITION_AFTER = "after"
    _POSITION_DEFAULT = "default"

    @classmethod
    def serialize(
        cls,
        data_type: TableChange.ColumnPosition,
    ) -> Union[str, dict[str, str]]:
        if isinstance(data_type, First):
            return cls._POSITION_FIRST
        if isinstance(data_type, After):
            return {cls._POSITION_AFTER: data_type.get_column()}
        if isinstance(data_type, Default):
            return cls._POSITION_DEFAULT

        raise ValueError(f"Unknown column position: {data_type}")

    @classmethod
    def deserialize(cls, data: Json) -> Union[First, After, Default]:
        if isinstance(data, str):
            data = data.lower()
            if data == cls._POSITION_FIRST:
                return TableChange.ColumnPosition.first()
            if data == cls._POSITION_DEFAULT:
                return TableChange.ColumnPosition.default_pos()

        if isinstance(data, dict):
            after_column = data.get(cls._POSITION_AFTER)
            if after_column:
                return TableChange.ColumnPosition.after(after_column)

        raise ValueError(f"Unknown json column position: {data}")
