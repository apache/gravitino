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

from typing import Any

from dataclasses_json.core import Json

from gravitino.api.rel.indexes.index import Index
from gravitino.api.rel.types.json_serdes.base import JsonSerializable
from gravitino.dto.rel.indexes.index_dto import IndexDTO
from gravitino.utils.serdes import SerdesUtilsBase


class TableIndexSerdes(SerdesUtilsBase, JsonSerializable[Index]):
    """JSON serializer/deserializer for table indexes."""

    @classmethod
    def serialize(cls, data_type: Index) -> dict[str, Any]:
        data: dict[str, Any] = {cls.INDEX_TYPE: data_type.type().name.upper()}
        if data_type.name() is not None:
            data[cls.INDEX_NAME] = data_type.name()
        data[cls.INDEX_FIELD_NAMES] = data_type.field_names()

        return data

    @classmethod
    def deserialize(cls, data: Json) -> Index:
        if not isinstance(data, dict):
            raise ValueError(f"Invalid index data: {data}")

        index_type = data.get(cls.INDEX_TYPE)
        field_names = data.get(cls.INDEX_FIELD_NAMES)
        if index_type is None or field_names is None:
            raise ValueError(
                "Invalid index data: 'indexType' and 'fieldNames' are required"
            )

        return IndexDTO(
            index_type=Index.IndexType[index_type.upper()],
            name=data.get(cls.INDEX_NAME),
            field_names=field_names,
        )
