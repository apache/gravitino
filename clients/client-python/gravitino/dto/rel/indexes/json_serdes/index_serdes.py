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

from gravitino.api.expressions.indexes.index import Index
from gravitino.api.types.json_serdes import JsonSerializable
from gravitino.dto.rel.indexes.index_dto import IndexDTO
from gravitino.utils.precondition import Precondition
from gravitino.utils.serdes import SerdesUtilsBase


class IndexSerdes(SerdesUtilsBase, JsonSerializable[Index]):
    @classmethod
    def serialize(cls, data_type: Index) -> dict[str, Any]:
        result: dict[str, Any] = {cls.INDEX_TYPE: data_type.type().name.upper()}
        if data_type.name() is not None:
            result[cls.INDEX_NAME] = data_type.name()
        result[cls.INDEX_FIELD_NAMES] = data_type.field_names()

        return result

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Index:
        Precondition.check_argument(
            isinstance(data, dict) and len(data) > 0,
            f"Index must be a valid JSON object, but found: {data}",
        )
        Precondition.check_argument(
            data.get(cls.INDEX_TYPE) is not None,
            f"Cannot parse index from missing type: {data}",
        )
        Precondition.check_argument(
            data.get(cls.INDEX_FIELD_NAMES) is not None,
            f"Cannot parse index from missing field names: {data}",
        )
        index_type = Index.IndexType(data[cls.INDEX_TYPE].upper())

        return IndexDTO(
            index_type, data.get(cls.INDEX_NAME), data[cls.INDEX_FIELD_NAMES]
        )
