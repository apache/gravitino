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

from typing import overload

from dataclasses_json.core import Json

from gravitino.api.column import Column
from gravitino.api.expressions.expression import Expression
from gravitino.api.types.json_serdes.base import JsonSerializable
from gravitino.dto.rel.expressions.json_serdes._helper.serdes_utils import SerdesUtils


class ColumnDefaultValueSerdes(JsonSerializable[Expression]):
    """Custom JSON serializer/deserializer for Column default value."""

    @classmethod
    def serialize(cls, value: Expression) -> Json:
        if cls.is_empty(value):
            return None
        return SerdesUtils.write_function_arg(arg=value)

    @classmethod
    def deserialize(cls, data: Json) -> Expression:
        if cls.is_empty(data):
            return Column.DEFAULT_VALUE_NOT_SET
        return SerdesUtils.read_function_arg(data=data)

    @classmethod
    @overload
    def is_empty(cls, value: Expression) -> bool: ...

    @classmethod
    @overload
    def is_empty(cls, value: Json) -> bool: ...

    @classmethod
    def is_empty(cls, value):
        if isinstance(value, (Expression, list)):
            return value is None or value is Column.DEFAULT_VALUE_NOT_SET
        return value is None
