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

from typing import Optional, cast

from gravitino.api.rel.column import Column
from gravitino.api.rel.expressions.expression import Expression
from gravitino.api.rel.types.type import Type
from gravitino.utils import HTTPClient


class GenericColumn(Column):
    """Represents a generic column."""

    def __init__(
        self,
        column: Column,
        rest_client: HTTPClient,
        metalake: str,
        catalog: str,
        schema: str,
        table: str,
    ):
        self._internal_column = column

    def name(self) -> str:
        return self._internal_column.name()

    def data_type(self) -> Type:
        return self._internal_column.data_type()

    def comment(self) -> Optional[str]:
        return self._internal_column.comment()

    def nullable(self) -> bool:
        return self._internal_column.nullable()

    def auto_increment(self) -> bool:
        return self._internal_column.auto_increment()

    def default_value(self) -> Expression:
        return self._internal_column.default_value()

    def __hash__(self) -> int:
        return hash(self._internal_column)

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, GenericColumn):
            return False
        column = cast(GenericColumn, value)
        return self._internal_column == column._internal_column
