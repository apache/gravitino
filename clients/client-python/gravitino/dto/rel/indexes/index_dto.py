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

from functools import reduce
from typing import ClassVar, List, Optional

from gravitino.api.expressions.indexes.index import Index
from gravitino.utils.precondition import Precondition


class IndexDTO(Index):
    """Data transfer object representing index information.

    Attributes:
        EMPTY_INDEXES (List[IndexDTO]): An empty array of indexes.
    """

    EMPTY_INDEXES: ClassVar[List["IndexDTO"]] = []

    def __init__(
        self,
        index_type: Index.IndexType,
        name: Optional[str],
        field_names: List[List[str]],
    ):
        Precondition.check_argument(index_type is not None, "Index type cannot be null")
        Precondition.check_argument(
            field_names is not None and len(field_names) > 0,
            "The index must be set with corresponding column names",
        )

        self._index_type = index_type
        self._name = name
        self._field_names = field_names

    def type(self) -> Index.IndexType:
        return self._index_type

    def name(self) -> Optional[str]:
        return self._name

    def field_names(self) -> List[List[str]]:
        return self._field_names

    def __eq__(self, other) -> bool:
        if not isinstance(other, IndexDTO):
            return False
        return (
            self._index_type is other.type()
            and self._name == other.name()
            and self._field_names == other.field_names()
        )

    def __hash__(self) -> int:
        initial_hash = hash((self._index_type, self._name))
        return reduce(
            lambda result, field_name: 31 * result + hash(tuple(field_name)),
            self._field_names,
            initial_hash,
        )
