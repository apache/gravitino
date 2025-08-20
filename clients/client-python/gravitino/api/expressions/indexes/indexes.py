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


from typing import ClassVar, List, Optional, final

from gravitino.api.expressions.indexes.index import Index


class Indexes:
    """Helper methods to create index to pass into Apache Gravitino.

    Attributes:
        EMPTY_INDEXES (List[Index]):
            An empty array of indexes.
        DEFAULT_MYSQL_PRIMARY_KEY_NAME (str):
            MySQL does not support setting the name of the primary key,
            so the default name is used.
    """

    EMPTY_INDEXES: ClassVar[List[Index]] = []
    DEFAULT_MYSQL_PRIMARY_KEY_NAME: ClassVar[str] = "PRIMARY"

    @staticmethod
    def unique(name: str, field_names: List[List[str]]) -> Index:
        return Indexes.IndexImpl(Index.IndexType.UNIQUE_KEY, name, field_names)

    @staticmethod
    def primary(name: str, field_names: List[List[str]]) -> Index:
        return Indexes.IndexImpl(Index.IndexType.PRIMARY_KEY, name, field_names)

    @staticmethod
    def create_mysql_primary_key(field_names: List[List[str]]) -> Index:
        return Indexes.primary(Indexes.DEFAULT_MYSQL_PRIMARY_KEY_NAME, field_names)

    @final
    class IndexImpl(Index):
        def __init__(
            self,
            index_type: Index.IndexType,
            name: Optional[str],
            field_names: List[List[str]],
        ):
            self._index_type = index_type
            self._name = name
            self._field_names = field_names

        def type(self) -> Index.IndexType:
            return self._index_type

        def name(self) -> Optional[str]:
            return self._name

        def field_names(self) -> List[List[str]]:
            return self._field_names
