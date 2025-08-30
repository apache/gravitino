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

from abc import ABC, abstractmethod
from typing import Generic, TypeVar, Union

from dataclasses_json.core import Json

from gravitino.api.expressions.distributions.distribution import Distribution
from gravitino.api.expressions.expression import Expression
from gravitino.api.expressions.indexes.index import Index
from gravitino.api.expressions.sorts.sort_order import SortOrder
from gravitino.api.types.types import Type
from gravitino.dto.rel.partitioning.partitioning import Partitioning
from gravitino.dto.rel.partitions.partition_dto import PartitionDTO

_GravitinoTypeT = TypeVar(
    "_GravitinoTypeT",
    bound=Union[
        Expression, Type, Partitioning, PartitionDTO, Distribution, Index, SortOrder
    ],
)


class JsonSerializable(ABC, Generic[_GravitinoTypeT]):
    """Customized generic Serializer for DataClassJson."""

    @classmethod
    @abstractmethod
    def serialize(cls, data_type: _GravitinoTypeT) -> Json:
        """To serialize the given `data`.

        Args:
            data (_GravitinoTypeT): The data to be serialized.

        Returns:
            Json: The serialized data.
        """
        pass

    @classmethod
    @abstractmethod
    def deserialize(cls, data: Json) -> _GravitinoTypeT:
        """To deserialize the given `data`.

        Args:
            data (Json): The data to be deserialized.

        Returns:
            _GravitinoTypeT: The deserialized data.
        """
        pass
