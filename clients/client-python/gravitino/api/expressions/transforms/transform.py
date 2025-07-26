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
from typing import List

from gravitino.api.expressions.expression import Expression
from gravitino.api.expressions.partitions.partition import Partition
from gravitino.api.expressions.partitions.partitions import Partitions


class Transform(Expression, ABC):
    """Represents a transform function in the public logical expression API.

    For example, the transform date(ts) is used to derive a date value from a timestamp column.
    The transform name is "date" and its argument is a reference to the "ts" column.
    """

    @abstractmethod
    def name(self) -> str:
        """Gets the transform function name.

        Returns:
            str: The transform function name.
        """
        pass

    @abstractmethod
    def arguments(self) -> List[Expression]:
        """Gets the arguments passed to the transform function.

        Returns:
            List[Expression]: The arguments passed to the transform function.
        """
        pass

    def assignments(self) -> List[Partition]:
        """Gets the preassigned partitions in the partitioning.

        Currently, only `Transforms.ListTransform` and `Transforms.RangeTransform` need to deal with
        assignments

        Returns:
            List[Partition]: The preassigned partitions in the partitioning.
        """
        return Partitions.EMPTY_PARTITIONS

    def children(self) -> List[Expression]:
        return self.arguments()
