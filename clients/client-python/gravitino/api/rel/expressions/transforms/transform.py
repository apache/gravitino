"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""

from abc import ABC, abstractmethod
from typing import List, Tuple

from gravitino.api.rel.expressions.partitions.partition import Partition
from gravitino.api.rel.expressions.partitions.partitions import Partitions
from gravitino.api.rel.expressions.expression import Expression
from gravitino.api.rel.expressions.named_reference import NamedReference

class Transform(Expression, ABC):
    """
    Represents a transform function in the public logical expression API.
    For example, the transform 'date(ts)' is used to derive a date value from a timestamp column.
    The transform name is 'date' and its argument is a reference to the 'ts' column.
    """

    @abstractmethod
    def name(self) -> str:
        """Return the transform function name."""
        pass

    @abstractmethod
    def arguments(self) -> List[Expression]:
        """Return the arguments passed to the transform function."""
        pass

    def assignments(self) -> List[Partition]:
        """
        Return the preassigned partitions in the partitioning. Currently, only specific
        transforms like ListTransform and RangeTransform need to deal with assignments.
        """
        return Partitions.EMPTY_PARTITIONS

    def children(self) -> List[Expression]:
        """
        Default implementation to return arguments as children of this expression node.
        """
        return self.arguments()

class SingleFieldTransform(Transform, ABC):
    """
    Base class for simple transforms of a single field.
    """

    def __init__(self, ref: NamedReference):
        self.ref = ref

    def field_name(self) -> Tuple[str, ...]:
        """Return the referenced field name as a tuple of string parts."""
        return self.ref.field_name()

    def references(self) -> List[NamedReference]:
        """Return a list containing the reference used in this transform."""
        return [self.ref]

    def arguments(self) -> List[Expression]:
        """Return the reference as the sole argument to this transform."""
        return [self.ref]

    def __eq__(self, other) -> bool:
        """
        Check equality with another object, primarily checking the reference.
        """
        if not isinstance(other, SingleFieldTransform):
            return False
        return self.ref == other.ref

    def __hash__(self) -> int:
        """
        Compute a hash based on the reference.
        """
        return hash(self.ref)
