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

from abc import ABC
from typing import List
from gravitino.api.expressions.expression import Expression
from gravitino.api.expressions.named_reference import NamedReference

from gravitino.api.expressions.partitions.partition import Partition
from gravitino.api.expressions.partitions.partitions import Partitions


class Transform(Expression, ABC):
    """Represents a transform function."""

    def name(self) -> str:
        """Returns the transform function name."""
        pass

    def arguments(self) -> List[Expression]:
        """Returns the arguments passed to the transform function."""
        pass

    def assignments(self) -> List[Partition]:
        """
        Returns the preassigned partitions for the transform.
        By default, it returns an empty list of partitions,
        as only some transforms like ListTransform and RangeTransform
        need to deal with assignments.
        """
        return Partitions.EMPTY_PARTITIONS

    def children(self) -> List[Expression]:
        """Returns the children expressions. By default, it is the arguments."""
        return self.arguments()


class SingleFieldTransform(Transform):
    """Base class for transforms on a single field."""

    def __init__(self, ref: NamedReference):
        self.ref = ref

    def field_name(self) -> List[str]:
        """Returns the referenced field name as a list of string parts."""
        return self.ref.field_name()

    def references(self) -> List[NamedReference]:
        """Returns a list of references (i.e., the field reference)."""
        return [self.ref]

    def arguments(self) -> List[Expression]:
        """Returns a list of arguments for the transform, which is just `ref`."""
        return [self.ref]

    def __eq__(self, other: object) -> bool:
        """Checks equality based on the `ref`."""
        if not isinstance(other, SingleFieldTransform):
            return False
        return self.ref == other.ref

    def __hash__(self) -> int:
        """Generates a hash based on `ref`."""
        return hash(self.ref)
