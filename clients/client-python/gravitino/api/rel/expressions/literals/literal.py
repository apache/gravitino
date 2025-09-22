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

from abc import abstractmethod
from typing import Generic, List, TypeVar

from gravitino.api.rel.expressions.expression import Expression
from gravitino.api.rel.types.type import Type

T = TypeVar("T")


class Literal(Generic[T], Expression):
    """
    Represents a constant literal value in the public expression API.
    """

    @abstractmethod
    def value(self) -> T:
        """The literal value."""
        raise NotImplementedError("Subclasses must implement the `value` method.")

    @abstractmethod
    def data_type(self) -> Type:
        """The data type of the literal."""
        raise NotImplementedError("Subclasses must implement the `data_type` method.")

    def children(self) -> List[Expression]:
        return Expression.EMPTY_EXPRESSION
