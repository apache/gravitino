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

from typing import ClassVar, List, Optional, Union, overload

from gravitino.api.expressions.expression import Expression
from gravitino.api.expressions.literals.literal import Literal
from gravitino.api.expressions.literals.literals import Literals
from gravitino.api.expressions.named_reference import NamedReference
from gravitino.api.expressions.partitions.list_partition import ListPartition
from gravitino.api.expressions.partitions.partition import Partition
from gravitino.api.expressions.partitions.range_partition import RangePartition
from gravitino.api.expressions.transforms.transform import (
    SingleFieldTransform,
    Transform,
)


class Transforms(Transform):
    """Helper methods to create logical transforms to pass into Apache Gravitino."""

    EMPTY_TRANSFORM: ClassVar[List[Transform]] = []
    """An empty array of transforms."""
    NAME_OF_IDENTITY: ClassVar[str] = "identity"
    """The name of the identity transform."""
    NAME_OF_YEAR: ClassVar[str] = "year"
    """The name of the year transform. The year transform returns the year of the input value."""
    NAME_OF_MONTH: ClassVar[str] = "month"
    """The name of the month transform. The month transform returns the month of the input value."""
    NAME_OF_DAY: ClassVar[str] = "day"
    """The name of the day transform. The day transform returns the day of the input value."""
    NAME_OF_HOUR: ClassVar[str] = "hour"
    """The name of the hour transform. The hour transform returns the hour of the input value."""
    NAME_OF_BUCKET: ClassVar[str] = "bucket"
    """The name of the bucket transform. The bucket transform returns the bucket of the input value."""
    NAME_OF_TRUNCATE: ClassVar[str] = "truncate"
    """The name of the truncate transform. The truncate transform returns the truncated value of the"""
    NAME_OF_LIST: ClassVar[str] = "list"
    """The name of the list transform. The list transform includes multiple fields in a list."""
    NAME_OF_RANGE: ClassVar[str] = "range"
    """The name of the range transform. The range transform returns the range of the input value."""

    @staticmethod
    @overload
    def identity(field_name: List[str]) -> "Transforms.IdentityTransform": ...

    @staticmethod
    @overload
    def identity(field_name: str) -> "Transforms.IdentityTransform": ...

    @staticmethod
    def identity(field_name: Union[str, List[str]]) -> "Transforms.IdentityTransform":
        """Create a transform that returns the input value.

        Args:
            field_name (List[str]):
                The field name(s) to transform. Can be a list of field names or a single field name.
        Returns:
            Transforms.IdentityTransform: The created transform
        """

        return Transforms.IdentityTransform(
            NamedReference.field(
                [field_name] if isinstance(field_name, str) else field_name
            )
        )

    @staticmethod
    @overload
    def year(field_name: List[str]) -> "Transforms.YearTransform": ...

    @staticmethod
    @overload
    def year(field_name: str) -> "Transforms.YearTransform": ...

    @staticmethod
    def year(field_name: Union[str, List[str]]) -> "Transforms.YearTransform":
        """Create a transform that returns the input value.

        Args:
            field_name (List[str]):
                The field name(s) to transform. Can be a list of field names or a single field name.
        Returns:
            Transforms.YearTransform: The created transform
        """

        return Transforms.YearTransform(
            NamedReference.field(
                [field_name] if isinstance(field_name, str) else field_name
            )
        )

    @staticmethod
    @overload
    def month(field_name: List[str]) -> "Transforms.MonthTransform": ...

    @staticmethod
    @overload
    def month(field_name: str) -> "Transforms.MonthTransform": ...

    @staticmethod
    def month(field_name: Union[str, List[str]]) -> "Transforms.MonthTransform":
        """Create a transform that returns the input value.

        Args:
            field_name (List[str]):
                The field name(s) to transform. Can be a list of field names or a single field name.
        Returns:
            MonthTransform: The created transform
        """

        return Transforms.MonthTransform(
            NamedReference.field(
                [field_name] if isinstance(field_name, str) else field_name
            )
        )

    @staticmethod
    @overload
    def day(field_name: List[str]) -> "Transforms.DayTransform": ...

    @staticmethod
    @overload
    def day(field_name: str) -> "Transforms.DayTransform": ...

    @staticmethod
    def day(field_name: Union[str, List[str]]) -> "Transforms.DayTransform":
        """Create a transform that returns the input value.

        Args:
            field_name (List[str]):
                The field name(s) to transform. Can be a list of field names or a single field name.
        Returns:
            DayTransform: The created transform
        """

        return Transforms.DayTransform(
            NamedReference.field(
                [field_name] if isinstance(field_name, str) else field_name
            )
        )

    @staticmethod
    @overload
    def hour(field_name: List[str]) -> "Transforms.HourTransform": ...

    @staticmethod
    @overload
    def hour(field_name: str) -> "Transforms.HourTransform": ...

    @staticmethod
    def hour(field_name: Union[str, List[str]]) -> "Transforms.HourTransform":
        """Create a transform that returns the input value.

        Args:
            field_name (List[str]):
                The field name(s) to transform. Can be a list of field names or a single field name.
        Returns:
            Transforms.HourTransform: The created transform
        """

        return Transforms.HourTransform(
            NamedReference.field(
                [field_name] if isinstance(field_name, str) else field_name
            )
        )

    @staticmethod
    def bucket(
        num_buckets: int, *field_names: List[str]
    ) -> "Transforms.BucketTransform":
        """Create a transform that returns the bucket of the input value.

        Args:
            num_buckets (int): The number of buckets to use
            *field_names (List[str]): The field names to transform

        Returns:
            Transforms.BucketTransform: The created transform
        """
        return Transforms.BucketTransform(
            num_buckets=Literals.integer_literal(value=num_buckets),
            fields=[NamedReference.field(names) for names in field_names],
        )

    @staticmethod
    @overload
    def truncate(
        width: int, field_name: List[str]
    ) -> "Transforms.TruncateTransform": ...

    @staticmethod
    @overload
    def truncate(width: int, field_name: str) -> "Transforms.TruncateTransform": ...

    @staticmethod
    def truncate(
        width: int, field_name: Union[str, List[str]]
    ) -> "Transforms.TruncateTransform":
        """Create a transform that returns the truncated value of the input value with the given width.

        Args:
            width (int): The width to truncate to
            field_name (Union[str, List[str]]): The column/field name to transform

        Returns:
            Transforms.TruncateTransform: The created transform
        """
        return Transforms.TruncateTransform(
            width=Literals.integer_literal(value=width),
            field=NamedReference.field(
                [field_name] if isinstance(field_name, str) else field_name
            ),
        )

    @staticmethod
    def apply(name: str, arguments: List[Expression]) -> "Transforms.ApplyTransform":
        """Create a transform that applies a function to the input value.

        Args:
            name (str): The name of the function to apply
            arguments (List[Expression]): he arguments to the function

        Returns:
            Transforms.ApplyTransform: The created transform
        """

        return Transforms.ApplyTransform(name=name, arguments=arguments)

    @staticmethod
    def list(
        *field_names: List[str], assignments: Optional[List[ListPartition]] = None
    ) -> "Transforms.ListTransform":
        """Create a transform that includes multiple fields in a list.

        Args:
            *fields (List[NamedReference]):
                The fields to include in the list
            assignments (Optional[List[ListPartition]]):
                The preassigned list partitions

        Returns:
            Transforms.ListTransform: The created transform
        """
        return Transforms.ListTransform(
            fields=[
                NamedReference.field(field_name=field_name)
                for field_name in field_names
            ],
            assignments=[] if assignments is None else assignments,
        )

    @staticmethod
    def range(
        field_name: List[str], assignments: Optional[List[RangePartition]] = None
    ) -> "Transforms.RangeTransform":
        """Create a transform that returns the range of the input value with preassigned range partitions.

        Args:
            field_name (List[str]):
                The field name to transform
            assignments (Optional[List[RangePartition]], optional):
                The preassigned range partitions. Defaults to `None`.

        Returns:
            Transforms.RangeTransform: The created transform
        """
        return Transforms.RangeTransform(
            field=NamedReference.field(field_name=field_name),
            assignments=[] if assignments is None else assignments,
        )

    class IdentityTransform(SingleFieldTransform):
        """A transform that returns the input value."""

        def name(self) -> str:
            return Transforms.NAME_OF_IDENTITY

    class YearTransform(SingleFieldTransform):
        """A transform that returns the year of the input value."""

        def name(self) -> str:
            return Transforms.NAME_OF_YEAR

    class MonthTransform(SingleFieldTransform):
        """A transform that returns the month of the input value."""

        def name(self) -> str:
            return Transforms.NAME_OF_MONTH

    class DayTransform(SingleFieldTransform):
        """A transform that returns the day of the input value."""

        def name(self) -> str:
            return Transforms.NAME_OF_DAY

    class HourTransform(SingleFieldTransform):
        """A transform that returns the hour of the input value."""

        def name(self) -> str:
            return Transforms.NAME_OF_HOUR

    class BucketTransform(Transform):
        """A transform that returns the bucket of the input value."""

        def __init__(self, num_buckets: Literal[int], fields: List[NamedReference]):
            self.num_buckets_ = num_buckets
            self.fields = fields

        def name(self) -> str:
            return Transforms.NAME_OF_BUCKET

        def num_buckets(self) -> int:
            return self.num_buckets_.value()

        def field_names(self) -> List[List[str]]:
            return [field.field_name() for field in self.fields]

        def arguments(self) -> List[Expression]:
            return [self.num_buckets_, *self.fields]

        def __eq__(self, value: object) -> bool:
            if self is value:
                return True
            return (
                isinstance(value, Transforms.BucketTransform)
                and self.num_buckets_ == value.num_buckets_
                and self.fields == value.fields
            )

        def __hash__(self) -> int:
            return hash((self.num_buckets_, *self.fields))

    class TruncateTransform(Transform):
        """A transform that returns the truncated value of the input value with the given width."""

        def __init__(self, width: Literal[int], field: NamedReference):
            self.width_ = width
            self.field = field

        def name(self) -> str:
            return Transforms.NAME_OF_TRUNCATE

        def width(self) -> int:
            """Gets the width to truncate to.

            Returns:
                int: The width to truncate to.
            """

            return self.width_.value()

        def field_name(self) -> List[str]:
            """Gets the field name to transform.

            Returns:
                List[str]: The field name to transform.
            """

            return self.field.field_name()

        def arguments(self) -> List[Expression]:
            return [self.width_, self.field]

        def __eq__(self, value: object) -> bool:
            if self is value:
                return True
            return (
                isinstance(value, Transforms.TruncateTransform)
                and self.width_ == value.width_
                and self.field == value.field
            )

        def __hash__(self) -> int:
            return hash((self.width_, self.field))

    class ApplyTransform(Transform):
        """A transform that applies a function to the input value."""

        def __init__(self, name: str, arguments: List[Expression]):
            self.name_ = name
            self.arguments_ = arguments

        def name(self) -> str:
            return self.name_

        def arguments(self) -> List[Expression]:
            return self.arguments_

        def __eq__(self, value: object) -> bool:
            if self is value:
                return True
            return (
                isinstance(value, Transforms.ApplyTransform)
                and self.name_ == value.name_
                and self.arguments_ == value.arguments_
            )

        def __hash__(self) -> int:
            return 31 * hash(self.name_) + hash(tuple(self.arguments_))

    class ListTransform(Transform):
        """A transform that includes multiple fields in a list."""

        def __init__(
            self,
            fields: List[NamedReference],
            assignments: Optional[List[ListPartition]] = None,
        ):
            self._fields = fields
            self._assignments = [] if assignments is None else assignments

        def field_names(self) -> List[List[str]]:
            """Gets the field names to include in the list.

            Returns:
                List[List[str]]: The field names to include in the list.
            """
            return [field.field_name() for field in self._fields]

        def name(self) -> str:
            return Transforms.NAME_OF_LIST

        def arguments(self) -> List[Expression]:
            return self._fields

        def assignments(self) -> List[Partition]:
            return self._assignments

        def __eq__(self, value: object) -> bool:
            if not isinstance(value, Transforms.ListTransform):
                return False
            return self is value or self._fields == value.arguments()

        def __hash__(self) -> int:
            return hash(tuple(self._fields))

    class RangeTransform(Transform):
        """A transform that returns the range of the input value."""

        def __init__(
            self,
            field: NamedReference,
            assignments: Optional[List[RangePartition]] = None,
        ):
            self._field = field
            self._assignments = [] if assignments is None else assignments

        def name(self) -> str:
            return Transforms.NAME_OF_RANGE

        def field_name(self) -> List[str]:
            """Gets the field name to transform.

            Returns:
                List[str]: The field name to transform.
            """

            return self._field.field_name()

        def arguments(self) -> List[Expression]:
            return [self._field]

        def assignments(self) -> List[Partition]:
            return self._assignments

        def __eq__(self, value: object) -> bool:
            if not isinstance(value, Transforms.RangeTransform):
                return False
            return self is value or self.field_name() == value.field_name()

        def __hash__(self) -> int:
            return hash(self._field)
