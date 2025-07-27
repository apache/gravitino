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

from typing import List, Union, overload

from gravitino.api.expressions.named_reference import NamedReference
from gravitino.api.expressions.transforms.transform import (
    SingleFieldTransform,
    Transform,
)


class Transforms(Transform):
    """Helper methods to create logical transforms to pass into Apache Gravitino."""

    EMPTY_TRANSFORM: List[Transform] = []
    """An empty array of transforms."""
    NAME_OF_IDENTITY: str = "identity"
    """The name of the identity transform."""
    NAME_OF_YEAR: str = "year"
    """The name of the year transform. The year transform returns the year of the input value."""
    NAME_OF_MONTH: str = "month"
    """The name of the month transform. The month transform returns the month of the input value."""
    NAME_OF_DAY: str = "day"
    """The name of the day transform. The day transform returns the day of the input value."""
    NAME_OF_HOUR: str = "hour"
    """The name of the hour transform. The hour transform returns the hour of the input value."""
    NAME_OF_BUCKET: str = "bucket"
    """The name of the bucket transform. The bucket transform returns the bucket of the input value."""
    NAME_OF_TRUNCATE: str = "truncate"
    """The name of the truncate transform. The truncate transform returns the truncated value of the"""
    NAME_OF_LIST: str = "list"
    """The name of the list transform. The list transform includes multiple fields in a list."""
    NAME_OF_RANGE: str = "range"
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

    class IdentityTransform(SingleFieldTransform):
        """A transform that returns the input value."""

        def name(self) -> str:
            return Transforms.NAME_OF_IDENTITY

        def __eq__(self, other):
            return (
                isinstance(other, Transforms.IdentityTransform)
                and self.ref == other.ref
            )

        def __hash__(self):
            return hash(self.ref)

    class YearTransform(SingleFieldTransform):
        """A transform that returns the year of the input value."""

        def name(self) -> str:
            return Transforms.NAME_OF_YEAR

        def __eq__(self, other):
            return isinstance(other, Transforms.YearTransform) and self.ref == other.ref

        def __hash__(self):
            return hash(self.ref)

    class MonthTransform(SingleFieldTransform):
        """A transform that returns the month of the input value."""

        def name(self) -> str:
            return Transforms.NAME_OF_MONTH

        def __eq__(self, other):
            return (
                isinstance(other, Transforms.MonthTransform) and self.ref == other.ref
            )

        def __hash__(self):
            return hash(self.ref)

    class DayTransform(SingleFieldTransform):
        """A transform that returns the day of the input value."""

        def name(self) -> str:
            return Transforms.NAME_OF_DAY

        def __eq__(self, other):
            return isinstance(other, Transforms.DayTransform) and self.ref == other.ref

        def __hash__(self):
            return hash(self.ref)

    class HourTransform(SingleFieldTransform):
        """A transform that returns the hour of the input value."""

        def name(self) -> str:
            return Transforms.NAME_OF_HOUR

        def __eq__(self, other):
            return isinstance(other, Transforms.HourTransform) and self.ref == other.ref

        def __hash__(self):
            return hash(self.ref)
