from typing import List

from gravitino.api.expressions.expression import Expression
from gravitino.api.expressions.literals.literals import Literals
from gravitino.api.expressions.named_reference import NamedReference
from gravitino.api.expressions.partitions.partitions import (
    ListPartition,
    RangePartition,
)
from gravitino.api.expressions.transforms.transform import Transform


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
    def identity(field_name: List[str]) -> "IdentityTransform":
        """
        Create a transform that returns the input value.

        :param field_name: The field name(s) to transform.
                           Can be a list of field names or a single field name.
        :return: The created transform
        """
        # If a single column name is passed, convert it to a list.
        if isinstance(field_name, str):
            field_name = [field_name]

        return IdentityTransform(NamedReference.field(field_name))

    @staticmethod
    def year(field_name: List[str]) -> "YearTransform":
        """
        Create a transform that returns the year of the input value.

        :param field_name: The field name(s) to transform.
                           Can be a list of field names or a single field name.
        :return: The created transform
        """
        # If a single column name is passed, convert it to a list.
        if isinstance(field_name, str):
            field_name = [field_name]

        return YearTransform(NamedReference.field(field_name))

    @staticmethod
    def month(field_name: List[str]) -> "MonthTransform":
        """
        Create a transform that returns the month of the input value.

        :param field_name: The field name(s) to transform.
                           Can be a list of field names or a single field name.
        :return: The created transform
        """
        # If a single column name is passed, convert it to a list.
        if isinstance(field_name, str):
            field_name = [field_name]

        return MonthTransform(NamedReference.field(field_name))

    @staticmethod
    def day(field_name):
        """
        Create a transform that returns the day of the input value.

        :param field_name: The field name(s) to transform.
                           Can be a list of field names or a single column name.
        :return: The created transform
        """
        # If a single column name is passed, convert it to a list.
        if isinstance(field_name, str):
            field_name = [field_name]

        return DayTransform(NamedReference.field(field_name))

    @staticmethod
    def hour(field_name):
        """
        Create a transform that returns the hour of the input value.

        :param field_name: The field name(s) to transform.
                           Can be a list of field names or a single column name.
        :return: The created transform
        """
        # If a single column name is passed, convert it to a list.
        if isinstance(field_name, str):
            field_name = [field_name]

        return HourTransform(NamedReference.field(field_name))

    @staticmethod
    def bucket(num_buckets: int, *field_names: List[str]) -> "BucketTransform":
        """
        Create a transform that returns the bucket of the input value.

        :param num_buckets: The number of buckets to use
        :param field_names: The field names to transform
        :return: The created transform
        """
        fields = [NamedReference.field(fn) for fn in field_names]
        return BucketTransform(Literals.integer_literal(num_buckets), fields)

    @staticmethod
    def list(
        field_names: List[List[str]], assignments: List[ListPartition] = None
    ) -> "ListTransform":
        """
        Create a transform that includes multiple fields in a list.

        :param field_names: The field names to include in the list
        :param assignments: The preassigned list partitions (default is an empty list)
        :return: The created transform
        """
        if assignments is None:
            assignments = []
        # Convert the list of field names into NamedReference objects
        fields = [NamedReference.field(fn) for fn in field_names]

        return ListTransform(fields, assignments)

    @staticmethod
    def range(
        field_name: List[str], assignments: List[RangePartition] = None
    ) -> "RangeTransform":
        """
        Create a transform that returns the range of the input value.

        :param field_name: The field name to transform
        :param assignments: The preassigned range partitions (default is an empty list)
        :return: The created transform
        """
        if assignments is None:
            assignments = []
        return RangeTransform(NamedReference.field(field_name), assignments)

    @staticmethod
    def truncate(width: int, field_name) -> "TruncateTransform":
        """
        Create a transform that returns the truncated value of the input value with the given width.

        :param width: The width to truncate to
        :param field_name: The field name(s) to transform. Can be a list of field names or a single field name.
        :return: The created transform
        """
        # If a single column name is passed, convert it to a list.
        if isinstance(field_name, str):
            field_name = [field_name]

        return TruncateTransform(
            Literals.integer_literal(width), NamedReference.field(field_name)
        )

    @staticmethod
    def apply(name: str, *arguments: Expression) -> "ApplyTransform":
        """
        Create a transform that applies a function to the input value.

        :param name: The name of the function to apply
        :param arguments: The arguments to the function
        :return: The created transform
        """
        return ApplyTransform(name, arguments)


class IdentityTransform(Transforms):
    """A transform that returns the input value."""

    def __init__(self, ref: NamedReference):
        self.ref = ref

    def name(self) -> str:
        return Transforms.NAME_OF_IDENTITY

    def arguments(self) -> List[Expression]:
        return [self.ref]

    def __eq__(self, other):
        return isinstance(other, IdentityTransform) and self.ref == other.ref

    def __hash__(self):
        return hash(self.ref)


class YearTransform(Transforms):
    """A transform that returns the year of the input value."""

    def __init__(self, ref: NamedReference):
        self.ref = ref

    def name(self) -> str:
        return Transforms.NAME_OF_YEAR

    def children(self) -> List[Expression]:
        return [self.ref]

    def arguments(self) -> List[Expression]:
        return [self.ref]

    def __eq__(self, other):
        return isinstance(other, YearTransform) and self.ref == other.ref

    def __hash__(self):
        return hash(self.ref)


class MonthTransform(Transforms):
    """A transform that returns the month of the input value."""

    def __init__(self, ref: NamedReference):
        self.ref = ref

    def name(self) -> str:
        return Transforms.NAME_OF_MONTH

    def children(self) -> List[Expression]:
        return [self.ref]

    def arguments(self) -> List[Expression]:
        return [self.ref]

    def __eq__(self, other):
        return isinstance(other, MonthTransform) and self.ref == other.ref

    def __hash__(self):
        return hash(self.ref)


class DayTransform(Transforms):
    """A transform that returns the day of the input value."""

    def __init__(self, ref: NamedReference):
        self.ref = ref

    def name(self) -> str:
        return Transforms.NAME_OF_DAY

    def children(self) -> List[Expression]:
        return [self.ref]

    def arguments(self) -> List[Expression]:
        return [self.ref]

    def __eq__(self, other):
        return isinstance(other, DayTransform) and self.ref == other.ref

    def __hash__(self):
        return hash(self.ref)


class HourTransform(Transforms):
    """A transform that returns the hour of the input value."""

    def __init__(self, ref: NamedReference):
        self.ref = ref

    def name(self) -> str:
        return Transforms.NAME_OF_HOUR

    def children(self) -> List[Expression]:
        return [self.ref]

    def arguments(self) -> List[Expression]:
        return [self.ref]

    def __eq__(self, other):
        return isinstance(other, HourTransform) and self.ref == other.ref

    def __hash__(self):
        return hash(self.ref)


class BucketTransform(Transforms):
    """A transform that returns the bucket of the input value."""

    def __init__(self, num_buckets: int, fields: List[NamedReference]):
        self._num_buckets = num_buckets
        self.fields = fields

    @property
    def num_buckets(self) -> int:
        return self._num_buckets

    @property
    def field_names(self) -> List[str]:
        return [
            field_name for field in self.fields for field_name in field.field_name()
        ]

    def name(self) -> str:
        return Transforms.NAME_OF_BUCKET

    def arguments(self) -> List[Expression]:
        return [str(Literals.integer_literal(self.num_buckets))] + [
            field_name for field in self.fields for field_name in field.field_name()
        ]

    def __eq__(self, other):
        if not isinstance(other, BucketTransform):
            return False
        return (
            self.num_buckets == other.num_buckets
            and self.field_names == other.field_names
        )

    def __hash__(self):
        return hash((self.num_buckets, *(str(field) for field in self.fields)))


class TruncateTransform(Transforms):
    """A transform that returns the truncated value of the input value with the given width."""

    def __init__(self, width: int, field: NamedReference):
        self._width = width
        self.field = field

    @property
    def width(self) -> int:
        return self._width

    @property
    def field_name(self) -> List[str]:
        return self.field.field_name()

    def name(self) -> str:
        return Transforms.NAME_OF_TRUNCATE

    def arguments(self) -> List[Expression]:
        return [self.width, self.field]

    def __eq__(self, other):
        return (
            isinstance(other, TruncateTransform)
            and self.width == other.width
            and self.field == other.field
        )

    def __hash__(self):
        return hash((self.width, self.field))


class ListTransform(Transforms):
    """A transform that includes multiple fields in a list."""

    def __init__(
        self,
        fields: List[NamedReference],
        assignments: List[ListPartition] = None,
    ):
        if assignments is None:
            assignments = []
        self.fields = fields
        self.assignments = assignments

    @property
    def field_names(self) -> List[List[str]]:
        return [field.field_name() for field in self.fields]

    def name(self) -> str:
        return Transforms.NAME_OF_LIST

    def arguments(self) -> List["Expression"]:
        return self.fields

    def assignments(self) -> List["ListPartition"]:
        return self.assignments

    def __eq__(self, other):
        return isinstance(other, ListTransform) and self.fields == other.fields

    def __hash__(self):
        return hash(tuple(self.fields))


class RangeTransform(Transforms):
    """A transform that returns the range of the input value."""

    def __init__(self, field: NamedReference, assignments: List[RangePartition] = None):
        if assignments is None:
            assignments = []
        self.field = field
        self.assignments = assignments

    @property
    def field_name(self) -> List[str]:
        return self.field.field_name()

    def name(self) -> str:
        return Transforms.NAME_OF_RANGE

    def arguments(self) -> List[Expression]:
        return [self.field]

    def assignments(self) -> List[RangePartition]:
        return self.assignments

    def __eq__(self, other):
        return isinstance(other, RangeTransform) and self.field == other.field

    def __hash__(self):
        return hash(self.field)


class ApplyTransform(Transforms):
    """A transform that applies a function to the input value."""

    def __init__(self, name: str, arguments: List[Expression]):
        self._name = name
        self._arguments = list(arguments)

    def name(self) -> str:
        return self._name

    def arguments(self) -> List[Expression]:
        return self._arguments

    def __eq__(self, other):
        return (
            isinstance(other, ApplyTransform)
            and self.name() == other.name()
            and self.arguments() == other.arguments()
        )

    def __hash__(self):
        return hash((self.name(), tuple(self.arguments())))
