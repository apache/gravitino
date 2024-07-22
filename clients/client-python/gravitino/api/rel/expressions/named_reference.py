from abc import ABC, abstractmethod
from typing import List, Tuple
from gravitino.api.rel.expressions.expression import Expression

class NamedReference(Expression, ABC):
    """
    Represents a field or column reference in the public logical expression API.
    """

    @staticmethod
    def field(*field_names: str) -> 'FieldReference':
        """
        Returns a FieldReference for the given field name(s). The input can be multiple strings
        for nested fields.
        """
        return FieldReference(field_names)

    @abstractmethod
    def field_name(self) -> Tuple[str, ...]:
        """
        Returns the referenced field name as a tuple of string parts.
        Each string in the tuple represents a part of the field name.
        """
        pass

    def children(self) -> List[Expression]:
        """
        Override from Expression, field references have no children.
        """
        return self.EMPTY_EXPRESSION

    def references(self) -> List['NamedReference']:
        """
        By default, a NamedReference references itself.
        """
        return [self]

class FieldReference(NamedReference):
    def __init__(self, field_name: Tuple[str, ...]):
        self._field_name = field_name

    def field_name(self) -> Tuple[str, ...]:
        return self._field_name

    def __eq__(self, other):
        if isinstance(other, FieldReference):
            return self._field_name == other._field_name
        return False

    def __hash__(self):
        return hash(self._field_name)

    def __str__(self):
        return ".".join(self._field_name)
