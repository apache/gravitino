import unittest
from unittest.mock import Mock

from gravitino.api.column import Column, ColumnImpl
from gravitino.api.expressions.expression import Expression
from gravitino.api.expressions.function_expression import FunctionExpression
from gravitino.api.types.type import Type
from gravitino.exceptions.base import (
    IllegalArgumentException,
    UnsupportedOperationException,
)


class TestColumn(unittest.TestCase):
    def setUp(self):
        # Create mock Type for testing
        self.mock_type = Mock(spec=Type)

    def test_column_factory_method(self):
        """Test the Column.of() factory method."""

        column = Column.of("test_column", self.mock_type)

        self.assertIsInstance(column, ColumnImpl)
        self.assertEqual("test_column", column.name())
        self.assertEqual(self.mock_type, column.data_type())
        self.assertIsNone(column.comment())
        self.assertTrue(column.nullable())
        self.assertFalse(column.auto_increment())
        self.assertEqual(Column.DEFAULT_VALUE_NOT_SET, column.default_value())

    def test_column_factory_with_all_params(self):
        """Test the Column.of() factory method with all parameters."""

        default_value = Mock(spec=Expression)
        column = Column.of(
            name="test_column",
            data_type=self.mock_type,
            comment="Test comment",
            nullable=False,
            auto_increment=True,
            default_value=default_value,
        )

        self.assertEqual("test_column", column.name())
        self.assertEqual(self.mock_type, column.data_type())
        self.assertEqual("Test comment", column.comment())
        self.assertFalse(column.nullable())
        self.assertTrue(column.auto_increment())
        self.assertEqual(default_value, column.default_value())

    def test_column_equality(self):
        """Test equality comparison."""
        default_value = Mock(spec=Expression)

        col1 = Column.of("test", self.mock_type, "comment", False, True, default_value)
        col2 = Column.of("test", self.mock_type, "comment", False, True, default_value)
        col3 = Column.of("different", self.mock_type)

        self.assertEqual(col1, col2)
        self.assertNotEqual(col1, col3)
        self.assertNotEqual(col1, "not_a_column")

    def test_column_hash(self):
        """Test hash implementation.

        Same columns should have same hash.
        """
        col1 = Column.of("test", self.mock_type, "comment", False, True)
        col2 = Column.of("test", self.mock_type, "comment", False, True)
        col3 = Column.of("different", self.mock_type)

        self.assertEqual(hash(col1), hash(col2))
        self.assertNotEqual(hash(col1), hash(col3))

    def test_supports_tags_raises_exception(self):
        """Test that supports_tags raises `UnsupportedOperationException`."""

        column = Column.of("test", self.mock_type)

        with self.assertRaises(UnsupportedOperationException):
            column.supports_tags()

    def test_default_value_constants(self):
        """Test default value constants."""

        self.assertEqual(Expression.EMPTY_EXPRESSION, Column.DEFAULT_VALUE_NOT_SET)
        self.assertIsInstance(
            Column.DEFAULT_VALUE_OF_CURRENT_TIMESTAMP, FunctionExpression
        )

    def test_empty_name_validation(self):
        """Test validation for empty name to raise `IllegalArgumentException`."""

        with self.assertRaises(IllegalArgumentException):
            Column.of("", self.mock_type)

        with self.assertRaises(IllegalArgumentException):
            Column.of("   ", self.mock_type)

    def test_none_data_type_validation(self):
        """Test validation for None data type to raise `IllegalArgumentException`."""

        with self.assertRaises(IllegalArgumentException):
            Column.of("test", None)
