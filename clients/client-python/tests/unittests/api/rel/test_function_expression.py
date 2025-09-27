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

import unittest

from gravitino.api.rel.expressions.expression import Expression
from gravitino.api.rel.expressions.function_expression import (
    FuncExpressionImpl,
    FunctionExpression,
)


class MockExpression(Expression):
    """Mock implementation of the Expression class for testing."""

    def children(self):
        return []

    def references(self):
        return []

    def __str__(self):
        return "MockExpression()"


class TestFunctionExpression(unittest.TestCase):
    def test_function_without_arguments(self):
        func = FuncExpressionImpl("SUM", [])
        self.assertEqual(func.function_name(), "SUM")
        self.assertEqual(func.arguments(), [])
        self.assertEqual(str(func), "SUM()")

    def test_function_with_arguments(self):
        arg1 = MockExpression()
        arg2 = MockExpression()
        func = FuncExpressionImpl("SUM", [arg1, arg2])
        self.assertEqual(func.function_name(), "SUM")
        self.assertEqual(func.arguments(), [arg1, arg2])
        self.assertEqual(str(func), "SUM(MockExpression(), MockExpression())")

    def test_function_equality(self):
        func1 = FuncExpressionImpl("SUM", [])
        func2 = FuncExpressionImpl("SUM", [])
        self.assertEqual(func1, func2)
        self.assertEqual(hash(func1), hash(func2))

    def test_function_of_static_method(self):
        func = FunctionExpression.of("SUM", MockExpression())
        self.assertEqual(func.function_name(), "SUM")
