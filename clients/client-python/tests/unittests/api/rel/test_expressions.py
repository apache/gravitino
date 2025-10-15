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
from typing import List

from gravitino.api.rel.expressions.expression import Expression
from gravitino.api.rel.expressions.named_reference import NamedReference


class MockExpression(Expression):
    """Mock implementation of the Expression class for testing."""

    def __init__(
        self, children: List[Expression] = None, references: List[NamedReference] = None
    ):
        self._children = children if children else []
        self._references = references if references else []

    def children(self) -> List[Expression]:
        return self._children

    def references(self) -> List[NamedReference]:
        if self._references:
            return self._references
        return super().references()


class TestExpression(unittest.TestCase):
    def test_empty_expression(self):
        expr = MockExpression()
        self.assertEqual(expr.children(), [])
        self.assertEqual(expr.references(), [])

    def test_expression_with_references(self):
        ref = NamedReference.field(["student", "name"])
        child = MockExpression(references=[ref])
        expr = MockExpression(children=[child])
        self.assertEqual(expr.children(), [child])
        self.assertEqual(expr.references(), [ref])

    def test_multiple_children(self):
        ref1 = NamedReference.field(["student", "name"])
        ref2 = NamedReference.field(["student", "age"])
        child1 = MockExpression(references=[ref1])
        child2 = MockExpression(references=[ref2])
        expr = MockExpression(children=[child1, child2])
        self.assertCountEqual(expr.references(), [ref1, ref2])
