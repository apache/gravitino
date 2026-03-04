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

from gravitino.api.rel.expressions.unparsed_expression import UnparsedExpressionImpl


class TestUnparsedExpression(unittest.TestCase):
    def test_unparsed_expression_creation(self):
        expr = UnparsedExpressionImpl("some_expression")
        self.assertEqual(expr.unparsed_expression(), "some_expression")
        self.assertEqual(
            str(expr), "UnparsedExpressionImpl{unparsedExpression='some_expression'}"
        )

    def test_unparsed_expression_equality(self):
        expr1 = UnparsedExpressionImpl("some_expression")
        expr2 = UnparsedExpressionImpl("some_expression")
        self.assertEqual(expr1, expr2)
        self.assertEqual(hash(expr1), hash(expr2))
