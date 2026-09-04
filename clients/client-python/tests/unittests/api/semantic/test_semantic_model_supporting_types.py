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

from gravitino.api.semantic.ai_context import AIContext
from gravitino.api.semantic.ai_context_object import (
    MAX_ADDITIONAL_PROPERTY_NESTING_DEPTH,
    AIContextObject,
)
from gravitino.api.semantic.custom_extension import CustomExtension
from gravitino.api.semantic.data_type import DataType
from gravitino.api.semantic.dialect_expression import DialectExpression
from gravitino.api.semantic.dialects import Dialects
from gravitino.api.semantic.dimension import Dimension
from gravitino.api.semantic.expression import Expression


class TestSemanticModelSupportingTypes(unittest.TestCase):
    def test_data_type_uses_ossie_wire_values(self):
        self.assertEqual("Decimal", DataType.DECIMAL.value)
        self.assertEqual("DateTime", DataType.DATE_TIME.value)
        self.assertEqual("DateTimeTz", DataType.DATE_TIME_TZ.value)
        self.assertEqual(DataType.OPAQUE, DataType("Opaque"))

    def test_dialects_are_case_sensitive_constants(self):
        self.assertEqual("ANSI_SQL", Dialects.ANSI_SQL)
        self.assertEqual("BIGQUERY", Dialects.BIGQUERY)
        with self.assertRaises(TypeError):
            Dialects()

    def test_dialect_expression(self):
        dialect_expression = DialectExpression(Dialects.ANSI_SQL, "order_amount")
        equal_expression = DialectExpression(Dialects.ANSI_SQL, "order_amount")

        self.assertEqual(Dialects.ANSI_SQL, dialect_expression.dialect())
        self.assertEqual("order_amount", dialect_expression.expression())
        self.assertEqual(dialect_expression, equal_expression)
        self.assertEqual(hash(dialect_expression), hash(equal_expression))
        self.assertNotEqual(dialect_expression, "invalid")
        self.assertNotEqual(
            dialect_expression, DialectExpression("ansi_sql", "order_amount")
        )

    def test_dialect_expression_rejects_empty_fields(self):
        with self.assertRaisesRegex(ValueError, "dialect must not be null or empty"):
            DialectExpression("", "order_amount")
        with self.assertRaisesRegex(ValueError, "expression must not be null or empty"):
            DialectExpression(Dialects.ANSI_SQL, "")

    def test_expression_preserves_dialect_order(self):
        ansi = DialectExpression(Dialects.ANSI_SQL, "order_amount")
        snowflake = DialectExpression(Dialects.SNOWFLAKE, "ORDER_AMOUNT")
        expression = Expression([ansi, snowflake])

        self.assertEqual([ansi, snowflake], expression.dialects())
        self.assertEqual(Expression([ansi, snowflake]), expression)
        self.assertNotEqual(Expression([snowflake, ansi]), expression)

    def test_expression_returns_a_copy_of_its_dialects(self):
        ansi = DialectExpression(Dialects.ANSI_SQL, "order_amount")
        expression = Expression([ansi])

        expression.dialects().clear()

        self.assertEqual([ansi], expression.dialects())

    def test_expression_rejects_invalid_dialects(self):
        ansi = DialectExpression(Dialects.ANSI_SQL, "order_amount")

        with self.assertRaisesRegex(ValueError, "dialects must not be null or empty"):
            Expression([])
        with self.assertRaisesRegex(ValueError, r"dialects\[0\] must not be null"):
            Expression([None])
        with self.assertRaisesRegex(
            ValueError, "dialects must not contain duplicate dialect: ANSI_SQL"
        ):
            Expression([ansi, DialectExpression(Dialects.ANSI_SQL, "amount")])

    def test_dimension(self):
        self.assertIsNone(Dimension().is_time())
        self.assertTrue(Dimension(is_time=True).is_time())
        self.assertEqual(Dimension(True), Dimension(True))
        self.assertEqual(hash(Dimension(True)), hash(Dimension(True)))
        self.assertNotEqual(Dimension(True), Dimension(False))
        self.assertNotEqual(Dimension(True), "invalid")

    def test_custom_extension(self):
        extension = CustomExtension("acme", '{"unit": "usd"}')

        self.assertEqual("acme", extension.vendor_name())
        self.assertEqual('{"unit": "usd"}', extension.data())
        self.assertEqual(CustomExtension("acme", '{"unit": "usd"}'), extension)
        self.assertNotEqual(CustomExtension("other", '{"unit": "usd"}'), extension)
        self.assertNotEqual(extension, "invalid")

    def test_custom_extension_rejects_missing_fields(self):
        with self.assertRaisesRegex(ValueError, "vendorName must not be null"):
            CustomExtension(None, "data")
        with self.assertRaisesRegex(ValueError, "data must not be null"):
            CustomExtension("acme", None)

    def test_ai_context_holds_text(self):
        ai_context = AIContext.of("Use certified metrics only")

        self.assertTrue(ai_context.is_text())
        self.assertEqual("Use certified metrics only", ai_context.text())
        self.assertIsNone(ai_context.object())
        self.assertEqual(AIContext.of("Use certified metrics only"), ai_context)
        self.assertEqual(
            hash(AIContext.of("Use certified metrics only")), hash(ai_context)
        )
        self.assertNotEqual(ai_context, "invalid")

    def test_ai_context_holds_object(self):
        ai_context_object = AIContextObject(instructions="Use certified metrics only")
        ai_context = AIContext.of(ai_context_object)

        self.assertFalse(ai_context.is_text())
        self.assertIsNone(ai_context.text())
        self.assertEqual(ai_context_object, ai_context.object())

    def test_ai_context_rejects_unsupported_values(self):
        with self.assertRaisesRegex(
            ValueError, "AI context must be a string or an AIContextObject"
        ):
            AIContext.of(None)
        with self.assertRaisesRegex(
            ValueError, "AI context must be a string or an AIContextObject"
        ):
            AIContext.of(42)
        with self.assertRaisesRegex(
            ValueError, "AI context must contain exactly one of text or object"
        ):
            AIContext(None, None)
        with self.assertRaisesRegex(
            ValueError, "AI context must contain exactly one of text or object"
        ):
            AIContext("text", AIContextObject())

    def test_ai_context_object_defaults(self):
        ai_context_object = AIContextObject()

        self.assertIsNone(ai_context_object.instructions())
        self.assertIsNone(ai_context_object.synonyms())
        self.assertIsNone(ai_context_object.examples())
        self.assertEqual({}, ai_context_object.additional_properties())

    def test_ai_context_object_retains_additional_properties(self):
        ai_context_object = AIContextObject(
            instructions="Use certified metrics only",
            synonyms=["sales"],
            examples=["total revenue by month"],
            additional_properties={
                "audience": "finance",
                "thresholds": {"warn": 10, "limits": [1, 2.5, True, None]},
            },
        )

        self.assertEqual("Use certified metrics only", ai_context_object.instructions())
        self.assertEqual(["sales"], ai_context_object.synonyms())
        self.assertEqual(["total revenue by month"], ai_context_object.examples())
        self.assertEqual(
            {
                "audience": "finance",
                "thresholds": {"warn": 10, "limits": [1, 2.5, True, None]},
            },
            ai_context_object.additional_properties(),
        )

    def test_ai_context_object_returns_copies(self):
        synonyms = ["sales"]
        additional_properties = {"nested": {"key": "value"}}
        ai_context_object = AIContextObject(
            synonyms=synonyms, additional_properties=additional_properties
        )

        synonyms.append("revenue")
        additional_properties["nested"]["key"] = "mutated"
        ai_context_object.synonyms().clear()
        ai_context_object.additional_properties()["nested"]["key"] = "mutated"

        self.assertEqual(["sales"], ai_context_object.synonyms())
        self.assertEqual(
            {"nested": {"key": "value"}}, ai_context_object.additional_properties()
        )

    def test_ai_context_object_equality(self):
        ai_context_object = AIContextObject(
            instructions="instructions", additional_properties={"audience": "finance"}
        )
        equal_object = AIContextObject(
            instructions="instructions", additional_properties={"audience": "finance"}
        )

        self.assertEqual(equal_object, ai_context_object)
        self.assertEqual(hash(equal_object), hash(ai_context_object))
        self.assertNotEqual(AIContextObject(instructions="other"), ai_context_object)
        self.assertNotEqual(ai_context_object, "invalid")

    def test_ai_context_object_rejects_invalid_additional_properties(self):
        with self.assertRaisesRegex(
            ValueError,
            "additional property must not duplicate standard property: synonyms",
        ):
            AIContextObject(additional_properties={"synonyms": ["sales"]})

        with self.assertRaisesRegex(
            ValueError, "additional property name must be a string"
        ):
            AIContextObject(additional_properties={1: "value"})

        with self.assertRaisesRegex(
            ValueError,
            "Additional property audience has non-JSON-compatible value type: object",
        ):
            AIContextObject(additional_properties={"audience": object()})

        with self.assertRaisesRegex(
            ValueError, "Additional property ratio must contain a finite number"
        ):
            AIContextObject(additional_properties={"ratio": float("nan")})

        with self.assertRaisesRegex(
            ValueError,
            r"Additional property nested contains a map key that is not a string",
        ):
            AIContextObject(additional_properties={"nested": {1: "value"}})

    def test_ai_context_object_rejects_cyclic_additional_properties(self):
        cyclic = {}
        cyclic["self"] = cyclic

        with self.assertRaisesRegex(
            ValueError, r"Additional property nested\.self contains a cyclic value"
        ):
            AIContextObject(additional_properties={"nested": cyclic})

    def test_ai_context_object_rejects_deeply_nested_additional_properties(self):
        value = "leaf"
        for _ in range(MAX_ADDITIONAL_PROPERTY_NESTING_DEPTH + 1):
            value = [value]

        with self.assertRaisesRegex(ValueError, "exceeds maximum nesting depth of 100"):
            AIContextObject(additional_properties={"nested": value})

    def test_ai_context_object_rejects_none_elements(self):
        with self.assertRaisesRegex(ValueError, r"synonyms\[1\] must not be null"):
            AIContextObject(synonyms=["sales", None])
        with self.assertRaisesRegex(ValueError, r"examples\[0\] must not be null"):
            AIContextObject(examples=[None])
