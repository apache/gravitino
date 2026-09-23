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
from gravitino.api.semantic.ai_context_object import AIContextObject
from gravitino.api.semantic.dataset import Dataset
from gravitino.api.semantic.relationship import Relationship
from gravitino.api.semantic.semantic_model_change import SemanticModelChange
from gravitino.api.semantic.semantic_model_definition import SemanticModelDefinition
from gravitino.exceptions.base import IllegalArgumentException
from gravitino.name_identifier import NameIdentifier


class TestSemanticModelValueSemantics(unittest.TestCase):
    def test_source_copies_on_input_and_output(self):
        for mutate_input in (True, False):
            with self.subTest(mutate_input=mutate_input):
                source = NameIdentifier.of("catalog", "schema", "orders")
                dataset = Dataset("orders", source)
                definition = SemanticModelDefinition([dataset])
                expected = SemanticModelDefinition(
                    [
                        Dataset(
                            "orders", NameIdentifier.of("catalog", "schema", "orders")
                        )
                    ]
                )
                original_hash = hash(definition)
                models = {definition: "saved"}
                exposed = source if mutate_input else dataset.source()
                exposed.namespace().levels()[0] = "other"
                exposed._name = "changed"  # pylint: disable=protected-access
                self.assertEqual(expected, definition)
                self.assertEqual(original_hash, hash(definition))
                self.assertEqual("saved", models[expected])

    def test_json_boolean_and_number_are_distinct_recursively(self):
        for boolean, number in ((True, 1), (False, 0), (True, 1.0)):
            for wrap in (lambda x: x, lambda x: [x], lambda x: {"nested": [x]}):
                with self.subTest(boolean=boolean, number=number, wrap=wrap):
                    left = AIContextObject(
                        additional_properties={"value": wrap(boolean)}
                    )
                    right = AIContextObject(
                        additional_properties={"value": wrap(number)}
                    )
                    self.assertNotEqual(left, right)
                    dataset = Dataset(
                        "orders", NameIdentifier.of("catalog", "schema", "orders")
                    )
                    self.assertNotEqual(
                        SemanticModelDefinition(
                            [dataset], ai_context=AIContext.of(left)
                        ),
                        SemanticModelDefinition(
                            [dataset], ai_context=AIContext.of(right)
                        ),
                    )

    def test_json_hash_includes_values_and_ignores_object_order(self):
        left = AIContextObject(additional_properties={"a": [1, {"b": True}], "z": None})
        right = AIContextObject(
            additional_properties={"z": None, "a": (1, {"b": True})}
        )
        self.assertEqual(left, right)
        self.assertEqual(hash(left), hash(right))
        hashes = {
            hash(AIContextObject(additional_properties={"value": n}))
            for n in range(100)
        }
        self.assertGreater(len(hashes), 90)
        self.assertNotEqual(
            AIContextObject(additional_properties={"value": []}),
            AIContextObject(additional_properties={"value": {}}),
        )

    def test_ai_context_rejects_non_string_elements(self):
        for field in ("synonyms", "examples"):
            for value in (1, True, {}, [], {"term": "sales"}, None):
                with self.subTest(field=field, value=value):
                    with self.assertRaises(IllegalArgumentException):
                        AIContextObject(**{field: [value]})
            context = AIContextObject(**{field: [""]})
            self.assertEqual([""], getattr(context, field)())
            hash(context)

    def test_keys_and_relationship_columns_reject_non_strings(self):
        source = NameIdentifier.of("catalog", "schema", "orders")
        for value in (1, True, {}, []):
            for build in (
                lambda value=value: Dataset("orders", source, primary_key=[value]),
                lambda value=value: Dataset("orders", source, unique_keys=[[value]]),
                lambda value=value: Relationship(
                    "r", "orders", "customers", [value], ["id"]
                ),
                lambda value=value: Relationship(
                    "r", "orders", "customers", ["id"], [value]
                ),
            ):
                with self.subTest(value=value, build=build):
                    with self.assertRaises(IllegalArgumentException):
                        build()

    def test_clear_comment_display(self):
        self.assertEqual(
            "UPDATECOMMENT null", str(SemanticModelChange.update_comment(None))
        )
