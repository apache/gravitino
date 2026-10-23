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

from gravitino.api.semantic.dataset import Dataset
from gravitino.api.semantic.semantic_model_change import (
    RemoveProperty,
    RenameSemanticModel,
    ReplaceDefinition,
    SemanticModelChange,
    SetProperty,
    UpdateComment,
)
from gravitino.api.semantic.semantic_model_definition import SemanticModelDefinition
from gravitino.name_identifier import NameIdentifier


def _definition(dataset_name: str = "orders") -> SemanticModelDefinition:
    return SemanticModelDefinition(
        datasets=[
            Dataset(dataset_name, NameIdentifier.of("sales", "mart", dataset_name))
        ]
    )


class TestSemanticModelChange(unittest.TestCase):
    def test_rename(self):
        change = SemanticModelChange.rename("new_model")
        equal_change = SemanticModelChange.rename("new_model")

        self.assertIsInstance(change, RenameSemanticModel)
        self.assertEqual("new_model", change.new_name())
        self.assertEqual("RENAMESEMANTICMODEL new_model", str(change))
        self.assertEqual(equal_change, change)
        self.assertEqual(hash(equal_change), hash(change))
        self.assertNotEqual(SemanticModelChange.rename("other_model"), change)
        self.assertNotEqual(change, "invalid_change")

        with self.assertRaisesRegex(ValueError, "New name must not be null or blank"):
            SemanticModelChange.rename("")

    def test_update_comment(self):
        change = SemanticModelChange.update_comment("Updated sales definitions")
        equal_change = SemanticModelChange.update_comment("Updated sales definitions")

        self.assertIsInstance(change, UpdateComment)
        self.assertEqual("Updated sales definitions", change.new_comment())
        self.assertEqual("UPDATECOMMENT Updated sales definitions", str(change))
        self.assertEqual(equal_change, change)
        self.assertEqual(hash(equal_change), hash(change))
        self.assertNotEqual(change, "invalid_change")

    def test_update_comment_clears_the_comment(self):
        change = SemanticModelChange.update_comment(None)

        self.assertIsNone(change.new_comment())
        self.assertNotEqual(SemanticModelChange.update_comment(""), change)

    def test_set_property(self):
        change = SemanticModelChange.set_property("key", "value")
        equal_change = SemanticModelChange.set_property("key", "value")

        self.assertIsInstance(change, SetProperty)
        self.assertEqual("key", change.property())
        self.assertEqual("value", change.value())
        self.assertEqual("SETPROPERTY key value", str(change))
        self.assertEqual(equal_change, change)
        self.assertEqual(hash(equal_change), hash(change))
        self.assertNotEqual(SemanticModelChange.set_property("key", "other"), change)
        self.assertNotEqual(change, "invalid_change")

        with self.assertRaisesRegex(
            ValueError, "Property name must not be null or blank"
        ):
            SemanticModelChange.set_property("", "value")
        with self.assertRaisesRegex(ValueError, "Property value must not be null"):
            SemanticModelChange.set_property("key", None)

    def test_remove_property(self):
        change = SemanticModelChange.remove_property("key")
        equal_change = SemanticModelChange.remove_property("key")

        self.assertIsInstance(change, RemoveProperty)
        self.assertEqual("key", change.property())
        self.assertEqual("REMOVEPROPERTY key", str(change))
        self.assertEqual(equal_change, change)
        self.assertEqual(hash(equal_change), hash(change))
        self.assertNotEqual(change, "invalid_change")

        with self.assertRaisesRegex(
            ValueError, "Property name must not be null or blank"
        ):
            SemanticModelChange.remove_property("")

    def test_replace_definition(self):
        definition = _definition()
        change = SemanticModelChange.replace_definition(definition)
        equal_change = SemanticModelChange.replace_definition(_definition())

        self.assertIsInstance(change, ReplaceDefinition)
        self.assertEqual(definition, change.definition())
        self.assertEqual(f"REPLACEDEFINITION {definition}", str(change))
        self.assertEqual(equal_change, change)
        self.assertEqual(hash(equal_change), hash(change))
        self.assertNotEqual(
            SemanticModelChange.replace_definition(_definition("customers")), change
        )
        self.assertNotEqual(change, "invalid_change")

        with self.assertRaisesRegex(ValueError, "Definition must not be null"):
            SemanticModelChange.replace_definition(None)
