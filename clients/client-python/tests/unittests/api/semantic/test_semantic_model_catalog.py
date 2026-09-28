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
from typing import Optional

from gravitino.api.catalog import Catalog
from gravitino.api.semantic.dataset import Dataset
from gravitino.api.semantic.semantic_model import SemanticModel
from gravitino.api.semantic.semantic_model_catalog import SemanticModelCatalog
from gravitino.api.semantic.semantic_model_change import SemanticModelChange
from gravitino.api.semantic.semantic_model_definition import SemanticModelDefinition
from gravitino.dto.audit_dto import AuditDTO
from gravitino.exceptions.base import (
    NoSuchSemanticModelException,
    UnsupportedOperationException,
)
from gravitino.name_identifier import NameIdentifier
from gravitino.namespace import Namespace


def _definition() -> SemanticModelDefinition:
    return SemanticModelDefinition(
        datasets=[Dataset("orders", NameIdentifier.of("sales", "mart", "orders"))]
    )


class _InMemorySemanticModel(SemanticModel):
    def __init__(self, name: str, definition: SemanticModelDefinition):
        self._name = name
        self._definition = definition

    def name(self) -> str:
        return self._name

    def definition(self) -> SemanticModelDefinition:
        return self._definition

    def audit_info(self) -> AuditDTO:
        return AuditDTO(_creator="test")


class _InMemorySemanticModelCatalog(SemanticModelCatalog):
    def __init__(self):
        self._models = {}

    def list_semantic_models(self, namespace: Namespace) -> list[NameIdentifier]:
        return [
            NameIdentifier.of(namespace.level(0), name) for name in sorted(self._models)
        ]

    def load_semantic_model(self, identifier: NameIdentifier) -> SemanticModel:
        model = self._models.get(identifier.name())
        if model is None:
            raise NoSuchSemanticModelException(
                f"Semantic Model {identifier.name()} does not exist"
            )
        return model

    def create_semantic_model(
        self,
        identifier: NameIdentifier,
        comment: Optional[str],
        definition: SemanticModelDefinition,
        properties: Optional[dict[str, str]] = None,
    ) -> SemanticModel:
        model = _InMemorySemanticModel(identifier.name(), definition)
        self._models[identifier.name()] = model
        return model

    def alter_semantic_model(
        self, identifier: NameIdentifier, *changes: SemanticModelChange
    ) -> SemanticModel:
        return self.load_semantic_model(identifier)

    def drop_semantic_model(self, identifier: NameIdentifier) -> bool:
        return self._models.pop(identifier.name(), None) is not None


class TestSemanticModelCatalog(unittest.TestCase):
    def test_semantic_model_exists_uses_load(self):
        catalog = _InMemorySemanticModelCatalog()
        identifier = NameIdentifier.of("semantic", "sales_model")

        self.assertFalse(catalog.semantic_model_exists(identifier))

        catalog.create_semantic_model(identifier, "comment", _definition(), {})

        self.assertTrue(catalog.semantic_model_exists(identifier))
        self.assertTrue(catalog.drop_semantic_model(identifier))
        self.assertFalse(catalog.semantic_model_exists(identifier))

    def test_semantic_model_defaults(self):
        model = _InMemorySemanticModel("sales_model", _definition())

        self.assertEqual("sales_model", model.name())
        self.assertIsNone(model.comment())
        self.assertEqual({}, model.properties())
        self.assertEqual(_definition(), model.definition())

    def test_catalog_does_not_support_semantic_models_by_default(self):
        with self.assertRaisesRegex(
            UnsupportedOperationException,
            "Catalog does not support semantic model operations",
        ):
            Catalog.as_semantic_model_catalog(None)
