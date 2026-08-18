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

from gravitino.api.rel.dialects import Dialects
from gravitino.api.rel.sql_representation import SQLRepresentation
from gravitino.api.rel.types.types import Types
from gravitino.dto.audit_dto import AuditDTO
from gravitino.dto.rel.column_dto import ColumnDTO
from gravitino.dto.rel.sql_representation_dto import SQLRepresentationDTO
from gravitino.dto.rel.view_dto import ViewDTO


class TestViewDTO(unittest.TestCase):
    @staticmethod
    def _column(name: str = "id") -> ColumnDTO:
        return ColumnDTO(
            _name=name,
            _data_type=Types.IntegerType.get(),
            _comment="column comment",
            _nullable=False,
        )

    @staticmethod
    def _representation(
        dialect: str = Dialects.TRINO, sql: str = "SELECT id FROM table"
    ) -> SQLRepresentationDTO:
        return SQLRepresentationDTO(_dialect=dialect, _sql=sql)

    def _view_dto(
        self,
        name: str = "test_view",
        columns: list[ColumnDTO] = None,
        representations: list[SQLRepresentationDTO] = None,
        comment: str = "view comment",
        properties: dict[str, str] = None,
    ) -> ViewDTO:
        return ViewDTO(
            _name=name,
            _columns=columns if columns is not None else [self._column()],
            _representations=(
                representations
                if representations is not None
                else [self._representation()]
            ),
            _comment=comment,
            _default_catalog="catalog",
            _default_schema="schema",
            _properties=properties if properties is not None else {"k1": "v1"},
            _audit=AuditDTO(
                "creator", "2022-01-01T00:00:00Z", "modifier", "2022-01-02T00:00:00Z"
            ),
        )

    def test_view_dto_serialization(self):
        view = self._view_dto()

        deserialized = ViewDTO.from_json(view.to_json())

        self.assertEqual(view, deserialized)
        self.assertEqual("test_view", deserialized.name())
        self.assertEqual("view comment", deserialized.comment())
        self.assertEqual("catalog", deserialized.default_catalog())
        self.assertEqual("schema", deserialized.default_schema())
        self.assertEqual({"k1": "v1"}, deserialized.properties())
        self.assertEqual("creator", deserialized.audit_info().creator())
        self.assertEqual(1, len(deserialized.columns()))
        self.assertEqual(1, len(deserialized.representations()))
        self.assertIsInstance(deserialized.representations()[0], SQLRepresentation)
        self.assertEqual(
            "SELECT id FROM table",
            deserialized.representations()[0].sql(),
        )

    def test_view_dto_deserialize_from_raw_json(self):
        raw_json = """
        {
            "name": "raw_view",
            "comment": "raw comment",
            "columns": [
                {
                    "name": "id",
                    "type": "integer",
                    "comment": "id column",
                    "nullable": false,
                    "autoIncrement": false
                }
            ],
            "representations": [
                {
                    "type": "sql",
                    "dialect": "spark",
                    "sql": "SELECT id FROM table"
                }
            ],
            "defaultCatalog": "catalog",
            "defaultSchema": "schema",
            "properties": {
                "k1": "v1"
            },
            "audit": {
                "creator": "creator"
            }
        }
        """

        view = ViewDTO.from_json(raw_json)

        self.assertEqual("raw_view", view.name())
        self.assertEqual("raw comment", view.comment())
        self.assertEqual("catalog", view.default_catalog())
        self.assertEqual("schema", view.default_schema())
        self.assertEqual({"k1": "v1"}, view.properties())
        self.assertEqual("creator", view.audit_info().creator())
        self.assertEqual(1, len(view.columns()))
        self.assertEqual("id", view.columns()[0].name())
        self.assertEqual(1, len(view.representations()))
        self.assertIsInstance(view.representations()[0], SQLRepresentation)
        self.assertEqual("spark", view.representations()[0].dialect())
        self.assertEqual("SELECT id FROM table", view.representations()[0].sql())

    def test_view_dto_defaults(self):
        view = ViewDTO(
            _name="test_view",
            _representations=[self._representation()],
            _audit=AuditDTO("creator"),
        )

        self.assertEqual([], view.columns())
        self.assertEqual([], view.to_dict()["columns"])
        self.assertEqual({}, view.properties())
        self.assertIsNone(view.comment())
        self.assertIsNone(view.default_catalog())
        self.assertIsNone(view.default_schema())

    def test_view_dto_deserialize_invalid_name(self):
        raw_json = """
        {
            "name": "",
            "representations": [
                {
                    "type": "sql",
                    "dialect": "spark",
                    "sql": "SELECT 1"
                }
            ],
            "audit": {
                "creator": "creator"
            }
        }
        """

        with self.assertRaisesRegex(ValueError, "name cannot be null or empty"):
            ViewDTO.from_json(raw_json)

    def test_view_dto_deserialize_missing_audit(self):
        raw_json = """
        {
            "name": "test_view",
            "representations": [
                {
                    "type": "sql",
                    "dialect": "spark",
                    "sql": "SELECT 1"
                }
            ]
        }
        """

        with self.assertRaisesRegex(ValueError, "audit cannot be null"):
            ViewDTO.from_json(raw_json)

    def test_view_dto_deserialize_missing_representations(self):
        raw_json = """
        {
            "name": "test_view",
            "audit": {
                "creator": "creator"
            }
        }
        """

        with self.assertRaisesRegex(
            ValueError, "representations cannot be null or empty"
        ):
            ViewDTO.from_json(raw_json)

    def test_view_dto_deserialize_invalid_representations(self):
        raw_json_strings = [
            """
            {
                "name": "test_view",
                "representations": [],
                "audit": {
                    "creator": "creator"
                }
            }
            """,
            """
            {
                "name": "test_view",
                "representations": [null],
                "audit": {
                    "creator": "creator"
                }
            }
            """,
            """
            {
                "name": "test_view",
                "representations": [
                    {
                        "type": "sql",
                        "dialect": "",
                        "sql": "SELECT 1"
                    }
                ],
                "audit": {
                    "creator": "creator"
                }
            }
            """,
            """
            {
                "name": "test_view",
                "representations": [
                    {
                        "type": "sql",
                        "dialect": "spark",
                        "sql": ""
                    }
                ],
                "audit": {
                    "creator": "creator"
                }
            }
            """,
        ]

        for raw_json in raw_json_strings:
            with self.assertRaises(ValueError):
                ViewDTO.from_json(raw_json)

    def test_view_dto_validate_constructor_arguments(self):
        with self.assertRaises(ValueError):
            ViewDTO(
                _name="",
                _representations=[self._representation()],
                _audit=AuditDTO("creator"),
            )
        with self.assertRaises(ValueError):
            ViewDTO(_name="test_view", _representations=[], _audit=AuditDTO("creator"))
        with self.assertRaises(ValueError):
            ViewDTO(
                _name="test_view", _representations=[None], _audit=AuditDTO("creator")
            )
        with self.assertRaises(ValueError):
            ViewDTO(
                _name="test_view",
                _representations=[self._representation()],
                _audit=None,
            )

    def test_view_dto_sql_for(self):
        view = self._view_dto(
            representations=[
                self._representation(Dialects.TRINO, "SELECT 1"),
                self._representation(Dialects.SPARK, "SELECT 2"),
            ]
        )
        deserialized = ViewDTO.from_json(view.to_json())

        self.assertEqual("SELECT 1", deserialized.sql_for("trino").sql())
        self.assertEqual("SELECT 1", deserialized.sql_for("TRINO").sql())
        self.assertEqual("SELECT 1", deserialized.sql_for("Trino").sql())
        self.assertEqual("SELECT 2", deserialized.sql_for(Dialects.SPARK).sql())
        self.assertIsNone(deserialized.sql_for("hive"))
        self.assertIsNone(deserialized.sql_for(None))
