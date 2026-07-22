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
from gravitino.api.rel.representation import Representation
from gravitino.api.rel.types.types import Types
from gravitino.dto.rel.column_dto import ColumnDTO
from gravitino.dto.rel.sql_representation_dto import SQLRepresentationDTO
from gravitino.dto.requests.view_create_request import ViewCreateRequest


class TestViewCreateRequest(unittest.TestCase):
    @staticmethod
    def _request(
        name: str = "test_view",
        columns: list[ColumnDTO] = None,
        representations: list[SQLRepresentationDTO] = None,
    ) -> ViewCreateRequest:
        return ViewCreateRequest(
            _name=name,
            _columns=(
                columns
                if columns is not None
                else [
                    ColumnDTO(
                        _name="id",
                        _data_type=Types.IntegerType.get(),
                        _nullable=False,
                    )
                ]
            ),
            _representations=(
                representations
                if representations is not None
                else [
                    SQLRepresentationDTO(
                        _dialect=Dialects.TRINO,
                        _sql="SELECT id FROM test_table",
                    )
                ]
            ),
            _comment="comment",
            _default_catalog="catalog",
            _default_schema="schema",
            _properties={"k1": "v1"},
        )

    def test_validate_and_serialize(self):
        request = self._request()

        request.validate()
        json_str = request.to_json()
        deserialized = ViewCreateRequest.from_json(json_str)
        deserialized.validate()

        self.assertEqual(request, deserialized)
        self.assertIn('"name": "test_view"', json_str)
        self.assertIn('"columns"', json_str)
        self.assertIn('"representations"', json_str)
        self.assertIn(f'"type": "{Representation.TYPE_SQL}"', json_str)
        self.assertIn(f'"dialect": "{Dialects.TRINO}"', json_str)
        self.assertIn('"sql": "SELECT id FROM test_table"', json_str)
        self.assertIn('"defaultCatalog": "catalog"', json_str)
        self.assertIn('"defaultSchema": "schema"', json_str)

    def test_validate_rejects_invalid_name(self):
        request = self._request(name="")

        with self.assertRaises(ValueError):
            request.validate()

    def test_validate_rejects_empty_representations(self):
        request = self._request(representations=[])

        with self.assertRaises(ValueError):
            request.validate()

    def test_serialize_excludes_missing_representations(self):
        request = ViewCreateRequest(_name="test_view")

        self.assertNotIn('"representations"', request.to_json())

    def test_validate_rejects_missing_representations(self):
        request = ViewCreateRequest(_name="test_view")

        with self.assertRaisesRegex(
            ValueError, '"representations" field is required and cannot be empty'
        ):
            request.validate()

    def test_validate_rejects_null_representation(self):
        request = self._request(representations=[None])

        with self.assertRaises(ValueError):
            request.validate()

    def test_validate_rejects_invalid_representation(self):
        request = self._request(
            representations=[SQLRepresentationDTO(_dialect="", _sql="SELECT 1")]
        )

        with self.assertRaises(ValueError):
            request.validate()

    def test_validate_rejects_null_column(self):
        request = self._request(columns=[None])

        with self.assertRaises(ValueError):
            request.validate()

    def test_validate_rejects_invalid_column(self):
        request = self._request(
            columns=[ColumnDTO(_name="", _data_type=Types.IntegerType.get())]
        )

        with self.assertRaises(ValueError):
            request.validate()

    def test_validate_rejects_duplicate_dialect(self):
        request = self._request(
            representations=[
                SQLRepresentationDTO(_dialect=Dialects.TRINO, _sql="SELECT 1"),
                SQLRepresentationDTO(_dialect=Dialects.TRINO, _sql="SELECT 2"),
            ]
        )

        with self.assertRaises(ValueError):
            request.validate()
