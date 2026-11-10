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
from gravitino.dto.rel.column_dto import ColumnDTO
from gravitino.dto.rel.sql_representation_dto import SQLRepresentationDTO
from gravitino.dto.requests.view_update_request import ViewUpdateRequest
from gravitino.dto.requests.view_updates_request import ViewUpdatesRequest


class TestViewUpdateRequest(unittest.TestCase):
    @staticmethod
    def _column() -> ColumnDTO:
        return ColumnDTO(
            _name="id",
            _data_type=Types.IntegerType.get(),
            _nullable=False,
        )

    @staticmethod
    def _representation(
        dialect: str = Dialects.TRINO, sql: str = "SELECT id FROM table"
    ) -> SQLRepresentationDTO:
        return SQLRepresentationDTO(_dialect=dialect, _sql=sql)

    def test_rename_view_request(self):
        request = ViewUpdateRequest.RenameViewRequest(_new_name="new_view")

        request.validate()
        json_str = request.to_json()
        deserialized = ViewUpdateRequest.RenameViewRequest.from_json(json_str)
        deserialized.validate()

        self.assertEqual(request, deserialized)
        self.assertIn('"@type": "rename"', json_str)
        self.assertEqual("new_view", deserialized.view_change().new_name())

        with self.assertRaises(ValueError):
            ViewUpdateRequest.RenameViewRequest(_new_name="").validate()

    def test_set_view_property_request(self):
        request = ViewUpdateRequest.SetViewPropertyRequest(
            _property="key", _value="value"
        )

        request.validate()
        json_str = request.to_json()
        deserialized = ViewUpdateRequest.SetViewPropertyRequest.from_json(json_str)
        deserialized.validate()

        self.assertEqual(request, deserialized)
        self.assertIn('"@type": "setProperty"', json_str)
        self.assertEqual("key", deserialized.view_change().property())
        self.assertEqual("value", deserialized.view_change().value())

        with self.assertRaises(ValueError):
            ViewUpdateRequest.SetViewPropertyRequest(
                _property="", _value="value"
            ).validate()
        with self.assertRaises(ValueError):
            ViewUpdateRequest.SetViewPropertyRequest(
                _property="key", _value=None
            ).validate()

    def test_remove_view_property_request(self):
        request = ViewUpdateRequest.RemoveViewPropertyRequest(_property="key")

        request.validate()
        json_str = request.to_json()
        deserialized = ViewUpdateRequest.RemoveViewPropertyRequest.from_json(json_str)
        deserialized.validate()

        self.assertEqual(request, deserialized)
        self.assertIn('"@type": "removeProperty"', json_str)
        self.assertEqual("key", deserialized.view_change().property())

        with self.assertRaises(ValueError):
            ViewUpdateRequest.RemoveViewPropertyRequest(_property="").validate()

    def test_replace_view_request(self):
        request = ViewUpdateRequest.ReplaceViewRequest(
            _columns=[self._column()],
            _representations=[self._representation()],
            _default_catalog="catalog",
            _default_schema="schema",
            _comment="comment",
        )

        request.validate()
        json_str = request.to_json()
        deserialized = ViewUpdateRequest.ReplaceViewRequest.from_json(json_str)
        deserialized.validate()
        change = deserialized.view_change()

        self.assertEqual(request, deserialized)
        self.assertIn('"@type": "replaceView"', json_str)
        self.assertIn('"representations"', json_str)
        self.assertEqual("catalog", change.default_catalog())
        self.assertEqual("schema", change.default_schema())
        self.assertEqual("comment", change.comment())
        self.assertEqual(1, len(change.columns()))
        self.assertEqual(1, len(change.representations()))
        self.assertIsInstance(change.representations()[0], SQLRepresentation)
        self.assertEqual(Dialects.TRINO, change.representations()[0].dialect())
        self.assertEqual("SELECT id FROM table", change.representations()[0].sql())

    def test_replace_view_request_defaults_empty_columns(self):
        request = ViewUpdateRequest.ReplaceViewRequest(
            _representations=[self._representation()]
        )

        request.validate()
        self.assertEqual([], request.view_change().columns())

    def test_replace_view_request_excludes_missing_representations(self):
        request = ViewUpdateRequest.ReplaceViewRequest()

        self.assertNotIn('"representations"', request.to_json())

    def test_replace_view_request_validate(self):
        with self.assertRaises(ValueError):
            ViewUpdateRequest.ReplaceViewRequest(_representations=[]).validate()
        with self.assertRaises(ValueError):
            ViewUpdateRequest.ReplaceViewRequest(_representations=[None]).validate()
        with self.assertRaises(ValueError):
            ViewUpdateRequest.ReplaceViewRequest(
                _representations=[self._representation("", "SELECT 1")]
            ).validate()
        with self.assertRaises(ValueError):
            ViewUpdateRequest.ReplaceViewRequest(
                _columns=[None], _representations=[self._representation()]
            ).validate()
        with self.assertRaises(ValueError):
            ViewUpdateRequest.ReplaceViewRequest(
                _columns=[ColumnDTO(_name="", _data_type=Types.IntegerType.get())],
                _representations=[self._representation()],
            ).validate()
        with self.assertRaises(ValueError):
            ViewUpdateRequest.ReplaceViewRequest(
                _representations=[
                    self._representation(Dialects.TRINO, "SELECT 1"),
                    self._representation(Dialects.TRINO, "SELECT 2"),
                ]
            ).validate()

    def test_view_updates_request(self):
        updates = ViewUpdatesRequest(
            _updates=[
                ViewUpdateRequest.RenameViewRequest(_new_name="new_view"),
                ViewUpdateRequest.SetViewPropertyRequest(
                    _property="key", _value="value"
                ),
                ViewUpdateRequest.RemoveViewPropertyRequest(_property="key"),
                ViewUpdateRequest.ReplaceViewRequest(
                    _representations=[self._representation()]
                ),
            ]
        )

        updates.validate()
        json_str = updates.to_json()

        self.assertIn('"updates"', json_str)
        self.assertIn('"@type": "rename"', json_str)
        self.assertIn('"@type": "setProperty"', json_str)
        self.assertIn('"@type": "removeProperty"', json_str)
        self.assertIn('"@type": "replaceView"', json_str)

    def test_view_updates_request_validate(self):
        with self.assertRaises(ValueError):
            ViewUpdatesRequest(_updates=None).validate()
        with self.assertRaises(ValueError):
            ViewUpdatesRequest(_updates=[]).validate()
        with self.assertRaises(ValueError):
            ViewUpdatesRequest(_updates=[None]).validate()
        with self.assertRaises(ValueError):
            ViewUpdatesRequest(
                _updates=[ViewUpdateRequest.RenameViewRequest(_new_name="")]
            ).validate()
