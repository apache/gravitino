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

import json
import unittest
from dataclasses import dataclass, field

from dataclasses_json import DataClassJsonMixin, config

from gravitino.api.rel.indexes.index import Index
from gravitino.dto.rel.indexes.index_dto import IndexDTO
from gravitino.dto.rel.indexes.json_serdes.index_serdes import IndexSerdes
from gravitino.exceptions.base import IllegalArgumentException


@dataclass
class MockDataClass(DataClassJsonMixin):
    indexes: list[Index] = field(
        metadata=config(
            encoder=lambda idxs: [IndexSerdes.serialize(index) for index in idxs],
            decoder=lambda idxs: [IndexSerdes.deserialize(index) for index in idxs],
        )
    )


class TestIndexSerdes(unittest.TestCase):
    def test_index_serdes_invalid_json(self):
        invalid_json_string = [
            '{"indexes": [{}]}',
            '{"indexes": [""]}',
            '{"indexes": [null]}',
        ]

        for json_string in invalid_json_string:
            with self.assertRaisesRegex(
                IllegalArgumentException, "Index must be a valid JSON object"
            ):
                MockDataClass.from_json(json_string)

    def test_index_serdes_missing_type(self):
        json_string = """
        {
            "indexes": [
                {
                    "fieldNames": [["id"]]
                }
            ]
        }
        """

        with self.assertRaisesRegex(
            IllegalArgumentException, "Cannot parse index from missing type"
        ):
            MockDataClass.from_json(json_string)

    def test_index_serdes_missing_field_names(self):
        json_string = """
        {
            "indexes": [
                {
                    "indexType": "PRIMARY_KEY"
                }
            ]
        }
        """

        with self.assertRaisesRegex(
            IllegalArgumentException, "Cannot parse index from missing field names"
        ):
            MockDataClass.from_json(json_string)

    def test_index_serdes(self):
        json_string = """
        {
            "indexes": [
                {
                    "indexType": "PRIMARY_KEY",
                    "name": "PRIMARY",
                    "fieldNames": [["id"]]
                },
                {
                    "indexType": "UNIQUE_KEY",
                    "fieldNames": [["name"], ["createTime"]]
                }
            ]
        }
        """

        mock_data_class = MockDataClass.from_json(json_string)
        indexes = mock_data_class.indexes
        self.assertEqual(len(indexes), 2)
        for index in indexes:
            self.assertTrue(isinstance(index, IndexDTO))
        self.assertTrue(
            indexes[0] == IndexDTO(Index.IndexType.PRIMARY_KEY, "PRIMARY", [["id"]])
        )
        self.assertTrue(
            indexes[1]
            == IndexDTO(Index.IndexType.UNIQUE_KEY, None, [["name"], ["createTime"]])
        )

        json_dict = json.loads(json_string)
        serialized_dict = json.loads(mock_data_class.to_json())
        self.assertDictEqual(json_dict, serialized_dict)

    def test_text_index_properties_round_trip(self):
        indexes_json = {
            "indexes": [
                {
                    "indexType": "DATA_SKIPPING_TEXT",
                    "name": "idx_tokens",
                    "fieldNames": [["body"]],
                    "properties": {"tokenizer": "tokens"},
                },
                {
                    "indexType": "DATA_SKIPPING_TEXT",
                    "name": "idx_ngrams",
                    "fieldNames": [["body"]],
                    "properties": {"tokenizer": "ngrams", "ngram_size": "3"},
                },
            ]
        }

        decoded = MockDataClass.from_json(json.dumps(indexes_json))
        self.assertEqual(
            [index.type() for index in decoded.indexes],
            [Index.IndexType.DATA_SKIPPING_TEXT] * 2,
        )
        self.assertEqual(decoded.indexes[0].properties(), {"tokenizer": "tokens"})
        self.assertEqual(
            decoded.indexes[1].properties(),
            {"tokenizer": "ngrams", "ngram_size": "3"},
        )
        self.assertDictEqual(json.loads(decoded.to_json()), indexes_json)
