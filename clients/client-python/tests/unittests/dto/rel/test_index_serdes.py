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

    def test_index_serdes_legacy_clickhouse_metadata(self):
        annoy_properties = {
            "annoy_trees": "100",
            "clickhouse_type_full": "annoy(100)",
            "granularity": "1",
        }
        usearch_properties = {
            "clickhouse_type_full": "usearch('cosineDistance')",
            "granularity": "1",
            "usearch_distance_function": "cosineDistance",
        }

        annoy = IndexSerdes.deserialize(
            {
                "indexType": "data_skipping_annoy",
                "name": "idx_annoy",
                "fieldNames": [["embedding"]],
                "properties": annoy_properties,
            }
        )
        usearch = IndexSerdes.deserialize(
            {
                "indexType": "data_skipping_usearch",
                "name": "idx_usearch",
                "fieldNames": [["embedding"]],
                "properties": usearch_properties,
            }
        )

        self.assertEqual(Index.IndexType.DATA_SKIPPING_ANNOY, annoy.type())
        self.assertEqual(annoy_properties, annoy.properties())
        self.assertEqual(Index.IndexType.DATA_SKIPPING_USEARCH, usearch.type())
        self.assertEqual(usearch_properties, usearch.properties())
