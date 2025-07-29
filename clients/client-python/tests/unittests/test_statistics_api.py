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
from unittest.mock import Mock

from gravitino.api.metadata_object import MetadataObject
from gravitino.api.statistics.statistic_values import StatisticValues
from gravitino.client.metadata_object_impl import MetadataObjectImpl
from gravitino.client.metadata_object_statistics_operations import (
    MetadataObjectStatisticsOperations,
)
from gravitino.exceptions.base import IllegalArgumentException


class TestStatisticsAPI(unittest.TestCase):
    """Test cases for the statistics API."""

    def setUp(self):
        self.metalake_name = "test_metalake"
        self.catalog_name = "test_catalog"
        self.schema_name = "test_schema"
        self.table_name = "test_table"

        # Create a table metadata object
        self.table_object = MetadataObjectImpl(
            [self.catalog_name, self.schema_name, self.table_name],
            MetadataObject.Type.TABLE,
        )

        # Mock HTTP client
        self.mock_client = Mock()
        self.mock_client.get_context_map = Mock(return_value={})

        # Create statistics operations instance
        self.stats_ops = MetadataObjectStatisticsOperations(
            self.metalake_name, self.catalog_name, self.table_object, self.mock_client
        )

    def test_list_statistics(self):
        """Test listing statistics."""
        # Mock response
        mock_response = Mock()
        mock_response.json.return_value = {
            "code": 0,
            "statistics": [
                {
                    "name": "row_count",
                    "value": {"type": "long", "value": 1000},
                    "reserved": True,
                    "modifiable": False,
                },
                {
                    "name": "custom.avg_column_value",
                    "value": {"type": "double", "value": 123.45},
                    "reserved": False,
                    "modifiable": True,
                },
            ],
        }
        mock_response.in_stream = False

        self.mock_client.get.return_value = mock_response

        # Call list_statistics
        statistics = self.stats_ops.list_statistics()

        # Verify results
        assert len(statistics) == 2
        assert statistics[0].name() == "row_count"
        assert statistics[0].value().value() == 1000
        assert statistics[0].reserved() is True
        assert statistics[0].modifiable() is False

        assert statistics[1].name() == "custom.avg_column_value"
        assert statistics[1].value().value() == 123.45
        assert statistics[1].reserved() is False
        assert statistics[1].modifiable() is True

    def test_update_statistics(self):
        """Test updating statistics."""
        # Prepare update data
        statistics_to_update = {
            "row_count": StatisticValues.of_long(2000),
            "custom.max_value": StatisticValues.of_double(999.99),
            "custom.metadata": StatisticValues.of_object(
                {
                    "last_updated": StatisticValues.of_string("2024-01-01"),
                    "updated_by": StatisticValues.of_string("user1"),
                }
            ),
        }

        # Mock response
        mock_response = Mock()
        mock_response.json.return_value = {
            "code": 0,
            "statistics": [
                {
                    "name": "row_count",
                    "value": {"type": "long", "value": 2000},
                    "reserved": True,
                    "modifiable": False,
                },
                {
                    "name": "custom.max_value",
                    "value": {"type": "double", "value": 999.99},
                    "reserved": False,
                    "modifiable": True,
                },
                {
                    "name": "custom.metadata",
                    "value": {
                        "type": "object",
                        "value": {
                            "last_updated": {"type": "string", "value": "2024-01-01"},
                            "updated_by": {"type": "string", "value": "user1"},
                        },
                    },
                    "reserved": False,
                    "modifiable": True,
                },
            ],
        }
        mock_response.in_stream = False

        self.mock_client.post.return_value = mock_response

        # Call update_statistics
        updated_statistics = self.stats_ops.update_statistics(statistics_to_update)

        # Verify results
        assert len(updated_statistics) == 3
        assert updated_statistics[0].name() == "row_count"
        assert updated_statistics[0].value().value() == 2000

        # Verify the request was called with correct data
        self.mock_client.post.assert_called_once()
        call_args = self.mock_client.post.call_args
        assert "statistics" in call_args.kwargs["json"]

    def test_drop_statistics(self):
        """Test dropping statistics."""
        statistics_to_drop = ["custom.avg_column_value", "custom.max_value"]

        # Mock response
        mock_response = Mock()
        mock_response.json.return_value = {"code": 0, "dropped": True}
        mock_response.in_stream = False

        self.mock_client.delete.return_value = mock_response

        # Call drop_statistics
        dropped = self.stats_ops.drop_statistics(statistics_to_drop)

        # Verify results
        assert dropped is True

        # Verify the request was called with correct parameters
        self.mock_client.delete.assert_called_once()
        call_args = self.mock_client.delete.call_args
        assert (
            call_args.kwargs["params"]["statistics"]
            == "custom.avg_column_value,custom.max_value"
        )

    def test_invalid_metadata_object_type(self):
        """Test that non-table metadata objects are rejected."""
        catalog_object = MetadataObjectImpl(
            [self.catalog_name], MetadataObject.Type.CATALOG
        )

        with self.assertRaises(IllegalArgumentException) as cm:
            MetadataObjectStatisticsOperations(
                self.metalake_name, self.catalog_name, catalog_object, self.mock_client
            )

        assert "Statistics are only supported for TABLE object type" in str(
            cm.exception
        )

    def test_empty_statistics_update(self):
        """Test that empty statistics update is rejected."""
        with self.assertRaises(IllegalArgumentException) as cm:
            self.stats_ops.update_statistics({})

        assert "Statistics must not be None or empty" in str(cm.exception)

    def test_empty_statistics_drop(self):
        """Test that empty statistics drop is rejected."""
        with self.assertRaises(IllegalArgumentException) as cm:
            self.stats_ops.drop_statistics([])

        assert "Statistics list must not be None or empty" in str(cm.exception)

    def test_statistic_value_types(self):
        """Test all supported statistic value types."""
        # Test boolean value
        bool_value = StatisticValues.of_boolean(True)
        assert bool_value.value() is True
        assert bool_value.data_type() == "boolean"

        # Test long value
        long_value = StatisticValues.of_long(12345)
        assert long_value.value() == 12345
        assert long_value.data_type() == "long"

        # Test double value
        double_value = StatisticValues.of_double(123.45)
        assert double_value.value() == 123.45
        assert double_value.data_type() == "double"

        # Test string value
        string_value = StatisticValues.of_string("test string")
        assert string_value.value() == "test string"
        assert string_value.data_type() == "string"

        # Test list value
        list_value = StatisticValues.of_list(
            [
                StatisticValues.of_long(1),
                StatisticValues.of_long(2),
                StatisticValues.of_long(3),
            ]
        )
        assert len(list_value.value()) == 3
        assert list_value.data_type() == "list"

        # Test object value
        object_value = StatisticValues.of_object(
            {
                "key1": StatisticValues.of_string("value1"),
                "key2": StatisticValues.of_long(123),
            }
        )
        assert len(object_value.value()) == 2
        assert object_value.data_type() == "object"


if __name__ == "__main__":
    unittest.main()
