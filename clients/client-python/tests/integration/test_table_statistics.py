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

"""
Integration test for table statistics functionality.
This test demonstrates how statistics work with tables in Gravitino.
"""

import logging
from typing import List, Dict

from gravitino.api.metadata_object import MetadataObject
from gravitino.api.statistics.statistic import Statistic
from gravitino.api.statistics.statistic_value import StatisticValue
from gravitino.api.statistics.statistic_values import StatisticValues
from gravitino.api.statistics.supports_statistics import SupportsStatistics
from gravitino.client.gravitino_client import GravitinoClient
from gravitino.client.metadata_object_impl import MetadataObjectImpl
from gravitino.client.metadata_object_statistics_operations import (
    MetadataObjectStatisticsOperations,
)
from tests.integration.integration_test_env import IntegrationTestEnv

logger = logging.getLogger(__name__)


class TableWithStatistics(SupportsStatistics):
    """
    Example implementation of a table that supports statistics.
    This would typically be part of the RelationalTable implementation.
    """

    def __init__(
        self,
        metalake_name: str,
        catalog_name: str,
        schema_name: str,
        table_name: str,
        rest_client,
    ):
        self.metalake_name = metalake_name
        self.catalog_name = catalog_name
        self.schema_name = schema_name
        self.table_name = table_name

        # Create metadata object for the table
        table_object = MetadataObjectImpl(
            [catalog_name, schema_name, table_name], MetadataObject.Type.TABLE
        )

        # Initialize statistics operations
        self._statistics_operations = MetadataObjectStatisticsOperations(
            metalake_name, catalog_name, table_object, rest_client
        )

    def list_statistics(self) -> List[Statistic]:
        """List all statistics for this table."""
        return self._statistics_operations.list_statistics()

    def update_statistics(
        self, statistics: Dict[str, StatisticValue]
    ) -> List[Statistic]:
        """Update statistics for this table."""
        return self._statistics_operations.update_statistics(statistics)

    def drop_statistics(self, statistics: List[str]) -> bool:
        """Drop specified statistics from this table."""
        return self._statistics_operations.drop_statistics(statistics)


class TestTableStatistics(IntegrationTestEnv):
    """Integration test for table statistics functionality."""

    metalake_name: str = "test_metalake_statistics"
    catalog_name: str = "test_catalog_statistics"
    schema_name: str = "test_schema_statistics"
    table_name: str = "test_table_statistics"

    def setUp(self):
        super().setUp()
        self.gravitino_client = GravitinoClient(
            uri="http://localhost:8090",
            metalake_name=self.metalake_name,
            check_version=False,
        )

    def test_table_statistics_operations(self):
        """Test complete statistics workflow for a table."""
        # Note: This test assumes that the table already exists in Gravitino.
        # In a real scenario, you would create the metalake, catalog, schema, and table first.

        # Create a table with statistics support
        # Note: In a real implementation, this would be obtained from the catalog
        # Here we directly access the protected member for testing purposes
        # pylint: disable=protected-access
        table = TableWithStatistics(
            self.metalake_name,
            self.catalog_name,
            self.schema_name,
            self.table_name,
            self.gravitino_client._rest_client,
        )

        try:
            # 1. List initial statistics (might be empty)
            initial_stats = table.list_statistics()
            logger.info("Initial statistics count: %d", len(initial_stats))

            # 2. Update/create statistics
            statistics_to_update = {
                # System statistics
                "row_count": StatisticValues.of_long(1000000),
                "size_in_bytes": StatisticValues.of_long(1024 * 1024 * 100),  # 100MB
                # Custom statistics
                "custom.avg_order_value": StatisticValues.of_double(156.78),
                "custom.unique_customers": StatisticValues.of_long(50000),
                "custom.data_quality_score": StatisticValues.of_double(0.95),
                # Complex statistics
                "custom.column_stats": StatisticValues.of_object(
                    {
                        "order_id": StatisticValues.of_object(
                            {
                                "min": StatisticValues.of_long(1),
                                "max": StatisticValues.of_long(1000000),
                                "null_count": StatisticValues.of_long(0),
                            }
                        ),
                        "customer_id": StatisticValues.of_object(
                            {
                                "distinct_count": StatisticValues.of_long(50000),
                                "null_count": StatisticValues.of_long(100),
                            }
                        ),
                    }
                ),
                # List statistics
                "custom.top_products": StatisticValues.of_list(
                    [
                        StatisticValues.of_string("Product A"),
                        StatisticValues.of_string("Product B"),
                        StatisticValues.of_string("Product C"),
                    ]
                ),
            }

            updated_stats = table.update_statistics(statistics_to_update)
            logger.info("Updated statistics count: %d", len(updated_stats))

            # Print updated statistics
            for stat in updated_stats:
                logger.info(
                    "Statistic: %s, Value: %s, Reserved: %s, Modifiable: %s",
                    stat.name(),
                    stat.value().value() if stat.value() else "None",
                    stat.reserved(),
                    stat.modifiable(),
                )

            # 3. List statistics again to verify
            all_stats = table.list_statistics()
            logger.info("Total statistics after update: %d", len(all_stats))

            # 4. Drop some custom statistics
            stats_to_drop = ["custom.data_quality_score", "custom.top_products"]
            dropped = table.drop_statistics(stats_to_drop)
            logger.info("Statistics dropped: %s", dropped)

            # 5. List final statistics
            final_stats = table.list_statistics()
            logger.info("Final statistics count: %d", len(final_stats))

            # Verify the dropped statistics are gone
            remaining_names = [stat.name() for stat in final_stats]
            assert "custom.data_quality_score" not in remaining_names
            assert "custom.top_products" not in remaining_names

        except Exception as e:
            logger.error("Error during statistics test: %s", e)
            raise


if __name__ == "__main__":
    import unittest

    unittest.main()
