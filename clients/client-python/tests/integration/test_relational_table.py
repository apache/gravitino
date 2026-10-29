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

import logging
from datetime import date
from random import randint

from gravitino import (
    Catalog,
    GravitinoAdminClient,
    GravitinoClient,
    NameIdentifier,
)
from gravitino.api.rel.expressions.literals.literals import Literals
from gravitino.api.rel.partitions.partitions import Partitions
from gravitino.api.rel.types.types import Types
from gravitino.api.stats.partition_range import PartitionRange
from gravitino.api.stats.partition_statistics_drop import PartitionStatisticsDrop
from gravitino.api.stats.partition_statistics_update import PartitionStatisticsUpdate
from gravitino.api.stats.statistic_values import StatisticValues
from gravitino.dto.rel.column_dto import ColumnDTO
from gravitino.dto.rel.partitioning.identity_partitioning_dto import (
    IdentityPartitioningDTO,
)
from gravitino.exceptions.base import (
    IllegalStatisticNameException,
    NoSuchMetadataObjectException,
    NoSuchTableException,
)
from tests.integration.containers.hdfs_container import HDFSContainer
from tests.integration.integration_test_env import IntegrationTestEnv

logger = logging.getLogger(__name__)


class TestRelationalTable(IntegrationTestEnv):
    METALAKE_NAME: str = "TestRelationalTable_metalake" + str(randint(1, 10000))
    CATALOG_NAME: str = "relational_catalog"
    CATALOG_PROVIDER: str = "hive"
    SCHEMA_NAME: str = "test_schema"
    TABLE_NAME: str = "test_table"
    TABLE_IDENT: NameIdentifier = NameIdentifier.of(SCHEMA_NAME, TABLE_NAME)

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.hdfs_container: HDFSContainer = HDFSContainer()
        hive_metastore_uri = f"thrift://{cls.hdfs_container.get_ip()}:9083"
        logger.info("Started Hive container with metastore URI: %s", hive_metastore_uri)
        cls.gravitino_admin_client = GravitinoAdminClient(uri="http://localhost:8090")
        cls.gravitino_admin_client.create_metalake(
            cls.METALAKE_NAME,
            comment="Test metalake for relational catalog",
            properties={},
        )
        cls.gravitino_client: GravitinoClient = GravitinoClient(
            uri="http://localhost:8090", metalake_name=cls.METALAKE_NAME
        )
        cls.catalog = cls.gravitino_client.create_catalog(
            name=cls.CATALOG_NAME,
            catalog_type=Catalog.Type.RELATIONAL,
            provider=cls.CATALOG_PROVIDER,
            comment="Test relational catalog",
            properties={"metastore.uris": hive_metastore_uri},
        )
        cls.schema = cls.catalog.as_schemas().create_schema(
            schema_name=cls.SCHEMA_NAME,
            comment="Test schema",
            properties={},
        )
        cls.relational_catalog = cls.catalog.as_table_catalog()
        cls.relational_table = cls.relational_catalog.create_table(
            identifier=cls.TABLE_IDENT,
            columns=[
                ColumnDTO.builder()
                .with_name("dt")
                .with_data_type(Types.DateType.get())
                .build(),
                ColumnDTO.builder()
                .with_name("country")
                .with_data_type(Types.StringType.get())
                .build(),
            ],
            partitioning=[
                IdentityPartitioningDTO("dt"),
                IdentityPartitioningDTO("country"),
            ],
        )

    @classmethod
    def tearDownClass(cls):
        try:
            cls.catalog.as_schemas().drop_schema(
                schema_name=cls.SCHEMA_NAME, cascade=True
            )
            cls.gravitino_client.drop_catalog(name=cls.CATALOG_NAME, force=True)
            cls.gravitino_admin_client.drop_metalake(name=cls.METALAKE_NAME, force=True)
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.warning("Failed to clean up class-level resources: %s", e)

        # Clean up the HDFS/Hive container
        if cls.hdfs_container:
            try:
                cls.hdfs_container.close()
            except Exception as e:  # pylint: disable=broad-exception-caught
                logger.warning("Failed to clean up HDFS container: %s", e)

        super().tearDownClass()

    def test_relational_table_partition_ops(self):
        """Tests add/get/list/drop partition and list partition names of a relational table."""
        relational_table = self.relational_catalog.load_table(self.TABLE_IDENT)

        # Tests list partition names
        partition_names = relational_table.list_partition_names()
        self.assertEqual(len(partition_names), 0)

        # Tests add partition
        new_partition = relational_table.add_partition(
            Partitions.identity(
                name="dt=2025-12-03/country=us",
                field_names=[["dt"], ["country"]],
                values=[
                    Literals.date_literal(date.fromisoformat("2025-12-03")),
                    Literals.string_literal("us"),
                ],
            )
        )
        partition_names = relational_table.list_partition_names()
        self.assertEqual(len(partition_names), 1)

        # Tests list partitions
        partitions = relational_table.list_partitions()
        self.assertEqual(len(partitions), 1)
        self.assertEqual(partitions[0], new_partition)

        # Tests get partition
        partition = relational_table.get_partition(new_partition.name())
        self.assertEqual(new_partition, partition)

        # Tests drop partition
        result = relational_table.drop_partition(new_partition.name())
        self.assertTrue(result)
        partition_names = relational_table.list_partition_names()
        self.assertEqual(len(partition_names), 0)

    def test_relational_table_statistics_ops(self):
        table_stats_ops = self.relational_catalog.load_table(
            self.TABLE_IDENT
        ).supports_statistics()
        stats = {
            "custom-bool": StatisticValues.boolean_value(True),
            "custom-long": StatisticValues.long_value(100),
            "custom-double": StatisticValues.double_value(99.99),
            "custom-string": StatisticValues.string_value("test_stat"),
            "custom-list": StatisticValues.list_value(
                [StatisticValues.string_value("a")]
            ),
            "custom-object": StatisticValues.object_value(
                {"key": StatisticValues.long_value(1)}
            ),
        }

        table_stats_ops.update_statistics(stats)
        listed_stats = table_stats_ops.list_statistics()
        self.assertEqual(len(listed_stats), len(stats))
        stat_map = {stat.name(): stat for stat in listed_stats}
        for expected_key, expected_val in stats.items():
            self.assertIn(expected_key, stat_map)
            actual_stat = stat_map[expected_key]
            self.assertEqual(actual_stat.value().data_type(), expected_val.data_type())
            self.assertEqual(actual_stat.value().value(), expected_val.value())

        keys_to_drop = ["custom-bool", "custom-long"]
        self.assertTrue(table_stats_ops.drop_statistics(keys_to_drop))
        listed_stats_after_drop = table_stats_ops.list_statistics()
        self.assertEqual(len(listed_stats_after_drop), len(stats) - len(keys_to_drop))
        for key in keys_to_drop:
            self.assertNotIn(key, [s.name() for s in listed_stats_after_drop])
        remaining_keys = [k for k in stats if k not in keys_to_drop]
        self.assertTrue(table_stats_ops.drop_statistics(remaining_keys))
        self.assertEqual(len(table_stats_ops.list_statistics()), 0)

    def test_relational_table_partition_statistics_ops(self):
        """Round-trip partition statistics without creating physical partitions."""
        table = self.relational_catalog.load_table(self.TABLE_IDENT)
        operations = table.supports_partition_statistics()
        bound = PartitionRange.BoundType
        names = [f"dt=2026-10-0{day}/country=cn" for day in (1, 2, 3)]
        special_name = "dt=2026-10-04/country=中 +?#"
        all_names = names + [special_name]
        values = {
            "custom-bool": StatisticValues.boolean_value(False),
            "custom-long": StatisticValues.long_value(0),
            "custom-double": StatisticValues.double_value(1.5),
            "custom-string": StatisticValues.string_value(""),
            "custom-list": StatisticValues.list_value([StatisticValues.long_value(2)]),
            "custom-object": StatisticValues.object_value(
                {"nested": StatisticValues.boolean_value(False)}
            ),
        }
        self.assertEqual([], table.list_partition_names())
        self.assertEqual(
            [], operations.list_partition_statistics(PartitionRange.ALL_PARTITIONS)
        )
        try:
            operations.update_partition_statistics(
                [PartitionStatisticsUpdate(name, values) for name in all_names]
            )
            self._assert_partition_statistics_values(operations, all_names, values)
            self._assert_partition_statistics_ranges(operations, names, special_name)

            operations.update_partition_statistics(
                [
                    PartitionStatisticsUpdate(
                        names[0], {"custom-long": StatisticValues.long_value(20)}
                    )
                ]
            )
            updated = operations.list_partition_statistics(
                PartitionRange.between(names[0], bound.CLOSED, names[0], bound.CLOSED)
            )[0]
            self.assertEqual(
                20,
                next(
                    stat.value().value()
                    for stat in updated.statistics()
                    if stat.name() == "custom-long"
                ),
            )
            self.assertEqual(len(values), len(updated.statistics()))
            drops = [
                PartitionStatisticsDrop(name, ["custom-long"]) for name in names[:2]
            ]
            self.assertTrue(operations.drop_partition_statistics(drops))
            self.assertFalse(operations.drop_partition_statistics(drops))
            remaining = {
                item.partition_name(): {stat.name() for stat in item.statistics()}
                for item in operations.list_partition_statistics(
                    PartitionRange.ALL_PARTITIONS
                )
            }
            self.assertNotIn("custom-long", remaining[names[0]])
            self.assertNotIn("custom-long", remaining[names[1]])
            self.assertIn("custom-long", remaining[names[2]])
            with self.assertRaises(IllegalStatisticNameException):
                operations.update_partition_statistics(
                    [
                        PartitionStatisticsUpdate(
                            names[0], {"invalid-name": StatisticValues.long_value(1)}
                        )
                    ]
                )
            self.assertEqual([], table.list_partition_names())
        finally:
            operations.drop_partition_statistics(
                [PartitionStatisticsDrop(name, list(values)) for name in all_names]
            )
        self.assertEqual(
            [], operations.list_partition_statistics(PartitionRange.ALL_PARTITIONS)
        )

    def test_partition_statistics_missing_table(self):
        """A bound statistics operation still checks that its table exists."""
        identifier = NameIdentifier.of(self.SCHEMA_NAME, "partition_statistics_deleted")
        table = self.relational_catalog.create_table(
            identifier=identifier,
            columns=[
                ColumnDTO.builder()
                .with_name("id")
                .with_data_type(Types.IntegerType.get())
                .build()
            ],
        )
        operations = table.supports_partition_statistics()
        self.assertTrue(self.relational_catalog.drop_table(identifier))
        for operation, argument in [
            (operations.list_partition_statistics, PartitionRange.ALL_PARTITIONS),
            (
                operations.update_partition_statistics,
                [
                    PartitionStatisticsUpdate(
                        "p1", {"custom-x": StatisticValues.long_value(1)}
                    )
                ],
            ),
            (
                operations.drop_partition_statistics,
                [PartitionStatisticsDrop("p1", ["custom-x"])],
            ),
        ]:
            with self.subTest(operation=operation), self.assertRaises(
                (NoSuchMetadataObjectException, NoSuchTableException)
            ):
                operation(argument)

    def _assert_partition_statistics_values(self, operations, all_names, values):
        results = operations.list_partition_statistics(PartitionRange.ALL_PARTITIONS)
        self.assertEqual(set(all_names), {item.partition_name() for item in results})
        for result in results:
            actual = {stat.name(): stat for stat in result.statistics()}
            self.assertEqual(set(values), set(actual))
            for key, expected in values.items():
                self.assertEqual(expected.data_type(), actual[key].value().data_type())
                self.assertEqual(expected.value(), actual[key].value().value())
                self.assertFalse(actual[key].reserved())
                self.assertTrue(actual[key].modifiable())
                self.assertIsNotNone(actual[key].audit_info().creator())

    def _assert_partition_statistics_ranges(self, operations, names, special_name):
        bound = PartitionRange.BoundType
        for lower in bound:
            for upper in bound:
                with self.subTest(lower=lower, upper=upper):
                    expected = {names[1]}
                    if lower == bound.CLOSED:
                        expected.add(names[0])
                    if upper == bound.CLOSED:
                        expected.add(names[2])
                    actual = operations.list_partition_statistics(
                        PartitionRange.between(names[0], lower, names[2], upper)
                    )
                    self.assertEqual(
                        expected, {item.partition_name() for item in actual}
                    )
        for partition_range, expected in [
            (
                PartitionRange.down_to(names[1], bound.OPEN),
                {names[2], special_name},
            ),
            (PartitionRange.up_to(names[1], bound.CLOSED), set(names[:2])),
            (
                PartitionRange.between(names[1], bound.CLOSED, names[1], bound.CLOSED),
                {names[1]},
            ),
            (
                PartitionRange.between(names[1], bound.OPEN, names[1], bound.CLOSED),
                set(),
            ),
            (
                PartitionRange.between(names[2], bound.CLOSED, names[0], bound.CLOSED),
                set(),
            ),
            (
                PartitionRange.between(
                    special_name, bound.CLOSED, special_name, bound.CLOSED
                ),
                {special_name},
            ),
        ]:
            with self.subTest(partition_range=partition_range):
                actual = operations.list_partition_statistics(partition_range)
                self.assertEqual(expected, {item.partition_name() for item in actual})
