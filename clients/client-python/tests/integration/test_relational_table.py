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
from gravitino.dto.rel.column_dto import ColumnDTO
from gravitino.dto.rel.partitioning.identity_partitioning_dto import (
    IdentityPartitioningDTO,
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
