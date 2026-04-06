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
from random import randint

from gravitino import (
    Catalog,
    GravitinoAdminClient,
    GravitinoClient,
    NameIdentifier,
)
from gravitino.api.rel.column import Column
from gravitino.api.rel.expressions.distributions.distributions import Distributions
from gravitino.api.rel.expressions.transforms.transforms import Transforms
from gravitino.api.rel.indexes.indexes import Indexes
from gravitino.api.rel.table import Table
from gravitino.api.rel.types.types import Types
from gravitino.client.relational_table import RelationalTable
from gravitino.exceptions.base import (
    NoSuchSchemaException,
    NoSuchTableException,
    TableAlreadyExistsException,
)
from gravitino.namespace import Namespace
from tests.integration.containers.hdfs_container import HDFSContainer
from tests.integration.integration_test_env import IntegrationTestEnv

logger = logging.getLogger(__name__)


class TestRelationalCatalog(IntegrationTestEnv):
    METALAKE_NAME: str = "TestRelationalTable_metalake" + str(randint(1, 10000))
    CATALOG_NAME: str = "relational_catalog"
    CATALOG_PROVIDER: str = "hive"
    SCHEMA_NAME: str = "test_schema"
    TABLE_NAME: str = "test_table"
    TABLE_IDENT: NameIdentifier = NameIdentifier.of(SCHEMA_NAME, TABLE_NAME)
    TABLE_COMMENT: str = "Test table for relational catalog"
    TABLE_PROPERTIES = {"property1": "value1", "property2": "value2"}

    @classmethod
    def setUpClass(cls):
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

    @classmethod
    def tearDownClass(cls):
        try:
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

    def setUp(self):
        # Create schema for each test
        TestRelationalCatalog.schema = (
            TestRelationalCatalog.catalog.as_schemas().create_schema(
                schema_name=TestRelationalCatalog.SCHEMA_NAME,
                comment="Test schema",
                properties={},
            )
        )

    def tearDown(self):
        # Clean up schema and tables after each test
        try:
            TestRelationalCatalog.catalog.as_schemas().drop_schema(
                schema_name=TestRelationalCatalog.SCHEMA_NAME, cascade=True
            )
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.warning("Failed to clean up test resources: %s", e)

    def _create_test_table(self) -> Table:
        """Create a test table with basic columns."""

        relational_catalog = self.catalog.as_table_catalog()

        columns = [
            Column.of("id", Types.LongType.get(), "Primary key"),
            Column.of("name", Types.StringType.get(), "Name column"),
            Column.of("age", Types.IntegerType.get(), "Age column", nullable=True),
        ]
        table_ident = NameIdentifier.of(
            TestRelationalCatalog.SCHEMA_NAME, TestRelationalCatalog.TABLE_NAME
        )

        return relational_catalog.create_table(
            identifier=table_ident,
            columns=columns,
            comment=TestRelationalCatalog.TABLE_COMMENT,
            properties=TestRelationalCatalog.TABLE_PROPERTIES,
            partitioning=Transforms.EMPTY_TRANSFORM,
            distribution=Distributions.NONE,
            sort_orders=[],
            indexes=Indexes.EMPTY_INDEXES,
        )

    def test_relational_catalog_create_table(self):
        """Test creating a table in the relational catalog."""
        table = self._create_test_table()
        self.assertIsNotNone(table)
        self.assertEqual(table.name(), TestRelationalCatalog.TABLE_NAME)
        self.assertEqual(table.comment(), TestRelationalCatalog.TABLE_COMMENT)
        self.assertEqual(table.properties(), TestRelationalCatalog.TABLE_PROPERTIES)
        self.assertEqual(len(table.columns()), 3)

        columns = table.columns()
        self.assertEqual(columns[0].name(), "id")
        self.assertEqual(columns[0].data_type(), Types.LongType.get())
        self.assertEqual(columns[1].name(), "name")
        self.assertEqual(columns[1].data_type(), Types.StringType.get())
        self.assertEqual(columns[2].name(), "age")
        self.assertEqual(columns[2].data_type(), Types.IntegerType.get())
        self.assertTrue(columns[2].nullable())

    def test_relational_table_create_table_already_exists(self):
        """Test creating a table that already exists should raise exception."""
        self._create_test_table()

        relational_catalog = self.catalog.as_table_catalog()

        columns = [Column.of("id", Types.LongType.get(), "Primary key")]
        table_ident = NameIdentifier.of(
            TestRelationalCatalog.SCHEMA_NAME, TestRelationalCatalog.TABLE_NAME
        )

        with self.assertRaises(TableAlreadyExistsException):
            relational_catalog.create_table(
                identifier=table_ident,
                columns=columns,
                comment="Duplicate table",
                properties={},
            )

    def test_relational_catalog_list_tables(self):
        """Test listing tables in the relational catalog."""
        self._create_test_table()
        relational_catalog = TestRelationalCatalog.catalog.as_table_catalog()

        table_identifiers = relational_catalog.list_tables(
            Namespace.of(TestRelationalCatalog.SCHEMA_NAME)
        )
        self.assertEqual(len(table_identifiers), 1)
        self.assertEqual(table_identifiers[0], TestRelationalCatalog.TABLE_IDENT)

    def test_relational_catalog_list_tables_invalid_namespace(self):
        """Test listing tables with invalid namespace."""
        relational_catalog = self.catalog.as_table_catalog()
        invalid_namespace = NameIdentifier.of(
            "non_existent_schema", "dummy"
        ).namespace()

        with self.assertRaises(NoSuchSchemaException):
            relational_catalog.list_tables(namespace=invalid_namespace)

    def test_relational_catalog_load_table(self):
        """Test loading a table from the relational catalog."""
        self._create_test_table()
        relational_catalog = TestRelationalCatalog.catalog.as_table_catalog()

        relational_table = relational_catalog.load_table(
            identifier=TestRelationalCatalog.TABLE_IDENT
        )
        self.assertIsInstance(relational_table, RelationalTable)
        self.assertEqual(relational_table.name(), TestRelationalCatalog.TABLE_NAME)

    def test_relational_catalog_load_table_not_exists(self):
        """Test loading a table that doesn't exist should raise exception."""
        relational_catalog = self.catalog.as_table_catalog()

        non_existent_table = NameIdentifier.of(
            TestRelationalCatalog.SCHEMA_NAME, "non_existent_table"
        )

        with self.assertRaises(NoSuchTableException):
            relational_catalog.load_table(identifier=non_existent_table)

    def test_relational_catalog_table_exists(self):
        """Test checking if a table exists."""
        relational_catalog = self.catalog.as_table_catalog()

        self.assertFalse(
            relational_catalog.table_exists(
                identifier=TestRelationalCatalog.TABLE_IDENT
            )
        )

        self._create_test_table()

        self.assertTrue(
            relational_catalog.table_exists(
                identifier=TestRelationalCatalog.TABLE_IDENT
            )
        )

    def test_relational_catalog_drop_table_not_exists(self):
        """Test dropping a table that doesn't exist should return False."""
        relational_catalog = TestRelationalCatalog.catalog.as_table_catalog()

        ident = NameIdentifier.of(TestRelationalCatalog.SCHEMA_NAME, "invalid_table")
        is_dropped = relational_catalog.drop_table(ident)
        self.assertFalse(is_dropped)

    def test_relational_catalog_drop_table(self):
        """Test dropping a table from the relational catalog."""
        self._create_test_table()
        relational_catalog = TestRelationalCatalog.catalog.as_table_catalog()

        is_dropped = relational_catalog.drop_table(TestRelationalCatalog.TABLE_IDENT)
        self.assertTrue(is_dropped)

    def test_relational_catalog_purge_table_not_exists(self):
        """Test purging a table that doesn't exist should return False."""
        relational_catalog = TestRelationalCatalog.catalog.as_table_catalog()

        ident = NameIdentifier.of(TestRelationalCatalog.SCHEMA_NAME, "invalid_table")
        self.assertFalse(relational_catalog.table_exists(ident))
        is_dropped = relational_catalog.purge_table(ident)
        self.assertFalse(is_dropped)

    def test_relational_catalog_purge_table(self):
        """Test purging a table from the relational catalog."""
        self._create_test_table()
        relational_catalog = TestRelationalCatalog.catalog.as_table_catalog()
        self.assertTrue(
            relational_catalog.table_exists(
                identifier=TestRelationalCatalog.TABLE_IDENT
            )
        )
        is_dropped = relational_catalog.purge_table(TestRelationalCatalog.TABLE_IDENT)
        self.assertTrue(is_dropped)
        self.assertFalse(
            relational_catalog.table_exists(
                identifier=TestRelationalCatalog.TABLE_IDENT
            )
        )
