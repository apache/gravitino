/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.catalog.clickhouse.operations;

import static org.apache.gravitino.catalog.clickhouse.ClickHouseTablePropertiesMetadata.GRAVITINO_ENGINE_KEY;
import static org.apache.gravitino.catalog.clickhouse.operations.ClickHouseClusterUtils.CLUSTER_META_PREFIX;
import static org.apache.gravitino.catalog.clickhouse.operations.ClickHouseClusterUtils.extractClusterFromComment;
import static org.apache.gravitino.catalog.clickhouse.operations.ClickHouseClusterUtils.stripClusterMetadata;

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.catalog.clickhouse.ClickHouseConstants.ClusterConstants;
import org.apache.gravitino.catalog.clickhouse.ClickHouseConstants.DistributedTableConstants;
import org.apache.gravitino.catalog.clickhouse.converter.ClickHouseColumnDefaultValueConverter;
import org.apache.gravitino.catalog.clickhouse.converter.ClickHouseExceptionConverter;
import org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.catalog.jdbc.JdbcTable;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Index.IndexType;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestClickHouseTableOperationsCluster {

  private TestableClickHouseTableOperations ops;

  @BeforeEach
  void setUp() {
    ops = new TestableClickHouseTableOperations();
    ops.initialize(
        null,
        new ClickHouseExceptionConverter(),
        new ClickHouseTypeConverter(),
        new ClickHouseColumnDefaultValueConverter(),
        new HashMap<>());
  }

  @Test
  void testGenerateCreateTableSqlWithClusterDistributedEngine() {
    JdbcColumn[] columns =
        new JdbcColumn[] {
          JdbcColumn.builder()
              .withName("user_id")
              .withType(Types.IntegerType.get())
              .withNullable(false)
              .build()
        };

    Map<String, String> props = new HashMap<>();
    props.put(ClusterConstants.CLUSTER_NAME, "ck_cluster");
    props.put(ClusterConstants.ON_CLUSTER, "true");
    props.put(GRAVITINO_ENGINE_KEY, "Distributed");
    props.put(DistributedTableConstants.REMOTE_DATABASE, "remote_db");
    props.put(DistributedTableConstants.REMOTE_TABLE, "remote_table");
    props.put(DistributedTableConstants.SHARDING_KEY, "`user_id`");

    String sql =
        ops.buildCreateSql(
            "tbl", columns, "comment", props, null, Distributions.NONE, new Index[0], null);

    Assertions.assertTrue(sql.contains("CREATE TABLE `tbl` ON CLUSTER `ck_cluster`"));
    Assertions.assertTrue(
        sql.contains("ENGINE = Distributed(`ck_cluster`,`remote_db`,`remote_table`,`user_id`)"));

    props.remove(DistributedTableConstants.REMOTE_DATABASE);
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            ops.buildCreateSql(
                "tbl", columns, "comment", props, null, Distributions.NONE, new Index[0], null));

    props.put(DistributedTableConstants.REMOTE_DATABASE, "remote_db");
    props.remove(DistributedTableConstants.REMOTE_TABLE);
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            ops.buildCreateSql(
                "tbl", columns, "comment", props, null, Distributions.NONE, new Index[0], null));

    props.put(DistributedTableConstants.REMOTE_TABLE, "remote_table");
    props.remove(DistributedTableConstants.SHARDING_KEY);
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            ops.buildCreateSql(
                "tbl", columns, "comment", props, null, Distributions.NONE, new Index[0], null));

    props.put(DistributedTableConstants.SHARDING_KEY, "cityHash64(`user_id`)");
    String functionSql =
        ops.buildCreateSql(
            "tbl", columns, "comment", props, null, Distributions.NONE, new Index[0], null);
    Assertions.assertTrue(
        functionSql.contains(
            "ENGINE = Distributed(`ck_cluster`,`remote_db`,`remote_table`,cityHash64(`user_id`)"));
  }

  @Test
  void testShardingKeyValidation() {
    Map<String, String> props = new HashMap<>();
    props.put(ClusterConstants.CLUSTER_NAME, "ck_cluster");
    props.put(ClusterConstants.ON_CLUSTER, "true");
    props.put(GRAVITINO_ENGINE_KEY, "Distributed");
    props.put(DistributedTableConstants.REMOTE_DATABASE, "remote_db");
    props.put(DistributedTableConstants.REMOTE_TABLE, "remote_table");
    props.put(DistributedTableConstants.SHARDING_KEY, "user");

    JdbcColumn[] nullableColumn =
        new JdbcColumn[] {
          JdbcColumn.builder()
              .withName("user_id")
              .withType(Types.IntegerType.get())
              .withNullable(true)
              .build()
        };

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            ops.buildCreateSql(
                "tbl",
                nullableColumn,
                "comment",
                props,
                null,
                Distributions.NONE,
                new Index[0],
                null));
  }

  @Test
  void testFunctionOnlyShardingKeyAllowed() {
    Map<String, String> props = new HashMap<>();
    props.put(ClusterConstants.CLUSTER_NAME, "ck_cluster");
    props.put(ClusterConstants.ON_CLUSTER, "true");
    props.put(GRAVITINO_ENGINE_KEY, "Distributed");
    props.put(DistributedTableConstants.REMOTE_DATABASE, "remote_db");
    props.put(DistributedTableConstants.REMOTE_TABLE, "remote_table");
    props.put(DistributedTableConstants.SHARDING_KEY, "rand()");

    JdbcColumn[] columns =
        new JdbcColumn[] {
          JdbcColumn.builder()
              .withName("user_id")
              .withType(Types.IntegerType.get())
              .withNullable(false)
              .build()
        };

    String sql =
        ops.buildCreateSql(
            "tbl", columns, "comment", props, null, Distributions.NONE, new Index[0], null);
    Assertions.assertTrue(
        sql.contains("ENGINE = Distributed(`ck_cluster`,`remote_db`,`remote_table`,rand())"));
  }

  @Test
  void testGenerateCreateTableSqlWithDistributedEngineWithoutOnCluster() {
    JdbcColumn[] columns =
        new JdbcColumn[] {
          JdbcColumn.builder()
              .withName("user_id")
              .withType(Types.IntegerType.get())
              .withNullable(false)
              .build()
        };

    Map<String, String> props = new HashMap<>();
    props.put(ClusterConstants.CLUSTER_NAME, "ck_cluster");
    props.put(GRAVITINO_ENGINE_KEY, "Distributed");
    props.put(DistributedTableConstants.REMOTE_DATABASE, "remote_db");
    props.put(DistributedTableConstants.REMOTE_TABLE, "remote_table");
    props.put(DistributedTableConstants.SHARDING_KEY, "user_id");

    String sql =
        ops.buildCreateSql(
            "tbl", columns, "comment", props, null, Distributions.NONE, new Index[0], null);

    Assertions.assertTrue(sql.startsWith("CREATE TABLE `tbl`"));
    Assertions.assertTrue(
        sql.contains("ENGINE = Distributed(`ck_cluster`,`remote_db`,`remote_table`,`user_id`)"));
  }

  @Test
  void testDistributedTableWithEmptyColumns() {
    JdbcColumn[] columns = new JdbcColumn[] {};

    Map<String, String> props = new HashMap<>();
    props.put(ClusterConstants.CLUSTER_NAME, "ck_cluster");
    props.put(GRAVITINO_ENGINE_KEY, "Distributed");
    props.put(DistributedTableConstants.REMOTE_DATABASE, "remote_db");
    props.put(DistributedTableConstants.REMOTE_TABLE, "remote_table");
    props.put(DistributedTableConstants.SHARDING_KEY, "user_id");

    String sql =
        ops.buildCreateSql(
            "tbl", columns, "comment", props, null, Distributions.NONE, new Index[0], null);

    Assertions.assertTrue(sql.startsWith("CREATE TABLE `tbl`"));
    Assertions.assertTrue(sql.contains("AS `remote_db`.`remote_table`"));
    Assertions.assertTrue(
        sql.contains("ENGINE = Distributed(`ck_cluster`,`remote_db`,`remote_table`,`user_id`)"));
  }

  @Test
  void testGenerateDropTableSqlWithoutCluster() {
    Map<String, String> props = new HashMap<>();
    props.put(ClusterConstants.ON_CLUSTER, "false");

    String sql = ops.buildDropSql("orders", props);
    Assertions.assertEquals("DROP TABLE `orders`", sql);
  }

  @Test
  void testGenerateDropTableSqlWithNullProperties() {
    String sql = ops.buildDropSql("orders", null);
    Assertions.assertEquals("DROP TABLE `orders`", sql);
  }

  @Test
  void testGenerateDropTableSqlWithCluster() {
    Map<String, String> props = new HashMap<>();
    props.put(ClusterConstants.ON_CLUSTER, "true");
    props.put(ClusterConstants.CLUSTER_NAME, "ck_cluster");

    String sql = ops.buildDropSql("orders", props);
    Assertions.assertEquals("DROP TABLE `orders` ON CLUSTER `ck_cluster` SYNC", sql);
  }

  @Test
  void testGenerateDropTableSqlOnClusterWithoutClusterName() {
    // on-cluster=true but no cluster-name → fall back to plain DROP TABLE
    Map<String, String> props = new HashMap<>();
    props.put(ClusterConstants.ON_CLUSTER, "true");

    String sql = ops.buildDropSql("orders", props);
    Assertions.assertEquals("DROP TABLE `orders`", sql);
  }

  // ---------------------------------------------------------------------------
  // Cluster metadata embedded in COMMENT
  // ---------------------------------------------------------------------------

  /**
   * When a MergeTree table is created ON CLUSTER, the cluster name must be embedded in the stored
   * COMMENT so it can be recovered at DROP/load time (SHOW CREATE TABLE omits ON CLUSTER).
   */
  @Test
  void testCreateTableOnClusterEmbedsClusterNameInComment() {
    JdbcColumn[] columns =
        new JdbcColumn[] {
          JdbcColumn.builder()
              .withName("id")
              .withType(Types.IntegerType.get())
              .withNullable(false)
              .build()
        };

    Map<String, String> props = new HashMap<>();
    props.put(ClusterConstants.CLUSTER_NAME, "ck_cluster");
    props.put(ClusterConstants.ON_CLUSTER, "true");
    props.put(GRAVITINO_ENGINE_KEY, "MergeTree");

    String sql =
        ops.buildCreateSql(
            "tbl",
            columns,
            "user comment",
            props,
            null,
            Distributions.NONE,
            new Index[0],
            new SortOrder[] {SortOrders.ascending(NamedReference.field("id"))});

    // CREATE TABLE clause must have ON CLUSTER
    Assertions.assertTrue(
        sql.contains("ON CLUSTER `ck_cluster`"),
        "SQL must contain ON CLUSTER `ck_cluster`; got: " + sql);

    // The COMMENT clause must contain the embedded cluster metadata
    String expectedPrefix = CLUSTER_META_PREFIX + "ck_cluster";
    Assertions.assertTrue(
        sql.contains(expectedPrefix),
        "SQL COMMENT must contain cluster metadata '" + expectedPrefix + "'; got: " + sql);

    // User comment must be preserved before the separator
    Assertions.assertTrue(
        sql.contains("user comment"),
        "SQL COMMENT must still contain the original user comment; got: " + sql);
  }

  /** Non-cluster table must NOT embed any cluster metadata in the COMMENT. */
  @Test
  void testCreateTableWithoutClusterDoesNotEmbedClusterMetadata() {
    JdbcColumn[] columns =
        new JdbcColumn[] {
          JdbcColumn.builder()
              .withName("id")
              .withType(Types.IntegerType.get())
              .withNullable(false)
              .build()
        };

    Map<String, String> props = new HashMap<>();
    props.put(GRAVITINO_ENGINE_KEY, "MergeTree");

    String sql =
        ops.buildCreateSql(
            "tbl",
            columns,
            "plain comment",
            props,
            null,
            Distributions.NONE,
            new Index[0],
            new SortOrder[] {SortOrders.ascending(NamedReference.field("id"))});

    Assertions.assertFalse(
        sql.contains(ClickHouseClusterUtils.CLUSTER_META_PREFIX),
        "Non-cluster table must not embed cluster metadata; got: " + sql);
    Assertions.assertTrue(
        sql.contains("plain comment"), "User comment must be present unmodified; got: " + sql);
  }

  /** Cluster metadata round-trip: embed → extract → strip. */
  @Test
  void testClusterMetadataRoundTrip() {
    String stored = ClickHouseClusterUtils.embedClusterInComment("my comment", "ck_cluster");
    Assertions.assertEquals("ck_cluster", extractClusterFromComment(stored));
    Assertions.assertEquals("my comment", stripClusterMetadata(stored));
  }

  /** When no user comment is provided, only the cluster metadata token is stored. */
  @Test
  void testClusterMetadataRoundTripNullComment() {
    String stored = ClickHouseClusterUtils.embedClusterInComment(null, "ck_cluster");
    Assertions.assertEquals("ck_cluster", extractClusterFromComment(stored));
    Assertions.assertEquals("", stripClusterMetadata(stored));
  }

  /** ALTER TABLE with ON CLUSTER=true should include ON CLUSTER in SQL. */
  @Test
  void testAlterTableWithOnCluster() {
    StubClickHouseTableOperations ops = new StubClickHouseTableOperations();
    ops.initialize(
        null,
        new ClickHouseExceptionConverter(),
        new ClickHouseTypeConverter(),
        new ClickHouseColumnDefaultValueConverter(),
        new HashMap<>());
    ops.setTable(buildStubTableWithCluster("ck_cluster", true));

    String sql =
        ops.buildAlterSql(
            "default",
            "orders",
            new TableChange[] {
              TableChange.addColumn(new String[] {"new_col"}, Types.IntegerType.get())
            });

    Assertions.assertTrue(sql.contains("ON CLUSTER"), "ALTER TABLE should include ON CLUSTER");
    Assertions.assertTrue(sql.contains("`ck_cluster`"), "ALTER TABLE should include cluster name");
  }

  /** ALTER TABLE with ON CLUSTER=false should NOT include ON CLUSTER in SQL. */
  @Test
  void testAlterTableWithoutOnCluster() {
    StubClickHouseTableOperations ops = new StubClickHouseTableOperations();
    ops.initialize(
        null,
        new ClickHouseExceptionConverter(),
        new ClickHouseTypeConverter(),
        new ClickHouseColumnDefaultValueConverter(),
        new HashMap<>());
    ops.setTable(buildStubTableWithCluster("ck_cluster", false));

    String sql =
        ops.buildAlterSql(
            "default",
            "orders",
            new TableChange[] {
              TableChange.addColumn(new String[] {"new_col"}, Types.IntegerType.get())
            });

    Assertions.assertFalse(sql.contains("ON CLUSTER"), "ALTER TABLE should NOT include ON CLUSTER");
  }

  /** ALTER TABLE with null properties should NOT throw NPE. */
  @Test
  void testAlterTableWithNullProperties() {
    StubClickHouseTableOperations ops = new StubClickHouseTableOperations();
    ops.initialize(
        null,
        new ClickHouseExceptionConverter(),
        new ClickHouseTypeConverter(),
        new ClickHouseColumnDefaultValueConverter(),
        new HashMap<>());
    ops.setTable(buildStubTableWithNullProperties());

    String sql =
        ops.buildAlterSql(
            "default",
            "orders",
            new TableChange[] {
              TableChange.addColumn(new String[] {"new_col"}, Types.IntegerType.get())
            });

    Assertions.assertFalse(sql.contains("ON CLUSTER"), "ALTER TABLE should NOT include ON CLUSTER");
    Assertions.assertTrue(sql.contains("ADD COLUMN"), "ALTER TABLE should contain ADD COLUMN");
  }

  /** Shard key with nullable column should be rejected. */
  @Test
  void testShardingKeyNullableColumnRejected() {
    Map<String, String> props = new HashMap<>();
    props.put(ClusterConstants.CLUSTER_NAME, "ck_cluster");
    props.put(ClusterConstants.ON_CLUSTER, "true");
    props.put(GRAVITINO_ENGINE_KEY, "Distributed");
    props.put(DistributedTableConstants.REMOTE_DATABASE, "remote_db");
    props.put(DistributedTableConstants.REMOTE_TABLE, "remote_table");
    props.put(DistributedTableConstants.SHARDING_KEY, "user_id");

    JdbcColumn[] columns =
        new JdbcColumn[] {
          JdbcColumn.builder()
              .withName("user_id")
              .withType(Types.IntegerType.get())
              .withNullable(true)
              .build()
        };

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            ops.buildCreateSql(
                "tbl", columns, "comment", props, null, Distributions.NONE, new Index[0], null));
  }

  /** Shard key with non-integer column type should be rejected. */
  @Test
  void testShardingKeyNonIntegerColumnRejected() {
    Map<String, String> props = new HashMap<>();
    props.put(ClusterConstants.CLUSTER_NAME, "ck_cluster");
    props.put(ClusterConstants.ON_CLUSTER, "true");
    props.put(GRAVITINO_ENGINE_KEY, "Distributed");
    props.put(DistributedTableConstants.REMOTE_DATABASE, "remote_db");
    props.put(DistributedTableConstants.REMOTE_TABLE, "remote_table");
    props.put(DistributedTableConstants.SHARDING_KEY, "user_id");

    JdbcColumn[] columns =
        new JdbcColumn[] {
          JdbcColumn.builder()
              .withName("user_id")
              .withType(Types.StringType.get())
              .withNullable(false)
              .build()
        };

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            ops.buildCreateSql(
                "tbl", columns, "comment", props, null, Distributions.NONE, new Index[0], null));
  }

  /** Function-wrapped shard key with non-integer inner column should be accepted. */
  @Test
  void testShardingKeyFunctionWithNonIntegerColumnAccepted() {
    Map<String, String> props = new HashMap<>();
    props.put(ClusterConstants.CLUSTER_NAME, "ck_cluster");
    props.put(ClusterConstants.ON_CLUSTER, "true");
    props.put(GRAVITINO_ENGINE_KEY, "Distributed");
    props.put(DistributedTableConstants.REMOTE_DATABASE, "remote_db");
    props.put(DistributedTableConstants.REMOTE_TABLE, "remote_table");
    props.put(DistributedTableConstants.SHARDING_KEY, "cityHash64(user_name)");

    JdbcColumn[] columns =
        new JdbcColumn[] {
          JdbcColumn.builder()
              .withName("user_name")
              .withType(Types.StringType.get())
              .withNullable(false)
              .build()
        };

    // cityHash64(string_col) returns UInt64, so it should be accepted
    String sql =
        ops.buildCreateSql(
            "tbl", columns, "comment", props, null, Distributions.NONE, new Index[0], null);
    Assertions.assertTrue(sql.contains("cityHash64"));
  }

  /**
   * Bare Int128 column as shard key should be accepted because it is a valid integer type in
   * ClickHouse, even though it maps to ExternalType in Gravitino's type system.
   */
  @Test
  void testShardingKeyInt128ColumnAccepted() {
    Map<String, String> props = new HashMap<>();
    props.put(ClusterConstants.CLUSTER_NAME, "ck_cluster");
    props.put(ClusterConstants.ON_CLUSTER, "true");
    props.put(GRAVITINO_ENGINE_KEY, "Distributed");
    props.put(DistributedTableConstants.REMOTE_DATABASE, "remote_db");
    props.put(DistributedTableConstants.REMOTE_TABLE, "remote_table");
    props.put(DistributedTableConstants.SHARDING_KEY, "id");

    JdbcColumn[] columns =
        new JdbcColumn[] {
          JdbcColumn.builder()
              .withName("id")
              .withType(Types.ExternalType.of("Int128"))
              .withNullable(false)
              .build()
        };

    // Int128 is a valid integer type in ClickHouse. The isIntegerType() check recognizes
    // ExternalType with integer catalog strings (Int128/256, UInt128/256).
    String sql =
        ops.buildCreateSql(
            "tbl", columns, "comment", props, null, Distributions.NONE, new Index[0], null);
    Assertions.assertTrue(sql.contains("Distributed"));
  }

  /** A non-numeric index granularity should be rejected to avoid malformed DDL. */
  @Test
  void testIndexNonNumericGranularityRejected() {
    JdbcColumn[] columns =
        new JdbcColumn[] {
          JdbcColumn.builder()
              .withName("c1")
              .withType(Types.IntegerType.get())
              .withNullable(false)
              .build()
        };

    Index[] indexes =
        new Index[] {
          Indexes.of(
              Index.IndexType.DATA_SKIPPING_MINMAX,
              "idx_c1",
              new String[][] {{"c1"}},
              Map.of("granularity", "abc"))
        };

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            ops.buildCreateSql(
                "tbl",
                columns,
                "comment",
                new HashMap<>(),
                null,
                Distributions.NONE,
                indexes,
                null));
  }

  /** Index with GRANULARITY 0 should be rejected since ClickHouse requires GRANULARITY >= 1. */
  @Test
  void testIndexZeroGranularityRejected() {
    JdbcColumn[] columns =
        new JdbcColumn[] {
          JdbcColumn.builder()
              .withName("c1")
              .withType(Types.IntegerType.get())
              .withNullable(false)
              .build()
        };

    Index[] indexes =
        new Index[] {
          Indexes.of(
              Index.IndexType.DATA_SKIPPING_MINMAX,
              "idx_c1",
              new String[][] {{"c1"}},
              Map.of("granularity", "0"))
        };

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            ops.buildCreateSql(
                "tbl",
                columns,
                "comment",
                new HashMap<>(),
                null,
                Distributions.NONE,
                indexes,
                null));
  }

  /** ALTER TABLE ADD INDEX should use the default GRANULARITY value. */
  @Test
  void testAlterTableAddIndexDefaultGranularity() {
    StubClickHouseTableOperations stubOps = new StubClickHouseTableOperations();
    stubOps.initialize(
        null,
        new ClickHouseExceptionConverter(),
        new ClickHouseTypeConverter(),
        new ClickHouseColumnDefaultValueConverter(),
        new HashMap<>());
    stubOps.setTable(buildStubTableWithCluster("ck_cluster", true));

    String minmaxSql =
        stubOps.buildAlterSql(
            "default",
            "orders",
            new TableChange[] {
              TableChange.addIndex(
                  IndexType.DATA_SKIPPING_MINMAX, "idx_mm", new String[][] {{"id"}})
            });
    Assertions.assertTrue(
        minmaxSql.contains("ADD INDEX `idx_mm` `id` TYPE minmax GRANULARITY 1"),
        "minmax ADD INDEX should use default GRANULARITY 1, actual: " + minmaxSql);

    String bloomSql =
        stubOps.buildAlterSql(
            "default",
            "orders",
            new TableChange[] {
              TableChange.addIndex(
                  IndexType.DATA_SKIPPING_BLOOM_FILTER, "idx_bf", new String[][] {{"id"}})
            });
    Assertions.assertTrue(
        bloomSql.contains("ADD INDEX `idx_bf` `id` TYPE bloom_filter GRANULARITY 1"),
        "bloom_filter ADD INDEX should use default GRANULARITY 1, actual: " + bloomSql);
  }

  /** ALTER TABLE ADD INDEX on a cluster table should include ON CLUSTER in SQL. */
  @Test
  void testAlterTableAddIndexWithClusterProperties() {
    StubClickHouseTableOperations stubOps = new StubClickHouseTableOperations();
    stubOps.initialize(
        null,
        new ClickHouseExceptionConverter(),
        new ClickHouseTypeConverter(),
        new ClickHouseColumnDefaultValueConverter(),
        new HashMap<>());
    stubOps.setTable(buildStubTableWithCluster("ck_cluster", true));

    String sql =
        stubOps.buildAlterSql(
            "default",
            "orders",
            new TableChange[] {
              TableChange.addIndex(
                  IndexType.DATA_SKIPPING_MINMAX, "idx_mm", new String[][] {{"id"}})
            });

    Assertions.assertTrue(
        sql.contains("ON CLUSTER"), "ALTER TABLE ADD INDEX should include ON CLUSTER");
    Assertions.assertTrue(sql.contains("`ck_cluster`"), "ALTER TABLE should include cluster name");
    Assertions.assertTrue(
        sql.contains("ADD INDEX `idx_mm` `id` TYPE minmax GRANULARITY 1"),
        "Should contain ADD INDEX with default GRANULARITY");
  }

  /** ALTER TABLE ADD INDEX with null properties should not throw NPE. */
  @Test
  void testAlterTableAddIndexWithNullProperties() {
    StubClickHouseTableOperations stubOps = new StubClickHouseTableOperations();
    stubOps.initialize(
        null,
        new ClickHouseExceptionConverter(),
        new ClickHouseTypeConverter(),
        new ClickHouseColumnDefaultValueConverter(),
        new HashMap<>());
    stubOps.setTable(buildStubTableWithNullProperties());

    String sql =
        stubOps.buildAlterSql(
            "default",
            "orders",
            new TableChange[] {
              TableChange.addIndex(
                  IndexType.DATA_SKIPPING_MINMAX, "idx_mm", new String[][] {{"id"}})
            });

    Assertions.assertFalse(sql.contains("ON CLUSTER"), "ALTER TABLE should NOT include ON CLUSTER");
    Assertions.assertTrue(sql.contains("ADD INDEX"), "ALTER TABLE should contain ADD INDEX");
  }

  private static class TestableClickHouseTableOperations extends ClickHouseTableOperations {
    String buildCreateSql(
        String tableName,
        JdbcColumn[] columns,
        String comment,
        Map<String, String> properties,
        Transform[] partitioning,
        Distribution distribution,
        Index[] indexes,
        SortOrder[] sortOrders) {
      return generateCreateTableSql(
          tableName, columns, comment, properties, partitioning, distribution, indexes, sortOrders);
    }

    String buildDropSql(String tableName, Map<String, String> properties) {
      return generateDropTableSql(tableName, properties);
    }
  }

  private static final class StubClickHouseTableOperations extends ClickHouseTableOperations {
    private JdbcTable table;

    void setTable(JdbcTable table) {
      this.table = table;
    }

    @Override
    protected JdbcTable getOrCreateTable(
        String databaseName, String tableName, JdbcTable lazyLoadCreateTable) {
      return table;
    }

    String buildAlterSql(String db, String tableName, TableChange[] changes) {
      return generateAlterTableSql(db, tableName, changes);
    }
  }

  private static JdbcTable buildStubTableWithCluster(String clusterName, boolean onCluster) {
    JdbcColumn c1 =
        JdbcColumn.builder()
            .withName("id")
            .withType(Types.IntegerType.get())
            .withNullable(false)
            .build();
    Map<String, String> props = new HashMap<>();
    props.put(ClusterConstants.CLUSTER_NAME, clusterName);
    props.put(ClusterConstants.ON_CLUSTER, String.valueOf(onCluster));
    return JdbcTable.builder()
        .withName("orders")
        .withColumns(new JdbcColumn[] {c1})
        .withIndexes(new Index[0])
        .withProperties(props)
        .withTableOperation(null)
        .build();
  }

  private static JdbcTable buildStubTableWithNullProperties() {
    JdbcColumn c1 =
        JdbcColumn.builder()
            .withName("id")
            .withType(Types.IntegerType.get())
            .withNullable(false)
            .build();
    return JdbcTable.builder()
        .withName("orders")
        .withColumns(new JdbcColumn[] {c1})
        .withIndexes(new Index[0])
        .withTableOperation(null)
        .build();
  }
}
