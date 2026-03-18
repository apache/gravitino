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
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
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
  void testCreateTableOnClusterEmbedsCluterNameInComment() {
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
        sql.contains(String.valueOf(ClickHouseClusterUtils.CLUSTER_META_SEP)),
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
}
