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

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.catalog.clickhouse.ClickHouseConstants.ClusterConstants;
import org.apache.gravitino.catalog.clickhouse.ClickHouseConstants.DistributedTableConstants;
import org.apache.gravitino.catalog.clickhouse.converter.ClickHouseColumnDefaultValueConverter;
import org.apache.gravitino.catalog.clickhouse.converter.ClickHouseExceptionConverter;
import org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
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
          JdbcColumn.builder().withName("user_id").withType(Types.IntegerType.get()).build()
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

    Assertions.assertTrue(sql.startsWith("CREATE TABLE `tbl` ("));
    Assertions.assertTrue(
        sql.contains("ENGINE = Distributed(`ck_cluster`,`remote_db`,`remote_table`,`user_id`)"));
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
  }
}
