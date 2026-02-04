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

import static org.apache.gravitino.catalog.clickhouse.ClickHouseConstants.CLUSTER_NAME;
import static org.apache.gravitino.catalog.clickhouse.ClickHouseConstants.CLUSTER_REMOTE_DATABASE;
import static org.apache.gravitino.catalog.clickhouse.ClickHouseConstants.CLUSTER_REMOTE_TABLE;
import static org.apache.gravitino.catalog.clickhouse.ClickHouseConstants.CLUSTER_SHARDING_KEY;
import static org.apache.gravitino.catalog.clickhouse.ClickHouseConstants.ON_CLUSTER;
import static org.apache.gravitino.catalog.clickhouse.ClickHouseTablePropertiesMetadata.CLICKHOUSE_ENGINE_KEY;

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.catalog.clickhouse.converter.ClickHouseColumnDefaultValueConverter;
import org.apache.gravitino.catalog.clickhouse.converter.ClickHouseExceptionConverter;
import org.apache.gravitino.catalog.clickhouse.converter.ClickHouseTypeConverter;
import org.apache.gravitino.catalog.jdbc.JdbcColumn;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
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
              .withName("col_1")
              .withType(Types.IntegerType.get())
              .withNullable(false)
              .build()
        };

    Map<String, String> props = new HashMap<>();
    props.put(CLUSTER_NAME, "ck_cluster");
    props.put(ON_CLUSTER, "true");
    props.put(CLICKHOUSE_ENGINE_KEY, "Distributed");
    props.put(CLUSTER_REMOTE_DATABASE, "remote_db");
    props.put(CLUSTER_REMOTE_TABLE, "remote_table");
    props.put(CLUSTER_SHARDING_KEY, "user_id");

    String sql =
        ops.buildCreateSql(
            "tbl", columns, "comment", props, null, Distributions.NONE, new Index[0], null);

    Assertions.assertTrue(sql.contains("CREATE TABLE `tbl` ON CLUSTER `ck_cluster`"));
    Assertions.assertTrue(
        sql.contains("ENGINE = Distributed(`ck_cluster`,`remote_db`,`remote_table`,user_id)"));
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
        org.apache.gravitino.rel.expressions.sorts.SortOrder[] sortOrders) {
      return generateCreateTableSql(
          tableName, columns, comment, properties, partitioning, distribution, indexes, sortOrders);
    }
  }
}
