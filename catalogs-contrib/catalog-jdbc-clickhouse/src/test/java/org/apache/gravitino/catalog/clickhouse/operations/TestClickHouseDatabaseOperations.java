/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.catalog.clickhouse.operations;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.catalog.clickhouse.ClickHouseConfig;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestClickHouseDatabaseOperations {

  private static class TestableClickHouseDatabaseOperations extends ClickHouseDatabaseOperations {
    String buildCreateSql(String databaseName, String comment, Map<String, String> properties) {
      return generateCreateDatabaseSql(databaseName, comment, properties);
    }
  }

  private TestableClickHouseDatabaseOperations newOps(Map<String, String> conf) {
    TestableClickHouseDatabaseOperations ops = new TestableClickHouseDatabaseOperations();
    ops.initialize(null, new JdbcExceptionConverter(), conf);
    return ops;
  }

  @Test
  void testGenerateCreateDatabaseSqlWithoutCluster() {
    Map<String, String> conf = new HashMap<>();
    String sql = newOps(conf).buildCreateSql("db_name", null, Collections.emptyMap());
    Assertions.assertEquals("CREATE DATABASE `db_name`", sql);
  }

  @Test
  void testGenerateCreateDatabaseSqlWithCluster() {
    Map<String, String> conf = new HashMap<>();
    conf.put(ClickHouseConfig.CK_CLUSTER_NAME.getKey(), "ck_cluster");
    conf.put(ClickHouseConfig.CK_ON_CLUSTER.getKey(), "true");

    String sql = newOps(conf).buildCreateSql("db_name", "comment", Collections.emptyMap());
    Assertions.assertEquals(
        "CREATE DATABASE `db_name` ON CLUSTER ck_cluster COMMENT 'comment'", sql);
  }

  @Test
  void testGenerateCreateDatabaseSqlWithClusterNameButDisabled() {
    Map<String, String> conf = new HashMap<>();
    conf.put(ClickHouseConfig.CK_CLUSTER_NAME.getKey(), "ck_cluster");

    String sql = newOps(conf).buildCreateSql("db_name", "comment", Collections.emptyMap());
    Assertions.assertEquals("CREATE DATABASE `db_name` COMMENT 'comment'", sql);
  }

  @Test
  void testInitializeWithoutClusterName() {
    Map<String, String> conf = new HashMap<>();
    conf.put(ClickHouseConfig.CK_ON_CLUSTER.getKey(), "true");

    IllegalArgumentException exception =
        Assertions.assertThrows(IllegalArgumentException.class, () -> newOps(conf));
    Assertions.assertEquals(
        "ClickHouse 'ON CLUSTER' is enabled, but cluster name is not provided.",
        exception.getMessage());
  }
}
