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

import java.sql.Connection;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.gravitino.catalog.clickhouse.ClickHouseConfig;
import org.apache.gravitino.catalog.clickhouse.ClickHouseConstants.ClusterConstants;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestClickHouseDatabaseOperations {

  private static class TestableClickHouseDatabaseOperations extends ClickHouseDatabaseOperations {
    String buildCreateSql(String databaseName, String comment, Map<String, String> properties) {
      return generateCreateDatabaseSql(databaseName, comment, properties);
    }

    String buildDropSql(String databaseName, boolean cascade) {
      return generateDropDatabaseSql(databaseName, cascade);
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
  void testGenerateCreateDatabaseSqlWithClusterNameButDisabled() {
    Map<String, String> conf = new HashMap<>();
    conf.put(ClickHouseConfig.CK_CLUSTER_NAME.getKey(), "ck_cluster");

    String sql = newOps(conf).buildCreateSql("db_name", "comment", Collections.emptyMap());
    Assertions.assertEquals("CREATE DATABASE `db_name` COMMENT 'comment'", sql);
  }

  @Test
  void testGenerateCreateDatabaseSqlWithClusterEnabled() {
    Map<String, String> properties = new HashMap<>();
    properties.put(ClusterConstants.CLUSTER_NAME, "ck_cluster");
    properties.put(ClusterConstants.ON_CLUSTER, "true");

    String sql = newOps(new HashMap<>()).buildCreateSql("db_name", null, properties);
    Assertions.assertEquals("CREATE DATABASE `db_name` ON CLUSTER `ck_cluster`", sql);
  }

  @Test
  void testCreateUsesSystemCatalogBeforeExecution() throws Exception {
    DataSource dataSource = Mockito.mock(DataSource.class);
    Connection connection = Mockito.mock(Connection.class);
    Statement statement = Mockito.mock(Statement.class);

    Mockito.when(dataSource.getConnection()).thenReturn(connection);
    Mockito.when(connection.createStatement()).thenReturn(statement);
    Mockito.when(statement.executeUpdate(Mockito.anyString())).thenReturn(0);

    ClickHouseDatabaseOperations ops = new ClickHouseDatabaseOperations();
    ops.initialize(dataSource, new JdbcExceptionConverter(), new HashMap<>());

    ops.create("new_db", null, Collections.emptyMap());

    Mockito.verify(connection).setCatalog("information_schema");
    Mockito.verify(statement).executeUpdate("CREATE DATABASE `new_db`");
  }

  @Test
  void testGenerateDropDatabaseSqlWithoutCluster() {
    Map<String, String> conf = new HashMap<>();
    String sql = newOps(conf).buildDropSql("db_name", true);
    Assertions.assertEquals("DROP DATABASE `db_name`", sql);
  }

  @Test
  void testGenerateCreateDatabaseSqlEscapesCommentSingleQuotes() {
    Map<String, String> conf = new HashMap<>();
    String sql = newOps(conf).buildCreateSql("db", "it's a test", Collections.emptyMap());
    Assertions.assertEquals("CREATE DATABASE `db` COMMENT 'it''s a test'", sql);
  }

  @Test
  void testGenerateCreateDatabaseSqlEscapesClusterNameBackticks() {
    Map<String, String> properties = new HashMap<>();
    properties.put(ClusterConstants.CLUSTER_NAME, "cluster`name");
    properties.put(ClusterConstants.ON_CLUSTER, "true");

    String sql = newOps(new HashMap<>()).buildCreateSql("db", null, properties);
    Assertions.assertEquals("CREATE DATABASE `db` ON CLUSTER `cluster``name`", sql);
  }

  @Test
  void testGenerateCreateDatabaseSqlEscapesBothCommentAndCluster() {
    Map<String, String> properties = new HashMap<>();
    properties.put(ClusterConstants.CLUSTER_NAME, "c`k");
    properties.put(ClusterConstants.ON_CLUSTER, "true");

    String sql = newOps(new HashMap<>()).buildCreateSql("db", "it's", properties);
    Assertions.assertEquals("CREATE DATABASE `db` ON CLUSTER `c``k` COMMENT 'it''s'", sql);
  }
}
