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

import static org.apache.gravitino.catalog.clickhouse.operations.ClickHouseClusterUtils.CLUSTER_META_PREFIX;
import static org.apache.gravitino.catalog.clickhouse.operations.ClickHouseClusterUtils.embedClusterInComment;
import static org.apache.gravitino.catalog.clickhouse.operations.ClickHouseClusterUtils.extractClusterFromComment;
import static org.apache.gravitino.catalog.clickhouse.operations.ClickHouseClusterUtils.stripClusterMetadata;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;
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

    String buildDropSql(String databaseName, String clusterName) {
      return generateDropDatabaseSql(databaseName, clusterName);
    }
  }

  private TestableClickHouseDatabaseOperations newOps() {
    TestableClickHouseDatabaseOperations ops = new TestableClickHouseDatabaseOperations();
    ops.initialize(null, new JdbcExceptionConverter(), new HashMap<>());
    return ops;
  }

  // ---------------------------------------------------------------------------
  // CREATE DATABASE SQL generation
  // ---------------------------------------------------------------------------

  @Test
  void testGenerateCreateDatabaseSqlWithoutCluster() {
    String sql = newOps().buildCreateSql("db_name", null, Collections.emptyMap());
    Assertions.assertEquals("CREATE DATABASE `db_name`", sql);
  }

  @Test
  void testGenerateCreateDatabaseSqlWithComment() {
    String sql = newOps().buildCreateSql("db_name", "my comment", Collections.emptyMap());
    Assertions.assertEquals("CREATE DATABASE `db_name` COMMENT 'my comment'", sql);
  }

  @Test
  void testGenerateCreateDatabaseSqlWithClusterNameButDisabled() {
    String sql = newOps().buildCreateSql("db_name", "comment", Collections.emptyMap());
    Assertions.assertEquals("CREATE DATABASE `db_name` COMMENT 'comment'", sql);
  }

  @Test
  void testGenerateCreateDatabaseSqlWithClusterNoComment() {
    Map<String, String> properties = new HashMap<>();
    properties.put(ClusterConstants.CLUSTER_NAME, "ck_cluster");
    properties.put(ClusterConstants.ON_CLUSTER, "true");

    String sql = newOps().buildCreateSql("db_name", null, properties);
    // Cluster metadata embedded in COMMENT; user comment is empty
    String expectedComment = CLUSTER_META_PREFIX + "ck_cluster";
    Assertions.assertEquals(
        "CREATE DATABASE `db_name` ON CLUSTER `ck_cluster` COMMENT '" + expectedComment + "'", sql);
  }

  @Test
  void testGenerateCreateDatabaseSqlWithClusterAndComment() {
    Map<String, String> properties = new HashMap<>();
    properties.put(ClusterConstants.CLUSTER_NAME, "ck_cluster");
    properties.put(ClusterConstants.ON_CLUSTER, "true");

    String sql = newOps().buildCreateSql("db_name", "my comment", properties);
    // User comment preserved before the separator
    String expectedComment = "my comment" + CLUSTER_META_PREFIX + "ck_cluster";
    Assertions.assertEquals(
        "CREATE DATABASE `db_name` ON CLUSTER `ck_cluster` COMMENT '" + expectedComment + "'", sql);
  }

  // ---------------------------------------------------------------------------
  // DROP DATABASE SQL generation
  // ---------------------------------------------------------------------------

  @Test
  void testGenerateDropDatabaseSqlWithoutCluster() {
    String sql = newOps().buildDropSql("db_name", null);
    Assertions.assertEquals("DROP DATABASE `db_name`", sql);
  }

  @Test
  void testGenerateDropDatabaseSqlWithBlankCluster() {
    String sql = newOps().buildDropSql("db_name", "");
    Assertions.assertEquals("DROP DATABASE `db_name`", sql);
  }

  @Test
  void testGenerateDropDatabaseSqlWithCluster() {
    String sql = newOps().buildDropSql("db_name", "ck_cluster");
    Assertions.assertEquals("DROP DATABASE `db_name` ON CLUSTER `ck_cluster` SYNC", sql);
  }

  // ---------------------------------------------------------------------------
  // Comment metadata helpers
  // ---------------------------------------------------------------------------

  @Test
  void testEmbedClusterInCommentNullUserComment() {
    String stored = embedClusterInComment(null, "mycluster");
    Assertions.assertEquals(CLUSTER_META_PREFIX + "mycluster", stored);
  }

  @Test
  void testEmbedClusterInCommentWithUserComment() {
    String stored = embedClusterInComment("hello", "mycluster");
    Assertions.assertEquals("hello" + CLUSTER_META_PREFIX + "mycluster", stored);
  }

  @Test
  void testExtractClusterFromCommentPresent() {
    String stored = embedClusterInComment("some comment", "ck_cluster");
    Assertions.assertEquals("ck_cluster", extractClusterFromComment(stored));
  }

  @Test
  void testExtractClusterFromCommentAbsent() {
    Assertions.assertNull(extractClusterFromComment("plain comment"));
  }

  @Test
  void testExtractClusterFromCommentNull() {
    Assertions.assertNull(extractClusterFromComment(null));
  }

  @Test
  void testStripClusterMetadataPresent() {
    String stored = embedClusterInComment("user comment", "mycluster");
    Assertions.assertEquals("user comment", stripClusterMetadata(stored));
  }

  @Test
  void testStripClusterMetadataAbsent() {
    Assertions.assertEquals("plain comment", stripClusterMetadata("plain comment"));
  }

  @Test
  void testStripClusterMetadataNull() {
    Assertions.assertNull(stripClusterMetadata(null));
  }

  @Test
  void testStripClusterMetadataNoUserComment() {
    // When user has no comment but cluster info is embedded
    String stored = embedClusterInComment(null, "mycluster");
    Assertions.assertEquals("", stripClusterMetadata(stored));
  }

  // ---------------------------------------------------------------------------
  // CREATE uses system catalog before execution
  // ---------------------------------------------------------------------------

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
}
