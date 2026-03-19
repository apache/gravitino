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

import static org.apache.gravitino.catalog.clickhouse.operations.ClickHouseClusterUtils.embedClusterInComment;
import static org.apache.gravitino.catalog.clickhouse.operations.ClickHouseClusterUtils.escapeSingleQuotes;
import static org.apache.gravitino.catalog.clickhouse.operations.ClickHouseClusterUtils.extractClusterFromComment;
import static org.apache.gravitino.catalog.clickhouse.operations.ClickHouseClusterUtils.stripClusterMetadata;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.catalog.clickhouse.ClickHouseConstants.ClusterConstants;
import org.apache.gravitino.catalog.jdbc.JdbcSchema;
import org.apache.gravitino.catalog.jdbc.operation.JdbcDatabaseOperations;
import org.apache.gravitino.catalog.jdbc.utils.JdbcConnectorUtils;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.meta.AuditInfo;

public class ClickHouseDatabaseOperations extends JdbcDatabaseOperations {

  private static final Set<String> CLICK_HOUSE_SYSTEM_DATABASES =
      ImmutableSet.of("information_schema", "default", "system", "INFORMATION_SCHEMA");

  /**
   * Background: ClickHouse's {@code SHOW CREATE DATABASE} does not include {@code ON CLUSTER} in
   * its output for non-Replicated (Atomic) databases, and {@code system.databases.cluster} is only
   * populated for {@code Replicated} engine databases. Gravitino therefore embeds the cluster name
   * in the COMMENT field (see {@link ClickHouseClusterUtils}) so it can be retrieved at DROP time.
   */
  @Override
  protected boolean supportSchemaComment() {
    return true;
  }

  @Override
  protected Set<String> createSysDatabaseNameSet() {
    return CLICK_HOUSE_SYSTEM_DATABASES;
  }

  @Override
  public List<String> listDatabases() {
    List<String> databaseNames = new ArrayList<>();
    try (final Connection connection = getConnection()) {
      // It is possible that other catalogs have been deleted,
      // causing the following statement to error,
      // so here we manually set a system catalog
      connection.setCatalog(createSysDatabaseNameSet().iterator().next());
      try (Statement statement = connection.createStatement();
          ResultSet resultSet = statement.executeQuery("SHOW DATABASES")) {
        while (resultSet.next()) {
          String databaseName = resultSet.getString(1);
          if (!isSystemDatabase(databaseName)) {
            databaseNames.add(databaseName);
          }
        }
      }
      return databaseNames;
    } catch (final SQLException se) {
      throw this.exceptionMapper.toGravitinoException(se);
    }
  }

  @Override
  protected String generateCreateDatabaseSql(
      String databaseName, String comment, Map<String, String> properties) {

    String originComment = StringIdentifier.removeIdFromComment(comment);
    if (!supportSchemaComment() && StringUtils.isNotEmpty(originComment)) {
      throw new UnsupportedOperationException(
          "Doesn't support setting schema comment: " + originComment);
    }

    StringBuilder createDatabaseSql =
        new StringBuilder(String.format("CREATE DATABASE `%s`", databaseName));

    if (onCluster(properties)) {
      String clusterName = properties.get(ClusterConstants.CLUSTER_NAME);
      createDatabaseSql.append(String.format(" ON CLUSTER `%s`", clusterName));
      // Embed the cluster name into the COMMENT so it can be retrieved later (e.g., at DROP time).
      // ClickHouse does not persist ON CLUSTER info in SHOW CREATE DATABASE for Atomic databases.
      String storedComment = embedClusterInComment(originComment, clusterName);
      createDatabaseSql.append(String.format(" COMMENT '%s'", escapeSingleQuotes(storedComment)));
    } else if (StringUtils.isNotEmpty(originComment)) {
      createDatabaseSql.append(String.format(" COMMENT '%s'", escapeSingleQuotes(originComment)));
    }

    LOG.info("Generated create database:{} sql: {}", databaseName, createDatabaseSql);
    return createDatabaseSql.toString();
  }

  @Override
  public void create(String databaseName, String comment, Map<String, String> properties) {
    LOG.info("Beginning to create database {}", databaseName);
    String originComment = StringIdentifier.removeIdFromComment(comment);
    if (!supportSchemaComment() && StringUtils.isNotEmpty(originComment)) {
      throw new UnsupportedOperationException(
          "Doesn't support setting schema comment: " + originComment);
    }

    try (final Connection connection = getConnection()) {
      connection.setCatalog(createSysDatabaseNameSet().iterator().next());
      JdbcConnectorUtils.executeUpdate(
          connection, generateCreateDatabaseSql(databaseName, comment, properties));
      LOG.info("Finished creating database {}", databaseName);
    } catch (final SQLException se) {
      throw this.exceptionMapper.toGravitinoException(se);
    }
  }

  @Override
  public JdbcSchema load(String databaseName) throws NoSuchSchemaException {
    try (Connection connection = getConnection()) {
      connection.setCatalog(createSysDatabaseNameSet().iterator().next());
      try (PreparedStatement stmt =
          connection.prepareStatement(
              "SELECT name, comment FROM system.databases WHERE name = ?")) {
        stmt.setString(1, databaseName);
        try (ResultSet rs = stmt.executeQuery()) {
          if (!rs.next()) {
            throw new NoSuchSchemaException("Database %s could not be found", databaseName);
          }
          String storedComment = rs.getString("comment");
          String clusterName = extractClusterFromComment(storedComment);
          String userComment = stripClusterMetadata(storedComment);

          ImmutableMap.Builder<String, String> propsBuilder = ImmutableMap.builder();
          if (StringUtils.isNotBlank(clusterName)) {
            propsBuilder.put(ClusterConstants.ON_CLUSTER, String.valueOf(true));
            propsBuilder.put(ClusterConstants.CLUSTER_NAME, clusterName);
          }

          return JdbcSchema.builder()
              .withName(databaseName)
              .withComment(userComment)
              .withProperties(propsBuilder.build())
              .withAuditInfo(AuditInfo.EMPTY)
              .build();
        }
      }
    } catch (final SQLException se) {
      throw this.exceptionMapper.toGravitinoException(se);
    }
  }

  @Override
  protected void dropDatabase(String databaseName, boolean cascade) {
    try (final Connection connection = getConnection()) {
      connection.setCatalog(createSysDatabaseNameSet().iterator().next());
      if (!cascade) {
        // Query system.tables instead of SHOW TABLES IN so we get a proper parameterised query
        // and avoid any edge cases with special characters in database names.
        try (PreparedStatement checkStmt =
            connection.prepareStatement("SELECT 1 FROM system.tables WHERE database = ? LIMIT 1")) {
          checkStmt.setString(1, databaseName);
          try (ResultSet rs = checkStmt.executeQuery()) {
            if (rs.next()) {
              throw new IllegalArgumentException(
                  String.format(
                      "Database %s is not empty, you can drop it with CASCADE option.",
                      databaseName));
            }
          }
        }
      }
      // Read the cluster name stored in the COMMENT at CREATE time.
      // SHOW CREATE DATABASE does not include ON CLUSTER info for non-Replicated databases.
      String clusterName = readClusterName(connection, databaseName);
      JdbcConnectorUtils.executeUpdate(
          connection, generateDropDatabaseSql(databaseName, clusterName));
    } catch (final SQLException se) {
      throw this.exceptionMapper.toGravitinoException(se);
    }
  }

  /**
   * Generates the SQL statement to drop a ClickHouse database. If {@code clusterName} is non-blank,
   * the DROP statement includes {@code ON CLUSTER `clusterName` SYNC} to propagate the operation
   * across all cluster nodes synchronously.
   *
   * @param databaseName The name of the database to drop.
   * @param clusterName The cluster name read from the stored comment, or {@code null}/blank if not
   *     a cluster database.
   * @return The DROP DATABASE SQL statement.
   */
  @VisibleForTesting
  String generateDropDatabaseSql(String databaseName, String clusterName) {
    StringBuilder sql = new StringBuilder(String.format("DROP DATABASE `%s`", databaseName));
    if (StringUtils.isNotBlank(clusterName)) {
      sql.append(String.format(" ON CLUSTER `%s`", clusterName));
    }
    LOG.info("Generated drop database:{} sql: {}", databaseName, sql);
    return sql.toString();
  }

  /**
   * Reads the cluster name from {@code system.databases.comment} for the given database. Returns
   * {@code null} if the database has no embedded cluster metadata (i.e., it was created without
   * {@code ON CLUSTER}).
   */
  private String readClusterName(Connection connection, String databaseName) throws SQLException {
    try (PreparedStatement stmt =
        connection.prepareStatement("SELECT comment FROM system.databases WHERE name = ?")) {
      stmt.setString(1, databaseName);
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          return extractClusterFromComment(rs.getString("comment"));
        }
      }
    }
    return null;
  }

  private boolean onCluster(Map<String, String> dbProperties) {
    if (MapUtils.isEmpty(dbProperties)) {
      return false;
    }

    String clusterName = dbProperties.get(ClusterConstants.CLUSTER_NAME);
    if (StringUtils.isBlank(clusterName)) {
      return false;
    }

    return Boolean.parseBoolean(dbProperties.getOrDefault(ClusterConstants.ON_CLUSTER, "false"));
  }
}
