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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.catalog.clickhouse.ClickHouseConstants.ClusterConstants;
import org.apache.gravitino.catalog.jdbc.operation.JdbcDatabaseOperations;
import org.apache.gravitino.catalog.jdbc.utils.JdbcConnectorUtils;

public class ClickHouseDatabaseOperations extends JdbcDatabaseOperations {

  private static final Pattern ON_CLUSTER_PATTERN =
      Pattern.compile("(?i)\\bON\\s+CLUSTER\\s+`?([^`\\s(]+)`?");

  private static final Set<String> CLICK_HOUSE_SYSTEM_DATABASES =
      ImmutableSet.of("information_schema", "default", "system", "INFORMATION_SCHEMA");

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
    }

    if (StringUtils.isNotEmpty(originComment)) {
      createDatabaseSql.append(String.format(" COMMENT '%s'", originComment));
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
  protected void dropDatabase(String databaseName, boolean cascade) {
    try (final Connection connection = getConnection()) {
      connection.setCatalog(createSysDatabaseNameSet().iterator().next());
      if (!cascade) {
        try (Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(String.format("SHOW TABLES IN `%s`", databaseName))) {
          if (rs.next()) {
            throw new IllegalStateException(
                String.format(
                    "Database %s is not empty, the value of cascade should be true.",
                    databaseName));
          }
        }
      }
      String clusterName = getDatabaseClusterName(connection, databaseName);
      JdbcConnectorUtils.executeUpdate(
          connection, generateDropDatabaseSql(databaseName, clusterName));
    } catch (final SQLException se) {
      throw this.exceptionMapper.toGravitinoException(se);
    }
  }

  /**
   * Generates the SQL statement to drop a ClickHouse database. If the database was created with ON
   * CLUSTER, the DROP statement includes {@code ON CLUSTER `clusterName` SYNC} to propagate the
   * operation across all cluster nodes synchronously.
   *
   * @param databaseName The name of the database to drop.
   * @param clusterName The cluster name extracted from the database's CREATE SQL, or {@code null}
   *     if the database is not on a cluster.
   * @return The DROP DATABASE SQL statement.
   */
  @VisibleForTesting
  String generateDropDatabaseSql(String databaseName, String clusterName) {
    StringBuilder sql = new StringBuilder(String.format("DROP DATABASE `%s`", databaseName));
    if (StringUtils.isNotBlank(clusterName)) {
      sql.append(String.format(" ON CLUSTER `%s` SYNC", clusterName));
    }
    LOG.info("Generated drop database:{} sql: {}", databaseName, sql);
    return sql.toString();
  }

  /**
   * Extracts the cluster name from the {@code SHOW CREATE DATABASE} SQL of the given database.
   * Returns {@code null} when the database was not created with {@code ON CLUSTER}.
   */
  private String getDatabaseClusterName(Connection connection, String databaseName)
      throws SQLException {
    try (Statement stmt = connection.createStatement();
        ResultSet rs =
            stmt.executeQuery(String.format("SHOW CREATE DATABASE `%s`", databaseName))) {
      if (rs.next()) {
        String createSql = rs.getString(1);
        Matcher matcher = ON_CLUSTER_PATTERN.matcher(createSql);
        if (matcher.find()) {
          return matcher.group(1);
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
