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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
  private static final Pattern ON_CLUSTER_PATTERN =
      Pattern.compile("(?i)\\bON\\s+CLUSTER\\s+`?([^`\\s]+)`?");

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
      JdbcConnectorUtils.executeUpdate(connection, generateDropDatabaseSql(databaseName, cascade));
    } catch (final SQLException se) {
      throw this.exceptionMapper.toGravitinoException(se);
    }
  }

  @Override
  public JdbcSchema load(String databaseName) throws NoSuchSchemaException {
    if (!exist(databaseName)) {
      throw new NoSuchSchemaException("Database %s could not be found", databaseName);
    }

    Map<String, String> schemaProperties = new HashMap<>();
    schemaProperties.put(ClusterConstants.ON_CLUSTER, String.valueOf(false));

    try (Connection connection = getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                String.format("SHOW CREATE DATABASE `%s`", databaseName.replace("`", "``")))) {
      if (resultSet.next()) {
        String createSql = resultSet.getString(1);
        Matcher matcher = ON_CLUSTER_PATTERN.matcher(StringUtils.trimToEmpty(createSql));
        if (matcher.find()) {
          schemaProperties.put(ClusterConstants.ON_CLUSTER, String.valueOf(true));
          schemaProperties.put(ClusterConstants.CLUSTER_NAME, matcher.group(1));
        } else {
          String inferredClusterName = detectClusterName(connection, databaseName);
          if (StringUtils.isNotBlank(inferredClusterName)) {
            schemaProperties.put(ClusterConstants.ON_CLUSTER, String.valueOf(true));
            schemaProperties.put(ClusterConstants.CLUSTER_NAME, inferredClusterName);
          }
        }
      }
    } catch (SQLException se) {
      throw this.exceptionMapper.toGravitinoException(se);
    }

    return JdbcSchema.builder()
        .withName(databaseName)
        .withProperties(ImmutableMap.copyOf(schemaProperties))
        .withAuditInfo(AuditInfo.EMPTY)
        .build();
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

  private String detectClusterName(Connection connection, String databaseName) throws SQLException {
    List<String> clusterNames = new ArrayList<>();
    String escapedDatabaseName = databaseName.replace("'", "''");
    try (Statement statement = connection.createStatement();
        ResultSet clusters =
            statement.executeQuery("SELECT DISTINCT cluster FROM system.clusters")) {
      while (clusters.next()) {
        clusterNames.add(clusters.getString(1));
      }
    }

    for (String clusterName : clusterNames) {
      String escapedClusterName = clusterName.replace("'", "''");
      int clusterHostCount;
      try (Statement statement = connection.createStatement();
          ResultSet hostCountResultSet =
              statement.executeQuery(
                  String.format(
                      "SELECT countDistinct(host_name) FROM system.clusters WHERE cluster = '%s'",
                      escapedClusterName))) {
        if (!hostCountResultSet.next()) {
          continue;
        }
        clusterHostCount = hostCountResultSet.getInt(1);
      }

      try (Statement statement = connection.createStatement();
          ResultSet dbHostCountResultSet =
              statement.executeQuery(
                  String.format(
                      "SELECT countDistinct(hostName()) FROM clusterAllReplicas('%s', system.databases) "
                          + "WHERE name = '%s'",
                      escapedClusterName, escapedDatabaseName))) {
        if (dbHostCountResultSet.next() && dbHostCountResultSet.getInt(1) == clusterHostCount) {
          return clusterName;
        }
      }
    }
    return null;
  }
}
