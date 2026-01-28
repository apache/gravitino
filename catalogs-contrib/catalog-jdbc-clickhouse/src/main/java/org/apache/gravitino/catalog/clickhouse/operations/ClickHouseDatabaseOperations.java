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

import static org.apache.gravitino.catalog.clickhouse.ClickHouseConfig.DEFAULT_CK_ON_CLUSTER;

import com.google.common.collect.ImmutableSet;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.sql.DataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.catalog.clickhouse.ClickHouseConfig;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.apache.gravitino.catalog.jdbc.operation.JdbcDatabaseOperations;

public class ClickHouseDatabaseOperations extends JdbcDatabaseOperations {

  // TODO: handle ClickHouse cluster properly when creating/dropping databases/tables
  //  use https://github.com/apache/gravitino/issues/9820 to track it.
  @SuppressWarnings("unused")
  private boolean onCluster = false;

  @SuppressWarnings("unused")
  private String clusterName = null;

  @Override
  public void initialize(
      DataSource dataSource, JdbcExceptionConverter exceptionMapper, Map<String, String> conf) {
    super.initialize(dataSource, exceptionMapper, conf);

    final String cn = conf.get(ClickHouseConfig.CK_CLUSTER_NAME.getKey());
    if (StringUtils.isNotBlank(cn)) {
      clusterName = cn;
    }

    final String oc =
        conf.getOrDefault(
            ClickHouseConfig.CK_ON_CLUSTER.getKey(), String.valueOf(DEFAULT_CK_ON_CLUSTER));
    onCluster = Boolean.parseBoolean(oc);

    if (onCluster && StringUtils.isBlank(clusterName)) {
      throw new IllegalArgumentException(
          "ClickHouse 'ON CLUSTER' is enabled, but cluster name is not provided.");
    }
  }

  @Override
  protected boolean supportSchemaComment() {
    return true;
  }

  @Override
  protected Set<String> createSysDatabaseNameSet() {
    return ImmutableSet.of("information_schema", "INFORMATION_SCHEMA", "default", "system");
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

    if (StringUtils.isNotEmpty(comment)) {
      createDatabaseSql.append(String.format(" COMMENT '%s'", originComment));
    }

    LOG.info("Generated create database:{} sql: {}", databaseName, createDatabaseSql);
    return createDatabaseSql.toString();
  }
}
