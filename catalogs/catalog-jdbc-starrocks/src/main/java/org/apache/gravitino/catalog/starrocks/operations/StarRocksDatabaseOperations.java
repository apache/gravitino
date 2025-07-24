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
package org.apache.gravitino.catalog.starrocks.operations;

import com.google.common.collect.ImmutableSet;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.catalog.jdbc.JdbcSchema;
import org.apache.gravitino.catalog.jdbc.operation.JdbcDatabaseOperations;
import org.apache.gravitino.catalog.starrocks.utils.StarRocksUtils;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.meta.AuditInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Database operations for StarRocks. */
public class StarRocksDatabaseOperations extends JdbcDatabaseOperations {

  private static final Logger LOG = LoggerFactory.getLogger(StarRocksDatabaseOperations.class);

  @Override
  public String generateCreateDatabaseSql(
      String databaseName, String comment, Map<String, String> properties) {
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append(String.format("CREATE DATABASE `%s`", databaseName));

    // Append properties
    sqlBuilder.append("\n");
    sqlBuilder.append(StarRocksUtils.generatePropertiesSql(properties));

    String ddl = sqlBuilder.toString();
    LOG.info("Generated create database:{} sql: {}", databaseName, ddl);
    return ddl;
  }

  @Override
  public String generateDropDatabaseSql(String databaseName, boolean cascade) {
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append(String.format("DROP DATABASE `%s`", databaseName));
    if (cascade) {
      sqlBuilder.append(" FORCE");
      return sqlBuilder.toString();
    }
    String query = String.format("SHOW TABLES IN `%s`", databaseName);
    try (final Connection connection = this.dataSource.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query)) {
      if (resultSet.next()) {
        throw new IllegalStateException(
            String.format(
                "Database %s is not empty, the value of cascade should be true.", databaseName));
      }
    } catch (SQLException sqlException) {
      throw this.exceptionMapper.toGravitinoException(sqlException);
    }
    return sqlBuilder.toString();
  }

  @Override
  public JdbcSchema load(String databaseName) throws NoSuchSchemaException {
    List<String> allDatabases = listDatabases();
    String dbName =
        allDatabases.stream()
            .filter(db -> db.equals(databaseName))
            .findFirst()
            .orElseThrow(
                () -> new NoSuchSchemaException("Database %s could not be found", databaseName));
    // StarRocks support set properties, but can't get them after setting
    // https://docs.starrocks.io/docs/3.3/sql-reference/sql-statements/Database/SHOW_CREATE_DATABASE/
    return JdbcSchema.builder()
        .withName(dbName)
        .withComment("")
        .withProperties(Collections.emptyMap())
        .withAuditInfo(AuditInfo.EMPTY)
        .build();
  }

  @Override
  protected boolean supportSchemaComment() {
    return false;
  }

  @Override
  protected Set<String> createSysDatabaseNameSet() {
    return ImmutableSet.of("information_schema");
  }
}
