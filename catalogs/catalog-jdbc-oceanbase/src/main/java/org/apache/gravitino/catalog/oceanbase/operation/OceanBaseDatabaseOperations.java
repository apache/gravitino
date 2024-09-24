/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.catalog.oceanbase.operation;

import com.google.common.collect.ImmutableMap;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.catalog.jdbc.JdbcSchema;
import org.apache.gravitino.catalog.jdbc.operation.JdbcDatabaseOperations;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.meta.AuditInfo;

/** Database operations for OceanBase. */
public class OceanBaseDatabaseOperations extends JdbcDatabaseOperations {

  public static final Set<String> SYS_OCEANBASE_DATABASE_NAMES = createSysOceanBaseDatabaseNames();

  private static Set<String> createSysOceanBaseDatabaseNames() {
    Set<String> set = new HashSet<>();
    set.add("information_schema");
    set.add("mysql");
    set.add("sys");
    set.add("oceanbase");
    return Collections.unmodifiableSet(set);
  }

  @Override
  public String generateCreateDatabaseSql(
      String databaseName, String comment, Map<String, String> properties) {

    String originComment = StringIdentifier.removeIdFromComment(comment);
    if (StringUtils.isNotEmpty(originComment)) {
      throw new UnsupportedOperationException(
          "OceanBase doesn't support set schema comment: " + originComment);
    }

    String createDatabaseSql = String.format("CREATE DATABASE `%s`", databaseName);
    // Append options
    if (MapUtils.isNotEmpty(properties)) {
      throw new UnsupportedOperationException("Properties are not supported yet.");
    }
    LOG.info("Generated create database:{} sql: {}", databaseName, createDatabaseSql);
    return createDatabaseSql;
  }

  @Override
  public String generateDropDatabaseSql(String databaseName, boolean cascade) {
    final String dropDatabaseSql = String.format("DROP DATABASE `%s`", databaseName);
    if (cascade) {
      return dropDatabaseSql;
    }

    try (final Connection connection = this.dataSource.getConnection()) {
      String query = String.format("SHOW TABLES IN `%s`", databaseName);
      try (Statement statement = connection.createStatement()) {
        // Execute the query and check if there exists any tables in the database
        try (ResultSet resultSet = statement.executeQuery(query)) {
          if (resultSet.next()) {
            throw new IllegalStateException(
                String.format(
                    "Database %s is not empty, the value of cascade should be true.",
                    databaseName));
          }
        }
      }
    } catch (SQLException sqlException) {
      throw this.exceptionMapper.toGravitinoException(sqlException);
    }
    return dropDatabaseSql;
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

    return JdbcSchema.builder()
        .withName(dbName)
        .withProperties(ImmutableMap.of())
        .withAuditInfo(AuditInfo.EMPTY)
        .build();
  }

  @Override
  protected boolean isSystemDatabase(String dbName) {
    return SYS_OCEANBASE_DATABASE_NAMES.contains(dbName.toLowerCase(Locale.ROOT));
  }
}
