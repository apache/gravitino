/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.mysql.operation;

import com.datastrato.gravitino.StringIdentifier;
import com.datastrato.gravitino.catalog.jdbc.JdbcSchema;
import com.datastrato.gravitino.catalog.jdbc.operation.JdbcDatabaseOperations;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.meta.AuditInfo;
import com.google.common.collect.ImmutableMap;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

/** Database operations for MySQL. */
public class MysqlDatabaseOperations extends JdbcDatabaseOperations {

  public static final Set<String> SYS_MYSQL_DATABASE_NAMES = createSysMysqlDatabaseNames();

  private static Set<String> createSysMysqlDatabaseNames() {
    Set<String> set = new HashSet<>();
    set.add("information_schema");
    set.add("mysql");
    set.add("performance_schema");
    set.add("sys");
    return Collections.unmodifiableSet(set);
  }

  @Override
  public String generateCreateDatabaseSql(
      String databaseName, String comment, Map<String, String> properties) {
    String originComment = StringIdentifier.removeIdFromComment(comment);
    if (StringUtils.isNotEmpty(originComment)) {
      throw new UnsupportedOperationException(
          "MySQL doesn't support set schema comment: " + originComment);
    }
    StringBuilder sqlBuilder = new StringBuilder("CREATE DATABASE ");

    // Append database name
    sqlBuilder.append("`").append(databaseName).append("`");
    // Append options
    if (MapUtils.isNotEmpty(properties)) {
      // TODO #804 Properties will be unified in the future.
      throw new UnsupportedOperationException("Properties are not supported yet");
      //      sqlBuilder.append("\n");
      //      sqlBuilder.append(
      //          properties.entrySet().stream()
      //              .map(entry -> entry.getKey() + " " + entry.getValue())
      //              .collect(Collectors.joining("\n")));
    }
    String result = sqlBuilder.toString();
    LOG.info("Generated create database:{} sql: {}", databaseName, result);
    return result;
  }

  @Override
  public String generateDropDatabaseSql(String databaseName, boolean cascade) {
    final String dropDatabaseSql = "DROP DATABASE `" + databaseName + "`";
    if (cascade) {
      return dropDatabaseSql;
    }

    try (final Connection connection = this.dataSource.getConnection()) {
      String query = "SHOW TABLES IN " + databaseName;
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
  protected ResultSet getSchema(Connection connection, String databaseName) throws SQLException {
    final DatabaseMetaData metaData = connection.getMetaData();
    // It'd indeed need to call getCatalogs() to get the schema not `getSchemas()` for MySQL.
    return metaData.getCatalogs();
  }


    @Override
  public JdbcSchema load(String databaseName) throws NoSuchSchemaException {
    try (final Connection connection = this.dataSource.getConnection()) {
      ResultSet resultSet = getSchema(connection, databaseName);

      boolean found = false;
      while (resultSet.next()) {
        if (Objects.equals(resultSet.getString(1), databaseName)) {
          found = true;
          break;
        }
      }

      if (!found) {
        throw new NoSuchSchemaException("Database %s could not be found", databaseName);
      }

      return JdbcSchema.builder()
          .withName(databaseName)
          .withProperties(ImmutableMap.of())
          .withAuditInfo(AuditInfo.EMPTY)
          .build();

    } catch (final SQLException se) {
      throw this.exceptionMapper.toGravitinoException(se);
    }
  }

  @Override
  protected boolean isSystemDatabase(String dbName) {
    return SYS_MYSQL_DATABASE_NAMES.contains(dbName.toLowerCase(Locale.ROOT));
  }
}
