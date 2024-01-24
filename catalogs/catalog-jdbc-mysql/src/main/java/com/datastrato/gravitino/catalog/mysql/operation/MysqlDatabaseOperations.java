/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.mysql.operation;

import com.datastrato.gravitino.catalog.jdbc.JdbcSchema;
import com.datastrato.gravitino.catalog.jdbc.operation.JdbcDatabaseOperations;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.meta.AuditInfo;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

/** Database operations for MySQL. */
public class MysqlDatabaseOperations extends JdbcDatabaseOperations {

  public static final Set<String> SYS_MYSQL_DATABASE_NAMES =
      Collections.unmodifiableSet(
          new HashSet<String>() {
            {
              add("information_schema");
              add("mysql");
              add("performance_schema");
              add("sys");
            }
          });

  @Override
  public String generateCreateDatabaseSql(
      String databaseName, String comment, Map<String, String> properties) {
    if (StringUtils.isNotEmpty(comment)) {
      LOG.warn(
          "Ignoring comment option on database create. mysql does not support comment option on database create.");
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
  public JdbcSchema load(String databaseName) throws NoSuchSchemaException {
    try (final Connection connection = this.dataSource.getConnection()) {
      String query = "SELECT * FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?";
      try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
        preparedStatement.setString(1, databaseName);

        // Execute the query
        try (ResultSet resultSet = preparedStatement.executeQuery()) {
          if (!resultSet.next()) {
            throw new NoSuchSchemaException(
                String.format(
                    "Database %s could not be found in information_schema.SCHEMATA", databaseName));
          }
          String schemaName = resultSet.getString("SCHEMA_NAME");
          // Mysql currently only supports these two attributes
          String characterSetName = resultSet.getString("DEFAULT_CHARACTER_SET_NAME");
          String collationName = resultSet.getString("DEFAULT_COLLATION_NAME");
          return new JdbcSchema.Builder()
              .withName(schemaName)
              .withProperties(
                  new HashMap<String, String>() {
                    {
                      put("CHARACTER SET", characterSetName);
                      put("COLLATE", collationName);
                    }
                  })
              .withAuditInfo(AuditInfo.EMPTY)
              .build();
        }
      }
    } catch (final SQLException se) {
      throw this.exceptionMapper.toGravitinoException(se);
    }
  }

  @Override
  protected boolean isSystemDatabase(String dbName) {
    return SYS_MYSQL_DATABASE_NAMES.contains(dbName.toLowerCase(Locale.ROOT));
  }
}
