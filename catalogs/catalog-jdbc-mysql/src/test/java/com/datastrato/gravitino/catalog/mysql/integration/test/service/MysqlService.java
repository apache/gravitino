/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.mysql.integration.test.service;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.catalog.jdbc.JdbcSchema;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.meta.AuditInfo;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.testcontainers.containers.MySQLContainer;

public class MysqlService {

  private Connection connection;

  public MysqlService(MySQLContainer<?> mysqlContainer) {
    String username = mysqlContainer.getUsername();
    String password = mysqlContainer.getPassword();

    try {
      connection =
          DriverManager.getConnection(
              StringUtils.substring(
                  mysqlContainer.getJdbcUrl(), 0, mysqlContainer.getJdbcUrl().lastIndexOf("/")),
              username,
              password);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public NameIdentifier[] listSchemas(Namespace namespace) {
    List<String> databases = new ArrayList<>();
    try (Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SHOW DATABASES")) {
      while (resultSet.next()) {
        databases.add(resultSet.getString(1));
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return databases.stream()
        .map(s -> NameIdentifier.of(ArrayUtils.add(namespace.levels(), s)))
        .toArray(NameIdentifier[]::new);
  }

  public JdbcSchema loadSchema(NameIdentifier schemaIdent) {
    String databaseName = schemaIdent.name();
    String query = "SELECT * FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?";
    try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
      preparedStatement.setString(1, databaseName);

      // Execute the query
      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        if (!resultSet.next()) {
          throw new NoSuchSchemaException(
              "Database %s could not be found in information_schema.SCHEMATA", databaseName);
        }
        String schemaName = resultSet.getString("SCHEMA_NAME");
        return new JdbcSchema.Builder().withName(schemaName).withAuditInfo(AuditInfo.EMPTY).build();
      }
    } catch (final SQLException se) {
      throw new RuntimeException(se);
    }
  }

  public void executeQuery(String sql) {
    try (Statement statement = connection.createStatement()) {
      statement.execute(sql);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public void close() {
    try {
      connection.close();
    } catch (SQLException e) {
      // ignore
    }
  }
}
