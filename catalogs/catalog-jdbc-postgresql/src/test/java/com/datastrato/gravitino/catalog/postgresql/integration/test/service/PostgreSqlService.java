/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.postgresql.integration.test.service;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.catalog.jdbc.JdbcSchema;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.ArrayUtils;
import org.testcontainers.containers.PostgreSQLContainer;

public class PostgreSqlService {

  private final Connection connection;

  private final String database;

  public PostgreSqlService(PostgreSQLContainer<?> postgreSQLContainer) {
    String username = postgreSQLContainer.getUsername();
    String password = postgreSQLContainer.getPassword();

    try {
      String jdbcUrl = postgreSQLContainer.getJdbcUrl();
      String database = new URI(jdbcUrl.substring(jdbcUrl.lastIndexOf("/") + 1)).getPath();
      this.connection = DriverManager.getConnection(jdbcUrl, username, password);
      connection.setCatalog(database);
      this.database = database;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public NameIdentifier[] listSchemas(Namespace namespace) {
    List<String> databases = new ArrayList<>();
    try (PreparedStatement statement =
        connection.prepareStatement(
            "SELECT schema_name FROM information_schema.schemata WHERE catalog_name = ?")) {
      statement.setString(1, database);
      ResultSet resultSet = statement.executeQuery();
      while (resultSet.next()) {
        String databaseName = resultSet.getString(1);
        databases.add(databaseName);
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    return databases.stream()
        .map(s -> NameIdentifier.of(ArrayUtils.add(namespace.levels(), s)))
        .toArray(NameIdentifier[]::new);
  }

  public JdbcSchema loadSchema(NameIdentifier schemaIdent) {
    String schema = schemaIdent.name();
    String sql =
        "SELECT schema_name FROM information_schema.schemata WHERE schema_name = ? AND catalog_name = ?";
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setString(1, schema);
      statement.setString(2, database);
      try (ResultSet resultSet = statement.executeQuery()) {
        if (!resultSet.next()) {
          throw new NoSuchSchemaException("No such schema: %s", schema);
        }
        String schemaName = resultSet.getString(1);
        return new JdbcSchema.Builder().withName(schemaName).build();
      }
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

  public void executeQuery(String sql) {
    try (Statement statement = connection.createStatement()) {
      statement.execute(sql);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
