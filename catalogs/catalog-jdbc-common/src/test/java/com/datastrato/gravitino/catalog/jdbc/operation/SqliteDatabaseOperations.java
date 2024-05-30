/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.jdbc.operation;

import static com.datastrato.gravitino.catalog.jdbc.converter.SqliteExceptionConverter.SCHEMA_ALREADY_EXISTS_CODE;

import com.datastrato.gravitino.catalog.jdbc.JdbcSchema;
import com.datastrato.gravitino.catalog.jdbc.utils.JdbcConnectorUtils;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import com.datastrato.gravitino.meta.AuditInfo;
import com.google.common.base.Preconditions;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;

public class SqliteDatabaseOperations extends JdbcDatabaseOperations {
  private String dbPath;

  public SqliteDatabaseOperations(String baseFileDir) {
    this.dbPath = Preconditions.checkNotNull(baseFileDir);
  }

  @Override
  public void create(String databaseName, String comment, Map<String, String> properties)
      throws SchemaAlreadyExistsException {
    try {
      if (exist(databaseName)) {
        throw new SQLException(
            String.format("Database %s already exists", databaseName),
            "CREATE DATABASE" + databaseName,
            SCHEMA_ALREADY_EXISTS_CODE);
      }
      try (Connection connection =
          DriverManager.getConnection("jdbc:sqlite:/" + dbPath + "/" + databaseName)) {
        JdbcConnectorUtils.executeUpdate(
            connection, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
      }
    } catch (SQLException e) {
      throw exceptionMapper.toGravitinoException(e);
    }
    Preconditions.checkArgument(exist(databaseName), "Database %s does not exist", databaseName);
  }

  public boolean exist(String databaseName) {
    return new File(dbPath + "/" + databaseName).exists();
  }

  @Override
  public List<String> listDatabases() {
    File file = new File(dbPath);
    Preconditions.checkArgument(file.exists(), "Database path %s does not exist", dbPath);
    return Arrays.stream(Objects.requireNonNull(file.listFiles()))
        .map(File::getName)
        .collect(Collectors.toList());
  }

  @Override
  public JdbcSchema load(String databaseName) throws NoSuchSchemaException {
    if (exist(databaseName)) {
      return JdbcSchema.builder().withName(databaseName).withAuditInfo(AuditInfo.EMPTY).build();
    }
    return null;
  }

  @Override
  public boolean delete(String databaseName, boolean cascade) throws NoSuchSchemaException {
    return delete(databaseName);
  }

  public boolean delete(String databaseName) {
    return FileUtils.deleteQuietly(new File(dbPath + "/" + databaseName));
  }

  @Override
  public String generateCreateDatabaseSql(
      String databaseName, String comment, Map<String, String> properties) {
    return null;
  }

  @Override
  public String generateDropDatabaseSql(String databaseName, boolean cascade) {
    return null;
  }
}
