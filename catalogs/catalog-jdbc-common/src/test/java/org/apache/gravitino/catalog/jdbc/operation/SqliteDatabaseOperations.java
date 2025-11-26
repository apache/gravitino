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
package org.apache.gravitino.catalog.jdbc.operation;

import com.google.common.base.Preconditions;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.catalog.jdbc.JdbcSchema;
import org.apache.gravitino.catalog.jdbc.converter.SqliteExceptionConverter;
import org.apache.gravitino.catalog.jdbc.utils.JdbcConnectorUtils;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.meta.AuditInfo;

public class SqliteDatabaseOperations extends JdbcDatabaseOperations {
  private String dbPath;

  public SqliteDatabaseOperations(String baseFileDir) {
    Preconditions.checkArgument(baseFileDir != null, "Base file directory cannot be null");
    this.dbPath = baseFileDir;
  }

  @Override
  public void create(String databaseName, String comment, Map<String, String> properties)
      throws SchemaAlreadyExistsException {
    try {
      if (exist(databaseName)) {
        throw new SQLException(
            String.format("Database %s already exists", databaseName),
            "CREATE DATABASE" + databaseName,
            SqliteExceptionConverter.SCHEMA_ALREADY_EXISTS_CODE);
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

  @Override
  public boolean exist(String databaseName) {
    return new File(dbPath + "/" + databaseName).exists();
  }

  @Override
  public List<String> listDatabases() {
    try {
      File file = new File(dbPath);
      Preconditions.checkArgument(file.exists(), "Database path %s does not exist", dbPath);
      return Arrays.stream(Objects.requireNonNull(file.listFiles()))
          .map(File::getName)
          .collect(Collectors.toList());
    } catch (Exception e) {
      throw new GravitinoRuntimeException(
          e, "Failed to list databases in %s: %s", dbPath, e.getMessage());
    }
  }

  @Override
  public JdbcSchema load(String databaseName) throws NoSuchSchemaException {
    if (exist(databaseName)) {
      return JdbcSchema.builder().withName(databaseName).withAuditInfo(AuditInfo.EMPTY).build();
    }
    return null;
  }

  @Override
  protected boolean supportSchemaComment() {
    return false;
  }

  @Override
  protected Set<String> createSysDatabaseNameSet() {
    return Collections.emptySet();
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
