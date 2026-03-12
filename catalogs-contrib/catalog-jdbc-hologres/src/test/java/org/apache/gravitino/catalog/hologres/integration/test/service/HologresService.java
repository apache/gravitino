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
package org.apache.gravitino.catalog.hologres.integration.test.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.jdbc.JdbcSchema;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.meta.AuditInfo;

/**
 * A helper service that directly connects to Hologres via JDBC for verification in integration
 * tests.
 */
public class HologresService {

  private final Connection connection;

  public HologresService(String jdbcUrl, String username, String password) {
    try {
      connection = DriverManager.getConnection(jdbcUrl, username, password);
    } catch (SQLException e) {
      throw new RuntimeException("Failed to connect to Hologres: " + e.getMessage(), e);
    }
  }

  public NameIdentifier[] listSchemas(Namespace namespace) {
    List<String> schemas = new ArrayList<>();
    try (ResultSet resultSet = connection.getMetaData().getSchemas(connection.getCatalog(), null)) {
      while (resultSet.next()) {
        schemas.add(resultSet.getString("TABLE_SCHEM"));
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return schemas.stream()
        .map(s -> NameIdentifier.of(org.apache.commons.lang3.ArrayUtils.add(namespace.levels(), s)))
        .toArray(NameIdentifier[]::new);
  }

  public JdbcSchema loadSchema(NameIdentifier schemaIdent) {
    String schemaName = schemaIdent.name();
    String query = "SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname = ?";
    try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
      preparedStatement.setString(1, schemaName);
      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        if (!resultSet.next()) {
          throw new NoSuchSchemaException("Schema %s could not be found", schemaName);
        }
        return JdbcSchema.builder().withName(schemaName).withAuditInfo(AuditInfo.EMPTY).build();
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
