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
package com.datastrato.gravitino.catalog.mysql.integration.test.service;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.catalog.jdbc.JdbcSchema;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.integration.test.container.MySQLContainer;
import com.datastrato.gravitino.integration.test.util.TestDatabaseName;
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

public class MysqlService {

  private Connection connection;

  public MysqlService(MySQLContainer mysqlContainer, TestDatabaseName testDBName) {
    String username = mysqlContainer.getUsername();
    String password = mysqlContainer.getPassword();

    try {
      connection =
          DriverManager.getConnection(
              StringUtils.substring(
                  mysqlContainer.getJdbcUrl(testDBName),
                  0,
                  mysqlContainer.getJdbcUrl(testDBName).lastIndexOf("/")),
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
