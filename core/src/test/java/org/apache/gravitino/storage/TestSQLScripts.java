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
package org.apache.gravitino.storage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

public class TestSQLScripts extends TestJDBCBackend {

  @TestTemplate
  public void testSQLScripts() throws SQLException, IOException {
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    Assertions.assertNotNull(gravitinoHome, "GRAVITINO_HOME environment variable is not set");
    Path scriptDir = Path.of(gravitinoHome, "scripts", backendType.toLowerCase());

    File[] scriptFiles = scriptDir.toFile().listFiles();
    Assertions.assertNotNull(scriptFiles, "No script files found in " + scriptDir);
    // Sort files to ensure the correct execution order (schema -> upgrade)
    Arrays.sort(scriptFiles, Comparator.comparing(File::getName));

    // A map to store connections for different schema versions
    Pattern schemaPattern =
        Pattern.compile("schema-([\\d.]+)-" + backendType.toLowerCase() + "\\.sql");
    Pattern upgradePattern =
        Pattern.compile("upgrade-([\\d.]+)-to-([\\d.]+)-" + backendType.toLowerCase() + "\\.sql");
    Pattern metricsPattern =
        Pattern.compile(
            "(?:iceberg|optimizer)-metrics-schema-([\\d.]+)-"
                + backendType.toLowerCase()
                + "\\.sql");

    Map<String, List<File>> versionScrips = new HashMap<>();
    for (File scriptFile : scriptFiles) {
      Matcher schemaMatcher = schemaPattern.matcher(scriptFile.getName());
      Matcher upgradeMatcher = upgradePattern.matcher(scriptFile.getName());
      Matcher metricsMatcher = metricsPattern.matcher(scriptFile.getName());

      if (schemaMatcher.matches()) {
        String version = schemaMatcher.group(1);
        versionScrips.computeIfAbsent(version, k -> new ArrayList<>()).add(scriptFile);

      } else if (upgradeMatcher.matches()) {
        String fromVersion = upgradeMatcher.group(1);
        Assertions.assertTrue(
            versionScrips.containsKey(fromVersion), "No schema script found for " + fromVersion);

      } else if (metricsMatcher.matches()) {
        String version = metricsMatcher.group(1);
        versionScrips.computeIfAbsent(version, k -> new ArrayList<>()).add(scriptFile);

      } else {
        Assertions.fail("Unrecognized script file name: " + scriptFile.getName());
      }
    }

    for (List<File> scripts : versionScrips.values()) {
      dropAllTables();
      for (File scriptFile : scripts) {
        List<String> ddls = extractStatements(scriptFile.toPath());

        try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true)) {
          try (Connection connection = sqlSession.getConnection()) {
            try (Statement statement = connection.createStatement()) {
              for (String ddl : ddls) {
                Assertions.assertDoesNotThrow(
                    () -> statement.execute(ddl),
                    "Failed to execute DDL in file " + scriptFile.getName() + "ddl: " + ddl);
              }
            }
          }
        }
      }
    }
  }

  private List<String> extractStatements(Path sqlFile) throws IOException {
    String executableSql =
        Files.readAllLines(sqlFile).stream()
            .map(String::trim)
            .filter(line -> !line.isEmpty())
            .filter(line -> !line.startsWith("--"))
            .reduce((left, right) -> left + "\n" + right)
            .orElse("");

    return Arrays.stream(executableSql.split(";"))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .toList();
  }

  private void dropAllTables() throws SQLException {
    try (SqlSession sqlSession =
        SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true)) {
      try (Connection connection = sqlSession.getConnection()) {
        if ("postgresql".equals(backendType)) {
          dropAllTablesForPostgreSQL(connection);
        } else {
          dropAllTablesForMySQLCompatible(connection);
        }
      }
    }
  }

  private void dropAllTablesForMySQLCompatible(Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement()) {
      String query = "SHOW TABLES";
      List<String> tableList = new ArrayList<>();
      try (ResultSet rs = statement.executeQuery(query)) {
        while (rs.next()) {
          tableList.add(rs.getString(1));
        }
      }
      for (String table : tableList) {
        statement.execute("DROP TABLE " + table);
      }
    }
  }

  private void dropAllTablesForPostgreSQL(Connection connection) throws SQLException {
    String query =
        "SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema()";
    List<String> tableList = new ArrayList<>();
    try (ResultSet rs = connection.createStatement().executeQuery(query)) {
      while (rs.next()) {
        tableList.add(rs.getString(1));
      }
    }

    if (tableList.isEmpty()) {
      return;
    }

    for (String table : tableList) {
      connection.createStatement().execute("DROP TABLE " + table);
    }
  }
  /**
   * Verifies the 1.3 migration expands column names and preserves data on retry.
   *
   * @throws SQLException if the migrated database cannot be queried
   * @throws IOException if the scripts cannot be read
   */
  @TestTemplate
  public void testUpgradeToOneThreeSupportsLongColumnNames() throws SQLException, IOException {
    Path scriptDir = Path.of(System.getenv("GRAVITINO_HOME"), "scripts", backendType.toLowerCase());
    String suffix = "-" + backendType.toLowerCase() + ".sql";
    File upgrade = scriptDir.resolve("upgrade-1.2.0-to-1.3.0" + suffix).toFile();
    dropAllTables();
    executeScript(scriptDir.resolve("schema-1.2.0" + suffix).toFile());
    String insert =
        "INSERT INTO table_column_version_info "
            + "(metalake_id, catalog_id, schema_id, table_id, table_version, column_id, column_name, "
            + "column_position, column_type, column_op_type, audit_info) "
            + "VALUES (1, 2, 3, 4, 1, %d, '%s', 0, 'string', 0, '{}')";
    String oldName = "c".repeat(128);
    executeStatements(List.of(String.format(insert, 1, oldName)), "existing 1.2 column");
    executeScript(upgrade);
    String longName = "c".repeat(300);
    executeStatements(List.of(String.format(insert, 2, longName)), "new long column");
    executeStatements(
        List.of(capacityAlter(scriptDir, suffix)), "repeat column capacity expansion");
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement();
        ResultSet rows =
            statement.executeQuery(
                "SELECT column_name FROM table_column_version_info ORDER BY column_id")) {
      Assertions.assertTrue(rows.next());
      Assertions.assertEquals(oldName, rows.getString(1));
      Assertions.assertTrue(rows.next());
      Assertions.assertEquals(longName, rows.getString(1));
      Assertions.assertFalse(rows.next());
    }
  }

  private void executeScript(File scriptFile) throws IOException, SQLException {
    List<String> ddls = extractStatements(scriptFile.toPath());
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement()) {
      for (String ddl : ddls) {
        Assertions.assertDoesNotThrow(
            () -> statement.execute(ddl),
            "Failed to execute DDL in file " + scriptFile.getName() + " ddl: " + ddl);
      }
    }
  }

  private void executeStatements(List<String> statements, String context) throws SQLException {
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = session.getConnection();
        Statement statement = connection.createStatement()) {
      for (String sql : statements) {
        Assertions.assertDoesNotThrow(() -> statement.execute(sql), context + ", sql: " + sql);
      }
    }
  }

  private String capacityAlter(Path scriptDir, String suffix) throws IOException {
    return extractStatements(scriptDir.resolve("upgrade-1.2.0-to-1.3.0" + suffix)).stream()
        .filter(
            sql ->
                sql.contains("ALTER TABLE")
                    && sql.contains("table_column_version_info")
                    && sql.contains("300"))
        .findFirst()
        .orElseThrow(() -> new IllegalStateException("Missing column capacity migration"));
  }
}
