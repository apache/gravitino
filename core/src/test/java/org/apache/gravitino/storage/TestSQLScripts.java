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

import static org.apache.gravitino.integration.test.util.TestDatabaseName.MYSQL_JDBC_BACKEND;
import static org.apache.gravitino.integration.test.util.TestDatabaseName.PG_JDBC_BACKEND;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.integration.test.container.MySQLContainer;
import org.apache.gravitino.integration.test.container.PostgreSQLContainer;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestSQLScripts extends BaseIT {

  private String jdbcBackend;
  private Path scriptDir;
  @TempDir private File tempDir;
  private Map<String, Connection> versionConnections;

  @BeforeAll
  public void startIntegrationTest() {
    jdbcBackend = System.getenv("jdbcBackend");
    Assertions.assertNotNull(jdbcBackend, "jdbcBackend environment variable is not set");

    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    Assertions.assertNotNull(gravitinoHome, "GRAVITINO_HOME environment variable is not set");
    scriptDir = Path.of(gravitinoHome, "scripts", jdbcBackend.toLowerCase());
    Assertions.assertTrue(Files.exists(scriptDir), "Script directory does not exist: " + scriptDir);
    versionConnections = new HashMap<>();
  }

  @AfterAll
  public void stopIntegrationTest() {
    // Close all connections
    for (Connection conn : versionConnections.values()) {
      if (conn != null) {
        try {
          conn.close();
        } catch (SQLException e) {
          // Ignore
        }
      }
    }
    versionConnections.clear();
  }

  @Test
  public void testSQLScripts() throws SQLException {
    File[] scriptFiles = scriptDir.toFile().listFiles();
    Assertions.assertNotNull(scriptFiles, "No script files found in " + scriptDir);
    // Sort files to ensure the correct execution order (schema -> upgrade)
    Arrays.sort(scriptFiles, Comparator.comparing(File::getName));

    // A map to store connections for different schema versions
    Pattern schemaPattern =
        Pattern.compile("schema-([\\d.]+)-" + jdbcBackend.toLowerCase() + "\\.sql");
    Pattern upgradePattern =
        Pattern.compile("upgrade-([\\d.]+)-to-([\\d.]+)-" + jdbcBackend.toLowerCase() + "\\.sql");
    Pattern metricsPattern =
        Pattern.compile("iceberg-metrics-schema-([\\d.]+)-" + jdbcBackend.toLowerCase() + "\\.sql");

    for (File scriptFile : scriptFiles) {
      Matcher schemaMatcher = schemaPattern.matcher(scriptFile.getName());
      Matcher upgradeMatcher = upgradePattern.matcher(scriptFile.getName());
      Matcher metricsMatcher = metricsPattern.matcher(scriptFile.getName());

      if (schemaMatcher.matches()) {
        String version = schemaMatcher.group(1);
        String dbName = "schema_" + version.replace('.', '_');

        Connection conn = getSQLConnection(jdbcBackend, dbName);
        Assertions.assertDoesNotThrow(
            () -> executeSQLScript(conn, scriptFile),
            "Failed to execute schema script for version " + version);
        versionConnections.put(version, conn);

      } else if (upgradeMatcher.matches()) {
        String fromVersion = upgradeMatcher.group(1);

        Connection conn = versionConnections.get(fromVersion);
        Assertions.assertNotNull(
            conn, "No existing database connection found for version " + fromVersion);
        Assertions.assertDoesNotThrow(
            () -> executeSQLScript(conn, scriptFile),
            "Failed to execute upgrade script" + " in file " + scriptFile.getName());
      } else if (metricsMatcher.matches()) {
        // ignore iceberg metrics scripts for now
      } else {
        Assertions.fail("Unrecognized script file name: " + scriptFile.getName());
      }
    }
  }

  private void executeSQLScript(Connection connection, File scriptFile) throws IOException {
    String sqlContent = FileUtils.readFileToString(scriptFile, "UTF-8");
    Arrays.stream(sqlContent.split(";"))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .forEach(
            sql -> {
              try (Statement stmt = connection.createStatement()) {
                stmt.execute(sql);
              } catch (SQLException e) {
                throw new RuntimeException("Failed to execute SQL: " + sql, e);
              }
            });
  }

  private Connection getSQLConnection(String jdbcBackend, String dBName) throws SQLException {
    if ("h2".equalsIgnoreCase(jdbcBackend)) {
      // Use a temporary directory for the database files
      String dbPath = new File(tempDir, dBName).getAbsolutePath();
      // AUTO_SERVER=TRUE allows multiple connections to the same database in the same process
      String url = String.format("jdbc:h2:file:%s;MODE=MYSQL;AUTO_SERVER=TRUE", dbPath);
      return DriverManager.getConnection(url, "sa", "");

    } else if ("mysql".equalsIgnoreCase(jdbcBackend)) {
      containerSuite.startMySQLContainer(MYSQL_JDBC_BACKEND);
      MySQLContainer mySQLContainer = containerSuite.getMySQLContainer();
      String mysqlUrl = mySQLContainer.getJdbcUrl(MYSQL_JDBC_BACKEND);
      Connection connection =
          DriverManager.getConnection(
              StringUtils.substring(mysqlUrl, 0, mysqlUrl.lastIndexOf("/")), "root", "root");
      Statement statement = connection.createStatement();
      statement.execute("create database if not exists " + dBName);
      connection.setCatalog(dBName);
      return connection;

    } else if ("PostgreSQL".equalsIgnoreCase(jdbcBackend)) {
      containerSuite.startPostgreSQLContainer(PG_JDBC_BACKEND);
      PostgreSQLContainer pgContainer = containerSuite.getPostgreSQLContainer();
      String pgUrlWithoutSchema = pgContainer.getJdbcUrl(PG_JDBC_BACKEND);
      Connection connection =
          DriverManager.getConnection(
              pgUrlWithoutSchema, pgContainer.getUsername(), pgContainer.getPassword());
      connection.setCatalog(PG_JDBC_BACKEND.toString().toLowerCase());
      Statement statement = connection.createStatement();
      statement.execute("create schema if not exists " + dBName);
      connection.setSchema(dBName);
      return connection;
    }
    Assertions.fail("Unsupported JDBC backend: " + jdbcBackend);
    return null;
  }
}
