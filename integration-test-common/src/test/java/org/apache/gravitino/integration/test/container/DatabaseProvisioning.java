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
package org.apache.gravitino.integration.test.container;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared, database-agnostic helper for provisioning a test database on an already-running MySQL or
 * PostgreSQL container.
 *
 * <p>This class holds the validated "create database if it does not already exist" logic that used
 * to live separately inside {@link MySQLContainer#createDatabase(
 * org.apache.gravitino.integration.test.util.TestDatabaseName)} and {@link
 * PostgreSQLContainer#createDatabase(org.apache.gravitino.integration.test.util.TestDatabaseName)}.
 * Both container classes now delegate to {@link #createDatabaseIfAbsent(String, String, String,
 * String)}, and any other caller (for example a shared-container test extension) that only has a
 * JDBC admin URL, credentials, and a database name can use it directly without going through a
 * {@code TestDatabaseName} enum value or a container instance.
 */
public class DatabaseProvisioning {

  private static final Logger LOG = LoggerFactory.getLogger(DatabaseProvisioning.class);

  private static final int MAX_DATABASE_NAME_LENGTH = 63;

  private static final String VALID_DATABASE_NAME_REGEX = "^[a-zA-Z0-9_$]+$";

  private static final String MYSQL_JDBC_URL_PREFIX = "jdbc:mysql:";

  private static final String POSTGRESQL_JDBC_URL_PREFIX = "jdbc:postgresql:";

  /** PostgreSQL's SQLSTATE for "a database with this name already exists". */
  private static final String POSTGRESQL_DUPLICATE_DATABASE_SQLSTATE = "42P04";

  // Private constructor to prevent instantiation of this static helper class.
  private DatabaseProvisioning() {}

  /**
   * Creates a database on the target MySQL or PostgreSQL server if it does not already exist.
   *
   * <p>The dialect (MySQL vs. PostgreSQL) is inferred from the scheme of {@code adminJdbcUrl}
   * ({@code jdbc:mysql:...} or {@code jdbc:postgresql:...}). {@code adminJdbcUrl} must be a JDBC
   * URL that can be connected to without naming {@code dbName} itself, e.g. the server root URL or
   * another already-existing database on the same server.
   *
   * <p>For MySQL this issues {@code CREATE DATABASE IF NOT EXISTS <dbName>}. PostgreSQL has no
   * {@code IF NOT EXISTS} clause for {@code CREATE DATABASE}, so this first checks {@code
   * pg_database} for an existing row and only issues {@code CREATE DATABASE "<dbName>"} when none
   * is found.
   *
   * @param adminJdbcUrl a JDBC URL that can be connected to without naming {@code dbName}
   * @param user the database user used to connect and create the database
   * @param password the password for {@code user}
   * @param dbName the name of the database to create; validated against {@link
   *     #isValidDatabaseName(String)}
   * @throws IllegalArgumentException if {@code dbName} is invalid, or {@code adminJdbcUrl} is
   *     neither a MySQL nor a PostgreSQL JDBC URL
   * @throws RuntimeException if the database cannot be created
   *     <p>Note for existing {@link MySQLContainer#createDatabase}/{@link
   *     PostgreSQLContainer#createDatabase} callers migrated onto this helper: the accepted name
   *     length cap is 63 characters (PostgreSQL's identifier limit), tighter than the 64 {@code
   *     MySQLContainer} previously allowed on its own; every {@code TestDatabaseName} value in this
   *     repo is well under that either way. An invalid name now surfaces as a bare {@link
   *     IllegalArgumentException} rather than one wrapped in a {@code RuntimeException}.
   */
  public static void createDatabaseIfAbsent(
      String adminJdbcUrl, String user, String password, String dbName) {
    if (!isValidDatabaseName(dbName)) {
      throw new IllegalArgumentException("Invalid database name: " + dbName);
    }

    if (adminJdbcUrl.startsWith(MYSQL_JDBC_URL_PREFIX)) {
      createMySQLDatabaseIfAbsent(adminJdbcUrl, user, password, dbName);
    } else if (adminJdbcUrl.startsWith(POSTGRESQL_JDBC_URL_PREFIX)) {
      createPostgreSQLDatabaseIfAbsent(adminJdbcUrl, user, password, dbName);
    } else {
      throw new IllegalArgumentException(
          "Unsupported JDBC URL for database provisioning: " + adminJdbcUrl);
    }
  }

  /**
   * Validates that {@code databaseName} only contains safe characters and does not exceed the
   * shared length cap, so it can be interpolated into a {@code CREATE DATABASE} statement.
   *
   * @param databaseName the candidate database name
   * @return {@code true} if the name is non-null, non-empty, at most {@value
   *     #MAX_DATABASE_NAME_LENGTH} characters, and matches {@code ^[a-zA-Z0-9_$]+$}
   */
  public static boolean isValidDatabaseName(String databaseName) {
    if (databaseName == null || databaseName.isEmpty()) {
      return false;
    }

    if (databaseName.length() > MAX_DATABASE_NAME_LENGTH) {
      return false;
    }

    return databaseName.matches(VALID_DATABASE_NAME_REGEX);
  }

  private static void createMySQLDatabaseIfAbsent(
      String adminJdbcUrl, String user, String password, String dbName) {
    try (Connection connection = DriverManager.getConnection(adminJdbcUrl, user, password);
        Statement statement = connection.createStatement()) {
      executeCreateMySQLDatabase(statement, dbName);
    } catch (SQLException e) {
      throw new RuntimeException("Failed to create MySQL database " + dbName, e);
    }
  }

  /**
   * Issues the MySQL {@code CREATE DATABASE IF NOT EXISTS} statement against an already-open {@link
   * Statement}. Package-private (rather than folded into {@link #createMySQLDatabaseIfAbsent}) so
   * it can be unit-tested against a mocked {@link Statement} without a real database connection.
   */
  static void executeCreateMySQLDatabase(Statement statement, String dbName) throws SQLException {
    // FIXME: dbName is validated by isValidDatabaseName(), but interpolating it into SQL is
    // still not ideal.
    statement.execute(String.format("CREATE DATABASE IF NOT EXISTS `%s`", dbName));
    LOG.info("MySQL database {} has been created (or already existed)", dbName);
  }

  private static void createPostgreSQLDatabaseIfAbsent(
      String adminJdbcUrl, String user, String password, String dbName) {
    try (Connection connection = DriverManager.getConnection(adminJdbcUrl, user, password);
        Statement statement = connection.createStatement()) {
      executeCreatePostgreSQLDatabase(statement, dbName);
    } catch (SQLException e) {
      throw new RuntimeException("Failed to create PostgreSQL database " + dbName, e);
    }
  }

  /**
   * Issues the PostgreSQL exists-check-then-{@code CREATE DATABASE} sequence against an
   * already-open {@link Statement}, tolerating SQLSTATE {@value
   * #POSTGRESQL_DUPLICATE_DATABASE_SQLSTATE} (duplicate database) as success. Package-private
   * (rather than folded into {@link #createPostgreSQLDatabaseIfAbsent}) so it can be unit-tested
   * against a mocked {@link Statement}/{@link ResultSet} without a real database connection.
   */
  static void executeCreatePostgreSQLDatabase(Statement statement, String dbName)
      throws SQLException {
    if (postgreSQLDatabaseExists(statement, dbName)) {
      LOG.info("PostgreSQL database {} already exists, skipping creation", dbName);
      return;
    }

    // PostgreSQL's CREATE DATABASE has no IF NOT EXISTS clause, so existence is checked above.
    // That check-then-create is not race-safe on its own (another caller could create dbName
    // between the check and this statement), so a duplicate-database error here is still
    // treated as success rather than propagated.
    try {
      statement.execute(String.format("CREATE DATABASE \"%s\"", dbName));
      LOG.info("PostgreSQL database {} has been created", dbName);
    } catch (SQLException e) {
      if (POSTGRESQL_DUPLICATE_DATABASE_SQLSTATE.equals(e.getSQLState())) {
        LOG.info(
            "PostgreSQL database {} was created concurrently by another caller, skipping", dbName);
        return;
      }
      throw e;
    }
  }

  static boolean postgreSQLDatabaseExists(Statement statement, String dbName) throws SQLException {
    String query = String.format("SELECT 1 FROM pg_database WHERE datname = '%s'", dbName);
    try (ResultSet resultSet = statement.executeQuery(query)) {
      return resultSet.next();
    }
  }
}
