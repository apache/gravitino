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
package org.apache.gravitino.storage.relational;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.integration.test.container.DatabaseProvisioning;

/**
 * Provisions an isolated database for one Core database test-JVM fork on a shared, build-wide
 * MySQL/PostgreSQL container, instead of that fork starting its own container.
 *
 * <p>Active only when the Gradle {@code core} test task's shared-container {@code
 * SharedDbContainerService} (buildSrc) has published admin connection info as system properties
 * under {@link #ADMIN_PROPERTY_PREFIX}; {@link #isEnabled(String)} returns {@code false} otherwise,
 * e.g. when this test class is run directly from an IDE outside the Gradle task, and the caller is
 * expected to fall back to starting a fresh, per-fork container as before.
 *
 * <p>Each call to {@link #provision(String)} allocates and migrates a brand-new database - this
 * relies on {@link BackendTestExtension} only calling it when a genuinely new backend is actually
 * needed (a fresh {@link DatabaseTestContext} fixture, or the first fixture for a test class);
 * reuse across test methods within a class is already handled one layer up, by {@link
 * BackendTestExtension}'s own class-scoped backend cache. Databases provisioned here are never
 * individually dropped; they are discarded together with the shared container when the build ends
 * (see {@code SharedDbContainerService#close()} in buildSrc).
 */
final class SharedCoreDatabaseProvisioner {

  /** System property prefix under which the shared-container BuildService publishes info. */
  static final String ADMIN_PROPERTY_PREFIX = "gravitino.test.db.";

  /** System property carrying the shared-safe connection-pool budget, if published. */
  static final String POOL_MAX_CONNECTIONS_PROPERTY = ADMIN_PROPERTY_PREFIX + "pool.maxConnections";

  private static final AtomicLong DATABASE_COUNTER = new AtomicLong();

  private SharedCoreDatabaseProvisioner() {}

  /**
   * Returns whether the shared-container BuildService has published admin connection info for
   * {@code backendType} ({@code "mysql"} or {@code "postgresql"}).
   */
  static boolean isEnabled(String backendType) {
    return adminUrl(backendType) != null;
  }

  /**
   * Creates and migrates a brand-new, uniquely-named database on the shared container for {@code
   * backendType}, returning its JDBC URL.
   *
   * @throws IllegalStateException if {@link #isEnabled(String)} is {@code false} for {@code
   *     backendType}
   */
  static String provision(String backendType) throws IOException {
    String adminUrl = adminUrl(backendType);
    if (adminUrl == null) {
      throw new IllegalStateException(
          "Shared-container mode is not enabled for backend " + backendType);
    }
    String adminUser = System.getProperty(ADMIN_PROPERTY_PREFIX + backendType + ".user");
    String adminPassword = System.getProperty(ADMIN_PROPERTY_PREFIX + backendType + ".password");

    if ("mysql".equals(backendType)) {
      loadMySQLDriver();
    }

    String dbName = nextDatabaseName();
    DatabaseProvisioning.createDatabaseIfAbsent(adminUrl, adminUser, adminPassword, dbName);
    String dbUrl = buildJdbcUrlForDatabase(adminUrl, dbName);
    runSchemaMigration(backendType, dbUrl, adminUser, adminPassword);
    return dbUrl;
  }

  /**
   * Forces the legacy MySQL JDBC driver class to load before the first {@link
   * DriverManager#getConnection}, mirroring the same workaround {@code
   * MySQLContainer#createDatabase} already applies for <a
   * href="https://github.com/apache/gravitino/issues/6392">#6392</a> (the driver may not register
   * itself via {@link java.util.ServiceLoader} in time on some JVMs/classloaders).
   */
  private static void loadMySQLDriver() {
    try {
      Class.forName("com.mysql.jdbc.Driver");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Failed to load MySQL JDBC driver", e);
    }
  }

  @Nullable
  private static String adminUrl(String backendType) {
    String value = System.getProperty(ADMIN_PROPERTY_PREFIX + backendType + ".adminUrl");
    return (value == null || value.isEmpty()) ? null : value;
  }

  /**
   * Computes a fresh database name: {@code "gravitino_core_" + sanitize(worker) + "_" + counter},
   * where {@code worker} is {@code System.getProperty("org.gradle.test.worker", "local")} and
   * {@code counter} is a per-JVM monotonically increasing value - unlike a name derived only from a
   * test class name, this can never collide within one fork no matter how many databases that fork
   * ends up provisioning, and the worker prefix keeps distinct forks apart from each other.
   */
  static String nextDatabaseName() {
    String worker =
        System.getProperty("org.gradle.test.worker", "local").replaceAll("[^a-zA-Z0-9_$]", "_");
    String name = "gravitino_core_" + worker + "_" + DATABASE_COUNTER.incrementAndGet();
    if (!DatabaseProvisioning.isValidDatabaseName(name)) {
      // Only reachable for an absurdly long worker id - fail loudly instead of silently
      // colliding or truncating into an unrelated name.
      throw new IllegalStateException("Computed an invalid shared database name: " + name);
    }
    return name;
  }

  /**
   * Derives the JDBC URL to connect directly to database {@code dbName} on the same server as
   * {@code adminUrl}, by replacing whatever database name (if any) {@code adminUrl} points at.
   */
  static String buildJdbcUrlForDatabase(String adminUrl, String dbName) {
    int queryIndex = adminUrl.indexOf('?');
    String base = queryIndex >= 0 ? adminUrl.substring(0, queryIndex) : adminUrl;
    String query = queryIndex >= 0 ? adminUrl.substring(queryIndex) : "";

    int schemeEnd = base.indexOf("://");
    int hostStart = schemeEnd >= 0 ? schemeEnd + 3 : 0;
    int pathStart = base.indexOf('/', hostStart);
    String hostPart = pathStart >= 0 ? base.substring(0, pathStart) : base;
    return hostPart + "/" + dbName + query;
  }

  /**
   * Runs the same OSS schema SQL that {@link
   * org.apache.gravitino.integration.test.util.BaseIT#startAndInitMySQLBackend()} / {@link
   * org.apache.gravitino.integration.test.util.BaseIT#startAndInitPGBackend()} apply, against the
   * given already-created, empty database.
   */
  private static void runSchemaMigration(String type, String dbUrl, String user, String password)
      throws IOException {
    String gravitinoHome =
        Objects.requireNonNull(
            System.getenv("GRAVITINO_ROOT_DIR"),
            "GRAVITINO_ROOT_DIR must be set to locate the schema SQL to migrate a shared "
                + type
                + " database");
    String schemaContent = loadSchemaContent(gravitinoHome, type);

    String[] statements =
        Arrays.stream(schemaContent.split(";"))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .toArray(String[]::new);

    String currentStatement = "";
    try (Connection connection = DriverManager.getConnection(dbUrl, user, password);
        Statement statement = connection.createStatement()) {
      for (String sql : statements) {
        currentStatement = sql;
        statement.execute(sql);
      }
    } catch (SQLException e) {
      throw new RuntimeException(
          "Failed to migrate shared "
              + type
              + " database "
              + dbUrl
              + ", statement: "
              + currentStatement,
          e);
    }
  }

  private static String loadSchemaContent(String gravitinoHome, String databaseType)
      throws IOException {
    String version = ConfigConstants.CURRENT_SCRIPT_VERSION;
    Path schema =
        Path.of(
            gravitinoHome,
            "scripts",
            databaseType,
            String.format("schema-%s-%s.sql", version, databaseType));
    return Files.readString(schema, StandardCharsets.UTF_8);
  }
}
