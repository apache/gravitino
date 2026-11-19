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

import static java.lang.String.format;

import com.google.common.collect.ImmutableSet;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

public class MySQLContainer extends DatabaseContainer {
  public static final Logger LOG = LoggerFactory.getLogger(MySQLContainer.class);

  public static final String DEFAULT_IMAGE = "mysql:8.0";
  public static final String HOST_NAME = "gravitino-ci-mysql";
  public static final int MYSQL_PORT = 3306;
  public static final String USER_NAME = "root";
  public static final String PASSWORD = "root";
  private static final String DATA_DIR = "/var/lib/mysql";
  private static final String DEFAULT_CONFIG = "mysql-conf/default";
  private static final String DISPOSABLE_CONFIG = "mysql-conf/disposable";

  public static Builder builder() {
    return new Builder();
  }

  protected MySQLContainer(
      String image,
      Durability durability,
      String hostName,
      Set<Integer> ports,
      Map<String, String> extraHosts,
      Map<String, String> filesToMount,
      Map<String, String> envVars,
      Optional<Network> network) {
    super(
        module(image, durability),
        durability,
        hostName,
        ports,
        extraHosts,
        filesToMount,
        envVars,
        network);
    applyDurability(DATA_DIR);
  }

  @Override
  protected void setupContainer() {
    super.setupContainer();
    withLogConsumer(new PrintingContainerLog(format("%-14s| ", "MySQLContainer")));
  }

  @Override
  public void start() {
    super.start();
    logDurability(
        List.of("datadir", "innodb_flush_log_at_trx_commit", "log_bin", "innodb_doublewrite"),
        "mysql",
        "-u" + getUsername(),
        "-p" + getPassword(),
        "--batch",
        "--skip-column-names",
        "-e",
        "SELECT @@datadir, @@innodb_flush_log_at_trx_commit, @@log_bin, @@innodb_doublewrite");
  }

  public void createDatabase(TestDatabaseName testDatabaseName) {
    // validate database name to ensure it only contains safe characters
    String databaseName = testDatabaseName.toString();
    if (!isValidDatabaseName(databaseName)) {
      throw new IllegalArgumentException("Invalid database name: " + databaseName);
    }

    try (Connection connection = db.createConnection("");
        Statement statement = connection.createStatement()) {
      String query = String.format("CREATE DATABASE IF NOT EXISTS `%s`;", databaseName);
      // FIXME: String, which is used in SQL, can be unsafe
      statement.execute(query);
      LOG.info(String.format("MySQL container database %s has been created", testDatabaseName));
    } catch (Exception e) {
      throw new RuntimeException("Failed to create database", e);
    }
  }

  private boolean isValidDatabaseName(String databaseName) {
    if (databaseName == null || databaseName.isEmpty()) {
      return false;
    }

    if (databaseName.length() > 64) {
      return false;
    }

    return databaseName.matches("^[a-zA-Z0-9_$]+$");
  }

  public String getJdbcUrl() {
    return format("jdbc:mysql://%s:%d", getContainerIpAddress(), MYSQL_PORT);
  }

  public String getJdbcUrl(TestDatabaseName testDatabaseName) {
    return format("jdbc:mysql://%s:%d/%s", getContainerIpAddress(), MYSQL_PORT, testDatabaseName);
  }

  public String getDriverClassName(TestDatabaseName testDatabaseName) throws SQLException {
    return DriverManager.getDriver(getJdbcUrl(testDatabaseName)).getClass().getName();
  }

  private static org.testcontainers.containers.MySQLContainer<?> module(
      String image, Durability durability) {
    // The image may be a mirror or a custom tag; the module only needs a MySQL server.
    return new org.testcontainers.containers.MySQLContainer<>(
            DockerImageName.parse(image).asCompatibleSubstituteFor("mysql"))
        .withUsername(USER_NAME)
        .withPassword(PASSWORD)
        // The module always creates a database. Its default, "test", is also a schema name catalog
        // tests create, so name MySQL's own system database instead.
        .withDatabaseName("mysql")
        // Our own config replaces the one Testcontainers 1.x bundles, so the server settings do not
        // depend on the Testcontainers version; it carries the disposable settings too.
        .withConfigurationOverride(
            durability == Durability.DISPOSABLE ? DISPOSABLE_CONFIG : DEFAULT_CONFIG)
        // The module's readiness check queries the database and ignores BaseContainer's startup
        // timeout, so it gets the same budget here.
        .withStartupTimeoutSeconds(300);
  }

  public static class Builder
      extends DatabaseContainer.Builder<MySQLContainer.Builder, MySQLContainer> {

    private Builder() {
      this.image = DEFAULT_IMAGE;
      this.hostName = HOST_NAME;
      this.exposePorts = ImmutableSet.of(MYSQL_PORT);
    }

    @Override
    public MySQLContainer build() {
      return new MySQLContainer(
          image, durability, hostName, exposePorts, extraHosts, filesToMount, envVars, network);
    }
  }
}
