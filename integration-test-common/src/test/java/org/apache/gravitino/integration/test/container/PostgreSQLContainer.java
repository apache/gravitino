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

public class PostgreSQLContainer extends DatabaseContainer {
  public static final Logger LOG = LoggerFactory.getLogger(PostgreSQLContainer.class);
  public static final PGImageName DEFAULT_IMAGE = PGImageName.VERSION_13;
  public static final String HOST_NAME = "gravitino-ci-pg";
  public static final int PG_PORT = 5432;
  public static final String USER_NAME = "root";
  public static final String PASSWORD = "root";
  private static final String DATA_DIR = "/var/lib/postgresql/data";

  public static Builder builder() {
    return new Builder();
  }

  protected PostgreSQLContainer(
      String image,
      Durability durability,
      String hostName,
      Set<Integer> ports,
      Map<String, String> extraHosts,
      Map<String, String> filesToMount,
      Map<String, String> envVars,
      Optional<Network> network) {
    super(
        // The image may be a mirror or a custom tag; the module only needs a PostgreSQL server.
        new org.testcontainers.containers.PostgreSQLContainer<>(
                DockerImageName.parse(image).asCompatibleSubstituteFor("postgres"))
            .withUsername(USER_NAME)
            .withPassword(PASSWORD)
            // getJdbcUrl() names no database, so connections use the one named after the user.
            .withDatabaseName(USER_NAME),
        durability,
        hostName,
        ports,
        extraHosts,
        filesToMount,
        envVars,
        network);
    // Same path as the tmpfs mount, so a newer image cannot place the data elsewhere.
    db.withEnv("PGDATA", DATA_DIR);
    applyDurability(
        DATA_DIR,
        "postgres",
        "-c",
        "fsync=off",
        "-c",
        "synchronous_commit=off",
        "-c",
        "full_page_writes=off");
  }

  @Override
  protected void setupContainer() {
    super.setupContainer();
    withLogConsumer(new PrintingContainerLog(format("%-14s| ", "PostgreSQLContainer")));
  }

  @Override
  public void start() {
    super.start();
    logDurability(
        List.of("data_directory", "fsync", "synchronous_commit", "full_page_writes"),
        "psql",
        "-U",
        getUsername(),
        "-d",
        "postgres",
        "--no-align",
        "--tuples-only",
        "--field-separator=\t",
        "-c",
        "SELECT current_setting('data_directory'), current_setting('fsync'),"
            + " current_setting('synchronous_commit'), current_setting('full_page_writes')");
  }

  public void createDatabase(TestDatabaseName testDatabaseName) {
    try (Connection connection = db.createConnection("");
        Statement statement = connection.createStatement()) {

      String query = format("CREATE DATABASE \"%s\"", testDatabaseName);
      statement.execute(query);
      LOG.info(format("PostgreSQL container database %s has been created", testDatabaseName));
    } catch (SQLException e) {
      if (e.getMessage()
          .equals(String.format("ERROR: database \"%s\" already exists", testDatabaseName))) {
        LOG.info("PostgreSQL Database {} already exists, skipping", testDatabaseName);
      } else throw new RuntimeException(e);
    }
  }

  /** getJdbcUrl without database name. */
  public String getJdbcUrl() {
    return format("jdbc:postgresql://%s:%d/", getContainerIpAddress(), PG_PORT);
  }

  /** getJdbcUrl with database name. */
  public String getJdbcUrl(TestDatabaseName testDatabaseName) {
    return format("jdbc:postgresql://%s:%d/%s", getContainerIpAddress(), PG_PORT, testDatabaseName);
  }

  public String getDriverClassName(TestDatabaseName testDatabaseName) throws SQLException {
    return DriverManager.getDriver(getJdbcUrl(testDatabaseName)).getClass().getName();
  }

  public static class Builder
      extends DatabaseContainer.Builder<PostgreSQLContainer.Builder, PostgreSQLContainer> {

    private Builder() {
      this.image = DEFAULT_IMAGE.toString();
      this.hostName = HOST_NAME;
      this.exposePorts = ImmutableSet.of(PG_PORT);
    }

    @Override
    public PostgreSQLContainer build() {
      return new PostgreSQLContainer(
          image, durability, hostName, exposePorts, extraHosts, filesToMount, envVars, network);
    }
  }
}
