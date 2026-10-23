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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.apache.ranger.RangerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.utility.MountableFile;

public class RangerContainer extends BaseContainer {
  public static final Logger LOG = LoggerFactory.getLogger(RangerContainer.class);

  public static final String DEFAULT_IMAGE = TestImages.RANGER;
  public static final String HOST_NAME = "gravitino-ci-ranger";
  public static final int RANGER_SERVER_PORT = 6080;
  public RangerClient rangerClient;
  private String rangerUrl;

  /**
   * for kerberos authentication: <br>
   * authType = "kerberos" <br>
   * username = principal <br>
   * password = path of the keytab file <br>
   */
  public static final String authType = "simple";

  public static final String rangerUserName = "admin";
  // Apache Ranger Password should be minimum 8 characters with min one alphabet and one numeric.
  public static final String rangerPassword = "rangerR0cks!";

  // The MySQL user Ranger admin connects as; Ranger's install.properties default.
  private static final String DB_USER = "rangeradmin";
  // Ranger hive/hdfs Docker startup environment variable name
  public static final String DOCKER_ENV_RANGER_SERVER_URL = "RANGER_SERVER_URL";
  public static final String DOCKER_ENV_RANGER_HDFS_REPOSITORY_NAME = "RANGER_HDFS_REPOSITORY_NAME";
  public static final String DOCKER_ENV_RANGER_HIVE_REPOSITORY_NAME = "RANGER_HIVE_REPOSITORY_NAME";

  /** The Ranger repository a Ranger-enabled Hive reports Hive access to. */
  public static final String HIVE_REPOSITORY = "hiveDev";

  /** The Ranger repository a Ranger-enabled Hive reports HDFS access to. */
  public static final String HDFS_REPOSITORY = "hdfsDev";

  /** The Hadoop user a Ranger-enabled Hive runs as. */
  public static final String HADOOP_USER = "gravitino";

  public static Builder builder() {
    return new Builder();
  }

  // The external MySQL server and database Ranger keeps its policies in, or null for the image's
  // MariaDB.
  @Nullable private final MySQLContainer databaseServer;
  @Nullable private final TestDatabaseName database;

  protected RangerContainer(
      String image,
      String hostName,
      Set<Integer> ports,
      Map<String, String> extraHosts,
      Map<String, String> filesToMount,
      Map<String, String> envVars,
      Optional<Network> network,
      @Nullable MySQLContainer databaseServer,
      @Nullable TestDatabaseName database) {
    super(image, hostName, ports, extraHosts, filesToMount, envVars, network);
    this.databaseServer = databaseServer;
    this.database = database;
  }

  // Without withDatabase, Ranger keeps policies and audits in the image's MariaDB, rebuilt at
  // every start.
  @Override
  protected void applyDisposableData(GenericContainer<?> container) {
    container.withCopyFileToContainer(
        MountableFile.forClasspathResource("disposable/ranger-mariadb.cnf"),
        "/etc/mysql/mariadb.conf.d/99-gravitino-disposable.cnf");
  }

  @Override
  protected void setupContainer() {
    super.setupContainer();
    // Our start script can point Ranger at an external MySQL (RANGER_DB_HOST); without it, it does
    // what the image's own script does.
    container.withCopyFileToContainer(
        MountableFile.forClasspathResource("ranger/start-ranger-services.sh", 0755),
        "/tmp/start-ranger-services.sh");
    withLogConsumer(new PrintingContainerLog(format("%-15s| ", "RangerContainer")));
  }

  @Override
  public void start() {
    if (databaseServer != null) {
      prepareDatabase(databaseServer, database);
    }
    super.start();

    rangerUrl = String.format("http://localhost:%s", this.getMappedPort(RANGER_SERVER_PORT));
    rangerClient = new RangerClient(rangerUrl, authType, rangerUserName, rangerPassword, null);
  }

  // The port opens before Ranger admin serves requests, so the plugins API must answer too.
  @Override
  protected WaitStrategy waitStrategy() {
    return new WaitAllStrategy()
        .withStrategy(Wait.forListeningPort())
        .withStrategy(
            new ProbeWaitStrategy(
                "Ranger admin to answer its plugins API",
                target -> {
                  String url =
                      String.format(
                          "http://%s:%d",
                          target.getHost(), target.getMappedPort(RANGER_SERVER_PORT));
                  new RangerClient(url, authType, rangerUserName, rangerPassword, null)
                      .getPluginsInfo();
                  return true;
                }));
  }

  // The port check's five minutes plus the 30 seconds the plugins API had before.
  @Override
  protected Duration startupTimeout() {
    return Duration.ofMinutes(5).plusSeconds(30);
  }

  /**
   * Drops and recreates {@code database}, since Ranger's setup reuses an existing schema, and
   * creates the {@code rangeradmin} user Ranger's setup logs in as. The user gets {@code
   * mysql_native_password} because the setup connects through the image's MySQL Connector/J 8.0
   * without TLS ({@code useSSL=false}), which refuses {@code caching_sha2_password} unless
   * public-key retrieval is allowed. It gets only what Ranger's {@code dba_script.py} needs: the
   * database with grant option, read access to {@code mysql}, and {@code RELOAD} for {@code FLUSH
   * PRIVILEGES}.
   */
  private static void prepareDatabase(MySQLContainer mysql, TestDatabaseName database) {
    String host = mysql.getContainerIpAddress();
    List<String> statements = new ArrayList<>();
    statements.add(String.format("DROP DATABASE IF EXISTS `%s`", database));
    statements.add(String.format("CREATE DATABASE `%s`", database));
    // setup.sh creates the user for '%', 'localhost' and the db_host value; creating them here
    // first, with the old plugin, makes it skip them.
    for (String userHost : List.of("%", "localhost", host)) {
      String user = String.format("'%s'@'%s'", DB_USER, userHost);
      statements.add(
          String.format(
              "CREATE USER IF NOT EXISTS %s IDENTIFIED WITH mysql_native_password BY '%s'",
              user, rangerPassword));
      statements.add(
          String.format(
              "ALTER USER %s IDENTIFIED WITH mysql_native_password BY '%s'", user, rangerPassword));
      statements.add(
          String.format("GRANT ALL PRIVILEGES ON `%s`.* TO %s WITH GRANT OPTION", database, user));
      statements.add(String.format("GRANT SELECT ON mysql.* TO %s", user));
      statements.add(String.format("GRANT RELOAD ON *.* TO %s", user));
    }
    mysql.execute(statements);
  }

  @Override
  public void close() {
    super.close();
  }

  public static class Builder
      extends BaseContainer.Builder<RangerContainer.Builder, RangerContainer> {

    @Nullable private MySQLContainer databaseServer;
    @Nullable private TestDatabaseName database;

    private Builder() {
      this.image = DEFAULT_IMAGE;
      this.hostName = HOST_NAME;
      this.exposePorts = ImmutableSet.of(RANGER_SERVER_PORT);
      this.envVars =
          ImmutableMap.<String, String>builder().put("RANGER_PASSWORD", rangerPassword).build();
    }

    @Override
    public RangerContainer build() {
      return new RangerContainer(
          image,
          hostName,
          exposePorts,
          extraHosts,
          filesToMount,
          envVars,
          network,
          databaseServer,
          database);
    }

    /**
     * Makes Ranger keep its policies in {@code database} on the given MySQL server instead of the
     * MariaDB inside the image. The container prepares the database when it starts.
     *
     * @param mysql the running MySQL server, reachable from this container by IP
     * @param database the database to use
     * @return this builder
     */
    public Builder withDatabase(MySQLContainer mysql, TestDatabaseName database) {
      this.databaseServer = mysql;
      this.database = database;
      this.envVars =
          ImmutableMap.<String, String>builder()
              .putAll(envVars)
              .put("RANGER_DB_HOST", mysql.getContainerIpAddress())
              .put("RANGER_DB_NAME", database.toString())
              .build();
      return this;
    }
  }
}
