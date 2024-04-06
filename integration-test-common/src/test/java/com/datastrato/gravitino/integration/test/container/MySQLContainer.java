/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.container;

import static java.lang.String.format;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import com.datastrato.gravitino.integration.test.util.AbstractIT;
import com.google.common.collect.ImmutableSet;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.rnorth.ducttape.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.Network;

public class MySQLContainer extends BaseContainer {
  public static final Logger LOG = LoggerFactory.getLogger(MySQLContainer.class);

  public static final String DEFAULT_IMAGE = "mysql:8.0";
  public static final String HOST_NAME = "gravitino-ci-mysql";
  public static final int MYSQL_PORT = 3306;
  public static final String USER_NAME = "root";
  public static final String PASSWORD = "root";

  public static Builder builder() {
    return new Builder();
  }

  protected MySQLContainer(
      String image,
      String hostName,
      Set<Integer> ports,
      Map<String, String> extraHosts,
      Map<String, String> filesToMount,
      Map<String, String> envVars,
      Optional<Network> network) {
    super(image, hostName, ports, extraHosts, filesToMount, envVars, network);
  }

  @Override
  protected void setupContainer() {
    super.setupContainer();
    withLogConsumer(new PrintingContainerLog(format("%-14s| ", "MySQLContainer")));
  }

  @Override
  public void start() {
    super.start();
    Preconditions.check("MySQL container startup failed!", checkContainerStatus(5));
  }

  @Override
  protected boolean checkContainerStatus(int retryLimit) {
    int nRetry = 0;
    boolean isMySQLContainerReady = false;
    int sleepTimeMillis = 20_00;
    while (nRetry++ < retryLimit) {
      try {
        String[] commandAndArgs =
            new String[] {
              "mysqladmin",
              "ping",
              "-h",
              "localhost",
              "-u",
              getUsername(),
              String.format("-p%s", getPassword())
            };
        Container.ExecResult execResult = executeInContainer(commandAndArgs);
        if (execResult.getExitCode() != 0) {
          String message =
              format(
                  "Command [%s] exited with %s",
                  String.join(" ", commandAndArgs), execResult.getExitCode());
          LOG.error("{}", message);
          LOG.error("stderr: {}", execResult.getStderr());
          LOG.error("stdout: {}", execResult.getStdout());
        } else {
          LOG.info("MySQL container startup success!");
          isMySQLContainerReady = true;
          break;
        }
        LOG.info(
            "MySQL container is not ready, recheck({}/{}) after {}ms",
            nRetry,
            retryLimit,
            sleepTimeMillis);
        await().atLeast(sleepTimeMillis, TimeUnit.MILLISECONDS);
      } catch (RuntimeException e) {
        LOG.error(e.getMessage(), e);
      }
    }

    return isMySQLContainerReady;
  }

  public void createDatabase(String testDBName) {
    String mySQLJdbcUrl =
        StringUtils.substring(getJdbcUrl(testDBName), 0, getJdbcUrl(testDBName).lastIndexOf("/"));

    // change password for root user, Gravitino API must set password in catalog properties
    try (Connection connection =
            DriverManager.getConnection(mySQLJdbcUrl, USER_NAME, getPassword());
        Statement statement = connection.createStatement()) {

      String query = String.format("CREATE DATABASE IF NOT EXISTS %s;", testDBName);
      // FIXME: String, which is used in SQL, can be unsafe
      statement.execute(query);
      LOG.info(String.format("MySQL container database %s has been created", testDBName));
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  public String getDatabaseNameByClass(Class<?> clz) {
    // use hashcode to avoid two class with same name in different directories.
    return clz.getSimpleName().concat(String.valueOf(clz.hashCode()));
  }

  public String getUsername() {
    return USER_NAME;
  }

  public String getPassword() {
    return PASSWORD;
  }

  public String getJdbcUrl(Class<?> clz) {
    return format(
        "jdbc:mysql://%s:%d/%s", getContainerIpAddress(), MYSQL_PORT, getDatabaseNameByClass(clz));
  }

  public String getJdbcUrl(String dbName) {
    return format("jdbc:mysql://%s:%d/%s", getContainerIpAddress(), MYSQL_PORT, dbName);
  }

  public String getDriverClassName() throws SQLException {
    return DriverManager.getDriver(getJdbcUrl(AbstractIT.class)).getClass().getName();
  }

  public static class Builder
      extends BaseContainer.Builder<MySQLContainer.Builder, MySQLContainer> {

    private Builder() {
      this.image = DEFAULT_IMAGE;
      this.hostName = HOST_NAME;
      this.exposePorts = ImmutableSet.of(MYSQL_PORT);
    }

    @Override
    public MySQLContainer build() {
      return new MySQLContainer(
          image, hostName, exposePorts, extraHosts, filesToMount, envVars, network);
    }
  }
}
