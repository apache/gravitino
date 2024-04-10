/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.container;

import static java.lang.String.format;

import com.google.common.collect.ImmutableSet;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.rnorth.ducttape.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;

public class DorisContainer extends BaseContainer {
  public static final Logger LOG = LoggerFactory.getLogger(DorisContainer.class);

  public static final String DEFAULT_IMAGE = System.getenv("GRAVITINO_CI_DORIS_DOCKER_IMAGE");
  public static final String HOST_NAME = "gravitino-ci-doris";
  public static final String USER_NAME = "root";
  public static final String PASSWORD = "root";
  public static final int FE_HTTP_PORT = 8030;
  public static final int FE_MYSQL_PORT = 9030;

  private static final String DORIS_FE_PATH = "/opt/apache-doris/fe/log";
  private static final String DORIS_BE_PATH = "/opt/apache-doris/be/log";

  public static Builder builder() {
    return new Builder();
  }

  protected DorisContainer(
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
    withLogConsumer(new PrintingContainerLog(format("%-14s| ", "DorisContainer")));
    withStartupTimeout(Duration.ofMinutes(5));
  }

  @Override
  public void start() {
    try {
      super.start();
      Preconditions.check("Doris container startup failed!", checkContainerStatus(5));
      Preconditions.check("Doris container password change failed!", changePassword());
    } finally {
      copyDorisLog();
    }
  }

  private void copyDorisLog() {
    try {
      // stop Doris container
      String destPath = System.getenv("IT_PROJECT_DIR");
      LOG.info("Copy doris log file to {}", destPath);

      String feTarPath = "/doris-be.tar";
      String beTarPath = "/doris-fe.tar";

      // Pack the jar files
      container.execInContainer("tar", "cf", feTarPath, DORIS_BE_PATH);
      container.execInContainer("tar", "cf", beTarPath, DORIS_FE_PATH);

      container.copyFileFromContainer(feTarPath, destPath + File.separator + "doris-be.tar");
      container.copyFileFromContainer(beTarPath, destPath + File.separator + "doris-fe.tar");
    } catch (Exception e) {
      LOG.error("Failed to copy container log to local", e);
    }
  }

  @Override
  protected boolean checkContainerStatus(int retryLimit) {
    int nRetry = 0;

    String dorisJdbcUrl = format("jdbc:mysql://%s:%d/", getContainerIpAddress(), FE_MYSQL_PORT);
    LOG.info("Doris url is " + dorisJdbcUrl);

    while (nRetry++ < retryLimit) {
      try (Connection connection = DriverManager.getConnection(dorisJdbcUrl, USER_NAME, "");
          Statement statement = connection.createStatement()) {

        // execute `SHOW PROC '/backends';` to check if backends is ready
        String query = "SHOW PROC '/backends';";
        try (ResultSet resultSet = statement.executeQuery(query)) {
          while (resultSet.next()) {
            String alive = resultSet.getString("Alive");
            if (alive.equalsIgnoreCase("true")) {
              LOG.info("Doris container startup success!");
              return true;
            }
          }
        }

        LOG.info("Doris container is not ready yet!");
        Thread.sleep(5000);
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
      }
    }

    return false;
  }

  private boolean changePassword() {
    String dorisJdbcUrl = format("jdbc:mysql://%s:%d/", getContainerIpAddress(), FE_MYSQL_PORT);

    // change password for root user, Gravitino API must set password in catalog properties
    try (Connection connection = DriverManager.getConnection(dorisJdbcUrl, USER_NAME, "");
        Statement statement = connection.createStatement()) {

      String query = String.format("SET PASSWORD FOR '%s' = PASSWORD('%s');", USER_NAME, PASSWORD);
      statement.execute(query);
      LOG.info("Doris container password has been changed");
      return true;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }

    return false;
  }

  public static class Builder
      extends BaseContainer.Builder<DorisContainer.Builder, DorisContainer> {
    private Builder() {
      this.image = DEFAULT_IMAGE;
      this.hostName = HOST_NAME;
      this.exposePorts = ImmutableSet.of(FE_HTTP_PORT, FE_MYSQL_PORT);
    }

    @Override
    public DorisContainer build() {
      return new DorisContainer(
          image, hostName, exposePorts, extraHosts, filesToMount, envVars, network);
    }
  }
}
