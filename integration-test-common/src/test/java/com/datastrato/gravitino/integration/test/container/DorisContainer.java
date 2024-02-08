/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.container;

import static java.lang.String.format;

import com.google.common.collect.ImmutableSet;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
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
  }

  @Override
  public void start() {
    super.start();
    Preconditions.check("Doris container startup failed!", checkContainerStatus(5));
    changePassword();
  }

  @Override
  protected boolean checkContainerStatus(int retryLimit) {
    int nRetry = 0;
    boolean isDorisContainerReady = false;

    String dorisMySQLUrl = format("jdbc:mysql://%s:%d/", getContainerIpAddress(), FE_MYSQL_PORT);
    LOG.info("Doris url is " + dorisMySQLUrl);

    while (nRetry++ < retryLimit) {
      try {
        Connection connection = DriverManager.getConnection(dorisMySQLUrl, USER_NAME, "");

        // execute `SHOW PROC '/backends';` to check if backends is ready
        String query = "SHOW PROC '/backends';";
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query);
        while (resultSet.next()) {
          String alive = resultSet.getString("Alive");
          if (alive.equalsIgnoreCase("true")) {
            LOG.info("Doris container startup success!");
            isDorisContainerReady = true;
            break;
          }
        }

        if (!isDorisContainerReady) {
          LOG.info("Doris container is not ready yet!");
          Thread.sleep(5000);
        } else {
          break;
        }
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
      }
    }

    return isDorisContainerReady;
  }

  private void changePassword() {
    // change password for root user, Gravitino API must set password in catalog properties
    try {
      String dorisMySQLUrl = format("jdbc:mysql://%s:%d/", getContainerIpAddress(), FE_MYSQL_PORT);
      Connection connection = DriverManager.getConnection(dorisMySQLUrl, USER_NAME, "");

      String query = String.format("SET PASSWORD FOR '%s' = PASSWORD('%s');", USER_NAME, PASSWORD);
      Statement statement = connection.createStatement();
      statement.execute(query);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
    LOG.info("Doris container password has been changed");
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
