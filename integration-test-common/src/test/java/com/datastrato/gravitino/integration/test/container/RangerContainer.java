/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.container;

import static java.lang.String.format;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.ranger.RangerClient;
import org.apache.ranger.RangerServiceException;
import org.rnorth.ducttape.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;

public class RangerContainer extends BaseContainer {
  public static final Logger LOG = LoggerFactory.getLogger(RangerContainer.class);

  public static final String DEFAULT_IMAGE = System.getenv("GRAVITINO_CI_RANGER_DOCKER_IMAGE");
  public static final String HOST_NAME = "gravitino-ci-ranger";
  public static final int RANGER_PORT = 6080;
  public RangerClient rangerClient;
  private String rangerUrl;
  private static final String username = "admin";
  // Apache Ranger Password should be minimum 8 characters with min one alphabet and one numeric.
  private static final String password = "rangerR0cks!";
  /* for kerberos authentication:
  authType = "kerberos"
  username = principal
  password = path of the keytab file */
  private static final String authType = "simple";

  public static Builder builder() {
    return new Builder();
  }

  protected RangerContainer(
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
    withLogConsumer(new PrintingContainerLog(format("%-15s| ", "RangerContainer")));
  }

  @Override
  public void start() {
    super.start();

    rangerUrl = String.format("http://localhost:%s", this.getMappedPort(6080));
    rangerClient = new RangerClient(rangerUrl, authType, username, password, null);

    Preconditions.check("Ranger container startup failed!", checkContainerStatus(10));
  }

  @Override
  protected boolean checkContainerStatus(int retryLimit) {
    int nRetry = 0;
    boolean isRangerContainerReady = false;
    int sleepTimeMillis = 3_000;
    while (nRetry++ < retryLimit) {
      try {
        rangerClient.getPluginsInfo();
        isRangerContainerReady = true;
        LOG.info("Ranger container startup success!");
        break;
      } catch (RangerServiceException e) {
        LOG.warn("Check Ranger startup status... {}", e.getMessage());
      }
      if (!isRangerContainerReady) {
        try {
          Thread.sleep(sleepTimeMillis);
          LOG.warn("Waiting for Ranger server to be ready... ({}ms)", nRetry * sleepTimeMillis);
        } catch (InterruptedException e) {
          // ignore
        }
      }
    }

    return isRangerContainerReady;
  }

  @Override
  public void close() {
    super.close();
  }

  public static class Builder
      extends BaseContainer.Builder<RangerContainer.Builder, RangerContainer> {

    private Builder() {
      this.image = DEFAULT_IMAGE;
      this.hostName = HOST_NAME;
      this.exposePorts = ImmutableSet.of(RANGER_PORT);
      this.envVars =
          ImmutableMap.<String, String>builder().put("RANGER_PASSWORD", password).build();
    }

    @Override
    public RangerContainer build() {
      return new RangerContainer(
          image, hostName, exposePorts, extraHosts, filesToMount, envVars, network);
    }
  }
}
