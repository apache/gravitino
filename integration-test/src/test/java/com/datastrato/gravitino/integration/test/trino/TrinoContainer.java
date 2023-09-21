/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.trino;

import static java.lang.String.format;

import com.google.common.collect.ImmutableSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;

public class TrinoContainer extends BaseContainer {
  public static final Logger LOG = LoggerFactory.getLogger(TrinoContainer.class);

  public static final String DEFAULT_IMAGE = "datastrato/gravitino-ci-trino:0.1.0";
  public static final String HOST_NAME = "gravitino-ci-trino";

  public static Builder builder() {
    return new Builder();
  }

  protected TrinoContainer(
      String image,
      String hostName,
      Set<Integer> ports,
      Map<String, String> extraHosts,
      Map<String, String> filesToMount,
      Map<String, String> envVars,
      Optional<Network> network,
      int startupRetryLimit) {
    super(image, hostName, ports, extraHosts, filesToMount, envVars, network, startupRetryLimit);
  }

  @Override
  protected void setupContainer() {
    super.setupContainer();
    withLogConsumer(new PrintingContainerLog(format("%-15s| ", "TrinoContainer")));
  }

  public static class Builder
      extends BaseContainer.Builder<TrinoContainer.Builder, TrinoContainer> {
    private Builder() {
      this.image = DEFAULT_IMAGE;
      this.hostName = HOST_NAME;
      this.exposePorts = ImmutableSet.of();
    }

    @Override
    public TrinoContainer build() {
      return new TrinoContainer(
          image,
          hostName,
          exposePorts,
          extraHosts,
          filesToMount,
          envVars,
          network,
          startupRetryLimit);
    }
  }
}
