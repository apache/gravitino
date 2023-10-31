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

  public static final String DEFAULT_IMAGE = "datastrato/gravitino-ci-trino:latest";
  public static final String HOST_NAME = "gravitino-ci-trino";
  static final int TRINO_PORT = 8080;

  static final String TRINO_CONF_GRAVITINO_URI = "gravitino.uri";
  static final String TRINO_CONF_GRAVITINO_METALAKE = "gravitino.metalake";
  static final String TRINO_CONF_HIVE_METASTORE_URI = "hive.metastore.uri";
  static final String TRINO_CONTAINER_CONF_DIR = "/etc/trino";
  static final String TRINO_CONTAINER_PLUGIN_GRAVITINO_DIR = "/usr/lib/trino/plugin/gravitino";

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
      Optional<Network> network) {
    super(image, hostName, ports, extraHosts, filesToMount, envVars, network);
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
          image, hostName, exposePorts, extraHosts, filesToMount, envVars, network);
    }
  }
}
