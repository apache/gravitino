/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.trino;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.ContainerNetwork;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.Wait;

public abstract class BaseContainer implements AutoCloseable {
  public static final Logger LOG = LoggerFactory.getLogger(BaseContainer.class);
  private final String hostName;
  private final Set<Integer> ports;
  private final Map<String, String> filesToMount;
  private final Map<String, String> envVars;
  private final Map<String, String> extraHosts;
  private final Optional<Network> network;
  private final int startupRetryLimit;

  private GenericContainer<?> container;

  protected BaseContainer(
      String image,
      String hostName,
      Set<Integer> ports,
      Map<String, String> extraHosts,
      Map<String, String> filesToMount,
      Map<String, String> envVars,
      Optional<Network> network,
      int startupRetryLimit) {
    checkArgument(startupRetryLimit > 0, "startupRetryLimit needs to be greater or equal to 0");
    this.container = new GenericContainer<>(requireNonNull(image, "image is null"));
    this.ports = requireNonNull(ports, "ports is null");
    this.hostName = requireNonNull(hostName, "hostName is null");
    this.extraHosts = extraHosts;
    this.filesToMount = requireNonNull(filesToMount, "filesToMount is null");
    this.envVars = requireNonNull(envVars, "envVars is null");
    this.network = requireNonNull(network, "network is null");
    this.startupRetryLimit = startupRetryLimit;
    setupContainer();
  }

  protected void setupContainer() {
    for (int port : this.ports) {
      container.addExposedPort(port);
    }
    filesToMount.forEach(
        (dockerPath, filePath) ->
            container.withCopyFileToContainer(forHostPath(filePath), dockerPath));
    container.withEnv(envVars);
    extraHosts.forEach((hostName, ipAddress) -> container.withExtraHost(hostName, ipAddress));
    container
        .withCreateContainerCmdModifier(c -> c.withHostName(hostName))
        .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
        .waitingFor(Wait.forListeningPort())
        .withStartupTimeout(Duration.ofMinutes(5));
    network.ifPresent(net -> container.withNetwork(net).withNetworkAliases(hostName));
  }

  protected void withLogConsumer(Consumer<OutputFrame> logConsumer) {
    container.withLogConsumer(logConsumer);
  }

  protected Integer getMappedPort(int exposedPort) {
    return container.getMappedPort(exposedPort);
  }

  protected String getContainerIpAddress() {
    DockerClient dockerClient = DockerClientFactory.instance().client();
    InspectContainerResponse containerResponse =
        dockerClient.inspectContainerCmd(container.getContainerId()).exec();

    String ipAddress = containerResponse.getNetworkSettings().getIpAddress();
    Map<String, ContainerNetwork> containerNetworkMap =
        containerResponse.getNetworkSettings().getNetworks();
    Assertions.assertEquals(1, containerNetworkMap.size());
    for (Map.Entry<String, ContainerNetwork> entry : containerNetworkMap.entrySet()) {
      ipAddress = entry.getValue().getIpAddress();
      break;
    }

    return ipAddress;
  }

  public void start() {
    container.start();
  }

  public Container.ExecResult executeInContainer(String... commandAndArgs) {
    try {
      return container.execInContainer(commandAndArgs);
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(
          "Exception while running command: " + String.join(" ", commandAndArgs), e);
    }
  }

  @Override
  public void close() {
    container.stop();
  }

  protected abstract static class Builder<
      SELF extends Builder<SELF, BUILD>, BUILD extends BaseContainer> {
    protected String image;
    protected String hostName;
    protected Set<Integer> exposePorts = ImmutableSet.of();
    protected Map<String, String> extraHosts = ImmutableMap.of();
    protected Map<String, String> filesToMount = ImmutableMap.of();
    protected Map<String, String> envVars = ImmutableMap.of();
    protected Optional<Network> network = Optional.empty();
    protected int startupRetryLimit = 5;

    protected SELF self;

    @SuppressWarnings("unchecked")
    public Builder() {
      this.self = (SELF) this;
    }

    public SELF withImage(String image) {
      this.image = image;
      return self;
    }

    public SELF withHostName(String hostName) {
      this.hostName = hostName;
      return self;
    }

    public SELF withExposePorts(Set<Integer> exposePorts) {
      this.exposePorts = exposePorts;
      return self;
    }

    public SELF withExtraHosts(Map<String, String> extraHosts) {
      this.extraHosts = extraHosts;
      return self;
    }

    public SELF withFilesToMount(Map<String, String> filesToMount) {
      this.filesToMount = filesToMount;
      return self;
    }

    public SELF withEnvVars(Map<String, String> envVars) {
      this.envVars = envVars;
      return self;
    }

    public SELF withNetwork(Network network) {
      this.network = Optional.of(network);
      return self;
    }

    public SELF withStartupRetryLimit(int startupRetryLimit) {
      this.startupRetryLimit = startupRetryLimit;
      return self;
    }

    public abstract BUILD build();
  }
}
