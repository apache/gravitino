/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.container;

import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.ContainerNetwork;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * The BaseContainer is the base for all containers. It's contains the common methods and settings
 * for all containers. You can extend this class to create your own container to integration test.
 */
public abstract class BaseContainer implements AutoCloseable {
  public static final Logger LOG = LoggerFactory.getLogger(BaseContainer.class);
  // Host name of the container
  private final String hostName;
  // Exposed ports of the container
  private final Set<Integer> ports;
  // Files to mount in the container
  private final Map<String, String> filesToMount;
  // environment variables of the container
  private final Map<String, String> envVars;
  // Additional host and IP address mapping
  private final Map<String, String> extraHosts;
  // Network of the container
  private final Optional<Network> network;

  protected final GenericContainer<?> container;

  protected BaseContainer(
      String image,
      String hostName,
      Set<Integer> ports,
      Map<String, String> extraHosts,
      Map<String, String> filesToMount,
      Map<String, String> envVars,
      Optional<Network> network) {
    this.container = new GenericContainer<>(requireNonNull(image, "image is null"));
    this.ports = requireNonNull(ports, "ports is null");
    this.hostName = requireNonNull(hostName, "hostName is null");
    this.extraHosts = extraHosts;
    this.filesToMount = requireNonNull(filesToMount, "filesToMount is null");
    this.envVars = requireNonNull(envVars, "envVars is null");
    this.network = requireNonNull(network, "network is null");

    setupContainer();
  }

  protected void setupContainer() {
    // Add exposed ports in the container
    for (int port : this.ports) {
      container.addExposedPort(port);
    }
    // Add files to mount in the container
    filesToMount.forEach(
        (dockerPath, filePath) ->
            container.withCopyFileToContainer(forHostPath(filePath), dockerPath));
    // Set environment variables
    container.withEnv(envVars);
    // Set up an additional host and IP address mapping through which the container
    // can look up the corresponding IP address by host name.
    // This method fixes an error that occurs when HDFS looks up hostnames from DNS.
    extraHosts.forEach((hostName, ipAddress) -> container.withExtraHost(hostName, ipAddress));
    container
        .withCreateContainerCmdModifier(c -> c.withHostName(hostName))
        .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
        .waitingFor(Wait.forListeningPort())
        .withStartupTimeout(Duration.ofMinutes(5));
    network.ifPresent(net -> container.withNetwork(net).withNetworkAliases(hostName));
  }

  // This method is used to set the log output of the container.
  protected void withLogConsumer(Consumer<OutputFrame> logConsumer) {
    container.withLogConsumer(logConsumer);
  }

  protected void withStartupTimeout(Duration duration) {
    container.withStartupTimeout(duration);
  }

  // This method is used to get the expose port number of the container.
  public Integer getMappedPort(int exposedPort) {
    return container.getMappedPort(exposedPort);
  }

  public GenericContainer<?> getContainer() {
    return container;
  }

  // This method is used to get the IP address of the container.
  public String getContainerIpAddress() {
    DockerClient dockerClient = DockerClientFactory.instance().client();
    InspectContainerResponse containerResponse =
        dockerClient.inspectContainerCmd(container.getContainerId()).exec();

    Map<String, ContainerNetwork> containerNetworkMap =
        containerResponse.getNetworkSettings().getNetworks();
    Preconditions.checkArgument(
        (containerNetworkMap.size() == 1),
        "Container \"NetworkMap\" size is required equals to 1.");
    for (Map.Entry<String, ContainerNetwork> entry : containerNetworkMap.entrySet()) {
      return entry.getValue().getIpAddress();
    }

    throw new RuntimeException("Impossible to reach here");
  }

  public void start() {
    container.start();
  }

  protected abstract boolean checkContainerStatus(int retryLimit);

  // Execute the command in the container.
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
      SELF extends Builder<SELF, CONTAINER>, CONTAINER extends BaseContainer> {
    protected String image;
    protected String hostName;
    protected Set<Integer> exposePorts = ImmutableSet.of();
    protected Map<String, String> extraHosts = ImmutableMap.of();
    protected Map<String, String> filesToMount = ImmutableMap.of();
    protected Map<String, String> envVars = ImmutableMap.of();
    protected Optional<Network> network = Optional.empty();

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
      this.network = Optional.ofNullable(network);
      return self;
    }

    public abstract CONTAINER build();
  }
}
