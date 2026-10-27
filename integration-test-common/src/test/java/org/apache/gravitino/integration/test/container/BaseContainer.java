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

import static java.util.Objects.requireNonNull;
import static org.testcontainers.utility.MountableFile.forHostPath;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.ContainerNetwork;
import com.github.dockerjava.api.model.Ulimit;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.startupcheck.IsRunningStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.lifecycle.Startable;

/**
 * The base for all integration test containers: it applies the common settings and manages the
 * lifecycle. Gravitino's own images have no official Testcontainers module, so they start from an
 * image name; services that have an official module supply that module instead. A supplied module
 * keeps its own readiness check only when the subclass also returns {@code null} from {@link
 * #waitStrategy()}; otherwise the default port check replaces it.
 */
public abstract class BaseContainer implements Startable {
  public static final Logger LOG = LoggerFactory.getLogger(BaseContainer.class);
  // Host name of the container
  protected final String hostName;
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

  /**
   * {@code tmpfs} mount options for a disposable data directory; the mount only takes what is
   * written.
   */
  protected static final String DISPOSABLE_DATA_TMPFS = "rw,size=2g";

  protected final GenericContainer<?> container;

  private volatile long lastContainerStartMs;

  private volatile Durability durability = Durability.runDefault();

  private boolean disposableDataApplied;

  /**
   * Builds the container from an image name. Used by Gravitino's own images, which have no official
   * Testcontainers module.
   *
   * @param image the Docker image name
   * @param hostName the host name and network alias of the container
   * @param ports the ports to expose
   * @param extraHosts additional host name to IP address mappings
   * @param filesToMount host files to copy into the container, keyed by container path
   * @param envVars the environment variables of the container
   * @param network the network to join, if any
   */
  protected BaseContainer(
      String image,
      String hostName,
      Set<Integer> ports,
      Map<String, String> extraHosts,
      Map<String, String> filesToMount,
      Map<String, String> envVars,
      Optional<Network> network) {
    this(
        new GenericContainer<>(requireNonNull(image, "image is null")),
        hostName,
        ports,
        extraHosts,
        filesToMount,
        envVars,
        network);
  }

  /**
   * Wraps a container the subclass has already built, such as an official Testcontainers module or
   * a {@link GenericContainer} with its own wait strategy. A subclass that relies on the readiness
   * check of the supplied container should also override {@link #waitStrategy()}.
   *
   * @param container the container to configure and manage
   * @param hostName the host name and network alias of the container
   * @param ports the ports to expose
   * @param extraHosts additional host name to IP address mappings
   * @param filesToMount host files to copy into the container, keyed by container path
   * @param envVars the environment variables of the container
   * @param network the network to join, if any
   */
  protected BaseContainer(
      GenericContainer<?> container,
      String hostName,
      Set<Integer> ports,
      Map<String, String> extraHosts,
      Map<String, String> filesToMount,
      Map<String, String> envVars,
      Optional<Network> network) {
    this.container =
        requireNonNull(container, "container is null")
            .withCreateContainerCmdModifier(
                cmd ->
                    cmd.getHostConfig()
                        .withSysctls(
                            Collections.singletonMap("net.ipv4.ip_local_port_range", "20000 40000"))
                        .withUlimits(new Ulimit[] {new Ulimit("nproc", 120000L, 120000L)}));
    this.ports = requireNonNull(ports, "ports is null");
    this.hostName = requireNonNull(hostName, "hostName is null");
    this.extraHosts = extraHosts;
    this.filesToMount = requireNonNull(filesToMount, "filesToMount is null");
    this.envVars = requireNonNull(envVars, "envVars is null");
    this.network = requireNonNull(network, "network is null");

    setupContainer();
  }

  /**
   * Applies the common settings and then the readiness check. Runs from the constructor, so an
   * override must call {@code super.setupContainer()} and must not read subclass fields.
   */
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
    // A per-run name instead of Docker's generated one, whose underscore breaks Hadoop's reverse
    // lookups and which does not say which run owns the container.
    String name = RunNames.containerName(hostName);
    container.withCreateContainerCmdModifier(
        c -> {
          c.withHostName(hostName);
          c.withName(name);
        });
    network.ifPresent(net -> container.withNetwork(net).withNetworkAliases(hostName));

    WaitStrategy waitStrategy = waitStrategy();
    if (waitStrategy != null) {
      container
          .withStartupCheckStrategy(new IsRunningStartupCheckStrategy())
          .waitingFor(waitStrategy)
          .withStartupTimeout(startupTimeout());
    }
  }

  /**
   * Returns how long {@link #waitStrategy()} may take in all. The default is five minutes; a
   * container whose strategy chains several checks returns at least the sum of their budgets.
   * Called from the constructor, so an override must not read subclass fields.
   *
   * @return the startup timeout
   */
  protected Duration startupTimeout() {
    return Duration.ofMinutes(5);
  }

  /**
   * Sets whether this container keeps its durability work. Takes effect at {@link #start()}.
   *
   * @param durability the durability; {@link Durability#runDefault()} until set
   */
  public void setDurability(Durability durability) {
    this.durability = requireNonNull(durability, "durability is null");
  }

  /**
   * Returns whether this container keeps its durability work.
   *
   * @return the durability
   */
  public Durability durability() {
    return durability;
  }

  /**
   * Makes the container skip durability work, such as putting its data directories on {@code tmpfs}
   * or turning off fsync. {@link #start()} calls it once when the container is {@link
   * Durability#DISPOSABLE}. The default does nothing; a service with its own storage overrides it.
   *
   * @param container the container to configure
   */
  protected void applyDisposableData(GenericContainer<?> container) {}

  /**
   * Returns how to tell that the container is ready. The default waits for the exposed ports to
   * listen. Override it to return a service-specific strategy, or return {@code null} to keep the
   * startup check, wait strategy and timeout the supplied container already has, as an official
   * module does. Called from the constructor, so an override must not read subclass fields.
   *
   * @return the wait strategy, or {@code null} to keep the container's own
   */
  @Nullable
  protected WaitStrategy waitStrategy() {
    return Wait.forListeningPort();
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
    return ipAddress(dockerClient.inspectContainerCmd(container.getContainerId()).exec());
  }

  /**
   * Returns the address of a container on its only network, as other containers and a Linux host
   * reach it. A wait strategy passes its target's {@link ContainerState#getContainerInfo()}.
   *
   * @param containerInfo the inspected container
   * @return the IP address
   */
  static String ipAddress(InspectContainerResponse containerInfo) {
    Map<String, ContainerNetwork> containerNetworkMap =
        containerInfo.getNetworkSettings().getNetworks();
    Preconditions.checkArgument(
        (containerNetworkMap.size() == 1),
        "Container \"NetworkMap\" size is required equals to 1.");
    for (Map.Entry<String, ContainerNetwork> entry : containerNetworkMap.entrySet()) {
      return entry.getValue().getIpAddress();
    }

    throw new RuntimeException("Impossible to reach here");
  }

  @Override
  public void start() {
    if (durability == Durability.DISPOSABLE && !disposableDataApplied) {
      applyDisposableData(container);
      disposableDataApplied = true;
    }
    timedStart(container);
  }

  /**
   * Starts a Testcontainers container, or Compose project, and records the time as {@link
   * #lastContainerStartMs()}. For a subclass that starts something other than {@link #container}.
   *
   * @param startable what to start
   */
  protected final void timedStart(Startable startable) {
    long startNanos = System.nanoTime();
    try {
      startable.start();
    } finally {
      lastContainerStartMs = (System.nanoTime() - startNanos) / 1_000_000;
    }
  }

  /**
   * Returns how long the last {@code start()} spent in Testcontainers' own start: creating and
   * starting the container and running its wait strategy, which holds the readiness checks. Work
   * after that, such as {@link #checkContainerStatus(int)} or creating clients, is not included.
   *
   * @return the time in milliseconds, or 0 before the first start
   */
  public long lastContainerStartMs() {
    return lastContainerStartMs;
  }

  @Override
  public void stop() {
    container.stop();
  }

  /**
   * Checks the started container beyond its wait strategy. Readiness belongs in {@link
   * #waitStrategy()}; the default checks nothing.
   *
   * @param retryLimit how many times to try
   * @return whether the container is ready
   */
  protected boolean checkContainerStatus(int retryLimit) {
    return true;
  }

  // Execute the command in the container.
  public Container.ExecResult executeInContainer(String... commandAndArgs) {
    try {
      return container.execInContainer(commandAndArgs);
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(
          "Exception while running command: " + String.join(" ", commandAndArgs), e);
    }
  }

  protected abstract static class Builder<
      SELF extends Builder<SELF, CONTAINER>, CONTAINER extends BaseContainer> {
    protected String image;
    protected String hostName;
    protected boolean kerberosEnabled;
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

    public SELF withKerberosEnabled(boolean kerberosEnabled) {
      this.kerberosEnabled = kerberosEnabled;
      return self;
    }

    public abstract CONTAINER build();
  }
}
