/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.integration.test.container;

import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.model.HostConfig;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategyTarget;
import org.testcontainers.utility.DockerImageName;

/** Tests {@link BaseContainer} configuration without Docker; nothing here starts a container. */
public class TestBaseContainer {

  private static final String HOST_NAME = "test-host";

  @Test
  void testSuppliedContainerIsConfiguredAndKept() {
    ProbeContainer supplied = new ProbeContainer();
    Network network = Network.newNetwork();

    BaseContainer base = new StubContainer(supplied, network);

    Assertions.assertSame(supplied, base.getContainer());
    Assertions.assertEquals(ImmutableList.of(8080), supplied.getExposedPorts());
    Assertions.assertEquals("v", supplied.getEnvMap().get("K"));
    Assertions.assertEquals(ImmutableList.of("other-host:10.0.0.1"), supplied.getExtraHosts());
    Assertions.assertSame(network, supplied.getNetwork());
    Assertions.assertTrue(supplied.getNetworkAliases().contains(HOST_NAME));

    CreateContainerCmd cmd = Mockito.mock(CreateContainerCmd.class);
    HostConfig hostConfig = new HostConfig();
    Mockito.when(cmd.getHostConfig()).thenReturn(hostConfig);
    supplied.getCreateContainerCmdModifiers().forEach(modifier -> modifier.modify(cmd));
    Mockito.verify(cmd).withHostName(HOST_NAME);
    Assertions.assertEquals(
        "20000 40000", hostConfig.getSysctls().get("net.ipv4.ip_local_port_range"));
  }

  @Test
  void testDefaultWaitStrategyReplacesTheSuppliedOne() {
    // GenericContainer already defaults to a port check, so start from a different strategy to
    // see that BaseContainer really applies its own.
    WaitStrategy own = Wait.forLogMessage(".*ready.*", 1);
    ProbeContainer supplied = new ProbeContainer();
    supplied.waitingFor(own);

    new StubContainer(supplied, null);

    Assertions.assertNotSame(own, supplied.currentWaitStrategy());
    Assertions.assertInstanceOf(HostPortWaitStrategy.class, supplied.currentWaitStrategy());
  }

  @Test
  void testNullWaitStrategyKeepsTheSuppliedOne() {
    RecordingWaitStrategy own = new RecordingWaitStrategy();
    ProbeContainer supplied = new ProbeContainer();
    supplied.waitingFor(own);

    new StubContainer(supplied, null) {
      @Override
      protected WaitStrategy waitStrategy() {
        return null;
      }
    };

    Assertions.assertSame(own, supplied.currentWaitStrategy());
    // GenericContainer forwards withStartupTimeout to the current wait strategy.
    Assertions.assertNull(own.startupTimeout, "the module's own startup timeout was overridden");
  }

  /** Records the startup timeout it is given and never waits. */
  private static class RecordingWaitStrategy implements WaitStrategy {
    private Duration startupTimeout;

    @Override
    public void waitUntilReady(WaitStrategyTarget waitStrategyTarget) {}

    @Override
    public WaitStrategy withStartupTimeout(Duration startupTimeout) {
      this.startupTimeout = startupTimeout;
      return this;
    }
  }

  /** Exposes the wait strategy, which GenericContainer keeps protected. */
  private static class ProbeContainer extends GenericContainer<ProbeContainer> {
    ProbeContainer() {
      super(DockerImageName.parse("alpine:3.20"));
    }

    WaitStrategy currentWaitStrategy() {
      return getWaitStrategy();
    }
  }

  private static class StubContainer extends BaseContainer {
    StubContainer(GenericContainer<?> container, Network network) {
      super(
          container,
          HOST_NAME,
          ImmutableSet.of(8080),
          ImmutableMap.of("other-host", "10.0.0.1"),
          ImmutableMap.of(),
          ImmutableMap.of("K", "v"),
          Optional.ofNullable(network));
    }

    @Override
    protected boolean checkContainerStatus(int retryLimit) {
      return true;
    }
  }
}
