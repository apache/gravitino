/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gravitino.integration.test.container;

import com.google.common.collect.ImmutableSet;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;

public class ZooKeeperContainer extends BaseContainer {
  private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperContainer.class);

  public static final String DEFAULT_IMAGE = "zookeeper:3.8.0";
  public static final String HOST_NAME = "gravitino-ci-zookeeper";
  public static final int ZK_PORT = 2181;

  public static Builder builder() {
    return new Builder();
  }

  protected ZooKeeperContainer(
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
    withStartupTimeout(Duration.ofMinutes(2));
    withLogConsumer(new PrintingContainerLog(String.format("%-14s| ", "zookeeper")));
  }

  @Override
  protected boolean checkContainerStatus(int retryLimit) {
    // Testcontainers wait strategy already ensures the port is ready.
    LOG.info("ZooKeeper container started");
    return true;
  }

  public String getZkConnectString() {
    return String.format("%s:%d", getContainerIpAddress(), ZK_PORT);
  }

  public static class Builder extends BaseContainer.Builder<Builder, ZooKeeperContainer> {
    private Builder() {
      this.image = DEFAULT_IMAGE;
      this.hostName = HOST_NAME;
      this.exposePorts = ImmutableSet.of(ZK_PORT);
    }

    @Override
    public ZooKeeperContainer build() {
      return new ZooKeeperContainer(
          image, hostName, exposePorts, extraHosts, filesToMount, envVars, network);
    }
  }
}
