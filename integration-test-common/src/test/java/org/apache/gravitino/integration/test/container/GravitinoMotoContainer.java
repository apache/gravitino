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

import com.google.common.collect.ImmutableSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.testcontainers.containers.Network;

/** Container wrapping {@code motoserver/moto} — a free AWS mock that supports Glue and more. */
public class GravitinoMotoContainer extends BaseContainer {

  public static final String DEFAULT_IMAGE = System.getenv("GRAVITINO_CI_MOTO_DOCKER_IMAGE");
  public static final String HOST_NAME = "gravitino-ci-moto";
  public static final int PORT = 5000;

  public GravitinoMotoContainer(
      String image,
      String hostName,
      Set<Integer> ports,
      Map<String, String> extraHosts,
      Map<String, String> filesToMount,
      Map<String, String> envVars,
      Optional<Network> network) {
    super(image, hostName, ports, extraHosts, filesToMount, envVars, network);
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  protected boolean checkContainerStatus(int retryLimit) {
    return true;
  }

  public static class Builder
      extends BaseContainer.Builder<GravitinoMotoContainer.Builder, GravitinoMotoContainer> {
    public Builder() {
      super();
      this.image = DEFAULT_IMAGE;
      this.hostName = HOST_NAME;
      this.exposePorts = ImmutableSet.of(PORT);
    }

    @Override
    public GravitinoMotoContainer build() {
      return new GravitinoMotoContainer(
          image, hostName, exposePorts, extraHosts, filesToMount, envVars, network);
    }
  }
}
