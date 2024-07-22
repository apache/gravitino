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

import static java.lang.String.format;
import static org.apache.gravitino.integration.test.util.AbstractIT.isHttpServerUp;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import com.google.common.collect.ImmutableSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.rnorth.ducttape.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;

public class S3MockContainer extends BaseContainer {
  private static final Logger LOGGER = LoggerFactory.getLogger(S3MockContainer.class);
  public static final String DEFAULT_IMAGE = System.getenv("GRAVITINO_CI_S3MOCK_DOCKER_IMAGE");
  public static final String HOST_NAME = "gravitino-ci-s3mock";
  public static final int HTTP_PORT = 9090;
  public static final int HTTPS_PORT = 9191;

  protected S3MockContainer(
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
    withLogConsumer(new PrintingContainerLog(format("%-14s| ", "S3MockContainer")));
  }

  @Override
  public void start() {
    super.start();
    Preconditions.check("S3Mock container startup failed!", checkContainerStatus(5));
  }

  @Override
  protected boolean checkContainerStatus(int retryLimit) {
    String testUrl =
        String.format("http://%s:%s/%s", getContainerIpAddress(), HTTP_PORT, "favicon.ico");

    await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(30 / retryLimit, TimeUnit.SECONDS)
        .until(() -> isHttpServerUp(testUrl));

    LOGGER.info("S3Mock container startup success");
    return true;
  }

  @Override
  public void close() {
    super.close();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder
      extends BaseContainer.Builder<S3MockContainer.Builder, S3MockContainer> {
    private Builder() {
      this.image = DEFAULT_IMAGE;
      this.hostName = HOST_NAME;
      this.exposePorts = ImmutableSet.of(HTTP_PORT, HTTPS_PORT);
    }

    @Override
    public S3MockContainer build() {
      return new S3MockContainer(
          image, hostName, exposePorts, extraHosts, filesToMount, envVars, network);
    }
  }
}
