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
import static org.awaitility.Awaitility.await;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.rnorth.ducttape.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.Network;

/**
 * A MinIO container providing S3-compatible object storage. MinIO enforces the session policy of an
 * {@code AssumeRole} request, so it can exercise credential vending, including the read-only
 * downgrade, without a cloud account.
 */
public class MinIOContainer extends BaseContainer {
  public static final Logger LOG = LoggerFactory.getLogger(MinIOContainer.class);

  public static final String DEFAULT_IMAGE = "minio/minio:RELEASE.2025-09-07T16-13-09Z";
  public static final String HOST_NAME = "gravitino-ci-minio";
  public static final int PORT = 9000;
  public static final String ACCESS_KEY = "minioadmin";
  public static final String SECRET_KEY = "minioadmin123";

  public static Builder builder() {
    return new Builder();
  }

  private MinIOContainer(
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
    // The image defines no default command; the server needs to be told where to keep its data.
    container.withCommand("server", "/data");
    withLogConsumer(new PrintingContainerLog(format("%-14s| ", "MinIOContainer")));
  }

  @Override
  public void start() {
    super.start();
    Preconditions.check("MinIO container startup failed!", checkContainerStatus(5));
  }

  @Override
  protected boolean checkContainerStatus(int retryLimit) {
    // `mc alias set` contacts the server, so it only succeeds once MinIO is accepting requests.
    await()
        .atMost(30, TimeUnit.SECONDS)
        .pollInterval(30 / retryLimit, TimeUnit.SECONDS)
        .until(
            () -> {
              try {
                return setLocalAlias().getExitCode() == 0;
              } catch (Exception e) {
                LOG.warn("MinIO is not ready yet", e);
                return false;
              }
            });
    return true;
  }

  /**
   * Creates a bucket, so that a warehouse location inside it can be used.
   *
   * @param bucketName the bucket to create
   */
  public void createBucket(String bucketName) {
    try {
      setLocalAlias();
      Container.ExecResult result = executeInContainer("mc", "mb", "local/" + bucketName);
      if (result.getExitCode() != 0) {
        throw new RuntimeException(
            format("Failed to create bucket %s: %s", bucketName, result.getStderr()));
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to create bucket " + bucketName, e);
    }
  }

  private Container.ExecResult setLocalAlias() throws Exception {
    return executeInContainer(
        "mc", "alias", "set", "local", "http://localhost:" + PORT, ACCESS_KEY, SECRET_KEY);
  }

  /**
   * Returns the S3 endpoint of this container, reachable from the host once container addresses are
   * routed.
   *
   * @return the endpoint, for example {@code http://10.20.30.5:9000}
   */
  public String getS3Endpoint() {
    return format("http://%s:%d", getContainerIpAddress(), PORT);
  }

  /** Builder for {@link MinIOContainer}. */
  public static class Builder
      extends BaseContainer.Builder<MinIOContainer.Builder, MinIOContainer> {

    private Builder() {
      this.image = DEFAULT_IMAGE;
      this.hostName = HOST_NAME;
      this.exposePorts = ImmutableSet.of(PORT);
      this.envVars =
          ImmutableMap.of("MINIO_ROOT_USER", ACCESS_KEY, "MINIO_ROOT_PASSWORD", SECRET_KEY);
    }

    @Override
    public MinIOContainer build() {
      return new MinIOContainer(
          image, hostName, exposePorts, extraHosts, filesToMount, envVars, network);
    }
  }
}
