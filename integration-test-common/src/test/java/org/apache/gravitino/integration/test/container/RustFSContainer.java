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
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * A RustFS container providing S3-compatible object storage. RustFS enforces the session policy of
 * an {@code AssumeRole} request, so it can exercise credential vending, including the read-only
 * downgrade, without a cloud account.
 */
public class RustFSContainer extends BaseContainer {
  private static final Logger LOG = LoggerFactory.getLogger(RustFSContainer.class);

  /** The image pin consumed from the Docker Compose manifest tracked by image updates. */
  public static final String DEFAULT_IMAGE = loadImage();

  /** The container hostname on the shared integration-test network. */
  public static final String HOST_NAME = "gravitino-ci-rustfs";

  /** The S3 and STS port inside the container. */
  public static final int PORT = 9000;

  /** Root access key used only by this local test fixture. */
  public static final String ACCESS_KEY = "rustfsadmin";

  /** Root secret key used only by this local test fixture. */
  public static final String SECRET_KEY = "rustfsadmin123";

  /** Region used when signing requests to the fixture. */
  public static final String REGION = "us-east-1";

  /**
   * Well-formed role identifier for the SDK. RustFS scopes sessions by caller and inline policy;
   * this fixture does not emulate AWS named-role trust or external-ID enforcement.
   */
  public static final String ROLE_ARN = "arn:aws:iam::123456789012:role/gravitino-test";

  /**
   * Creates a builder with the default test configuration.
   *
   * @return the container builder
   */
  public static Builder builder() {
    return new Builder();
  }

  private RustFSContainer(
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
    withLogConsumer(new PrintingContainerLog(format("%-14s| ", "RustFSContainer")));
  }

  @Override
  public void start() {
    super.start();
    checkContainerStatus(60);
  }

  @Override
  protected boolean checkContainerStatus(int retryLimit) {
    // A listening socket alone does not establish that IAM and the S3 API are initialized.
    try (S3Client client = createS3Client()) {
      await()
          .atMost(60, TimeUnit.SECONDS)
          .pollInterval(Math.max(1, 60 / retryLimit), TimeUnit.SECONDS)
          .until(
              () -> {
                try {
                  client.listBuckets();
                  return true;
                } catch (Exception e) {
                  LOG.info("RustFS is not ready yet: {}", e.getMessage());
                  return false;
                }
              });
    }
    return true;
  }

  /**
   * Creates a bucket, so that a warehouse location inside it can be used.
   *
   * @param bucketName the bucket to create
   */
  public void createBucket(String bucketName) {
    try (S3Client client = createS3Client()) {
      client.createBucket(request -> request.bucket(bucketName));
      client.headBucket(request -> request.bucket(bucketName));
    }
  }

  /**
   * Returns the S3 and STS endpoint reachable from the host through the published port.
   *
   * @return the endpoint, for example {@code http://localhost:32768}
   */
  public String getS3Endpoint() {
    return format("http://%s:%d", container.getHost(), getMappedPort(PORT));
  }

  static String imageFromCompose(String compose) {
    // This single-service manifest is deliberately limited to one unquoted image reference.
    // Fail closed if its structure changes, rather than silently using a different image pin.
    Matcher matcher = Pattern.compile("(?m)^\\s+image: (\\S+)\\s*$").matcher(compose);
    if (!matcher.find()) {
      throw new IllegalArgumentException("RustFS Compose manifest must contain an image");
    }
    String image = matcher.group(1);
    if (matcher.find() || !image.matches("rustfs/rustfs:[^@\\s]+@sha256:[0-9a-f]{64}")) {
      throw new IllegalArgumentException(
          "RustFS Compose manifest must contain one version and digest pin");
    }
    return image;
  }

  private static String loadImage() {
    try (InputStream stream =
        RustFSContainer.class.getResourceAsStream("/docker-compose-rustfs.yml")) {
      if (stream == null) {
        throw new IllegalStateException("Missing docker-compose-rustfs.yml test resource");
      }
      return imageFromCompose(new String(stream.readAllBytes(), StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new IllegalStateException("Unable to read RustFS image pin", e);
    }
  }

  private S3Client createS3Client() {
    return S3Client.builder()
        .endpointOverride(URI.create(getS3Endpoint()))
        .region(Region.of(REGION))
        .forcePathStyle(true)
        .credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
        .overrideConfiguration(
            config ->
                config
                    .apiCallTimeout(Duration.ofSeconds(5))
                    .apiCallAttemptTimeout(Duration.ofSeconds(3)))
        .build();
  }

  /** Builder for {@link RustFSContainer}. */
  public static class Builder
      extends BaseContainer.Builder<RustFSContainer.Builder, RustFSContainer> {

    private Builder() {
      this.image = DEFAULT_IMAGE;
      this.hostName = HOST_NAME;
      this.exposePorts = ImmutableSet.of(PORT);
      this.envVars =
          ImmutableMap.of(
              "RUSTFS_ACCESS_KEY", ACCESS_KEY,
              "RUSTFS_SECRET_KEY", SECRET_KEY,
              "RUSTFS_CONSOLE_ENABLE", "false");
    }

    @Override
    public RustFSContainer build() {
      return new RustFSContainer(
          image, hostName, exposePorts, extraHosts, filesToMount, envVars, network);
    }
  }
}
