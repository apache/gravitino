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

import com.google.common.collect.ImmutableSet;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.BucketCannedACL;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * LocalStack for S3, on the official {@link LocalStackContainer} module. It is ready once
 * LocalStack logs {@code Ready.}, the module's own check, and {@link #createBucket(String)} and the
 * other S3 helpers call it through the AWS SDK, so tests need no {@code awslocal} in the container.
 * Callers need {@code libs.aws.s3} on their test runtime classpath.
 */
public class GravitinoLocalStackContainer extends BaseContainer {

  public static final String DEFAULT_IMAGE = TestImages.LOCALSTACK;
  public static final String HOST_NAME = "gravitino-ci-localstack";
  public static final int PORT = 4566;

  /**
   * The access key clients use. LocalStack does not enforce IAM here, so any key works; tests share
   * this one instead of creating an IAM user.
   */
  public static final String ACCESS_KEY = "test";

  /** The secret key that goes with {@link #ACCESS_KEY}. */
  public static final String SECRET_KEY = "test";

  public GravitinoLocalStackContainer(
      String image,
      String hostName,
      Set<Integer> ports,
      Map<String, String> extraHosts,
      Map<String, String> filesToMount,
      Map<String, String> envVars,
      Optional<Network> network) {
    super(module(image), hostName, ports, extraHosts, filesToMount, envVars, network);
  }

  public static Builder builder() {
    return new Builder();
  }

  // The module's "Ready." log line comes once LocalStack serves requests; S3 starts on the first.
  @Override
  @Nullable
  protected WaitStrategy waitStrategy() {
    return null;
  }

  /**
   * Creates a bucket, or keeps it if this account already owns it.
   *
   * @param bucketName the bucket to create
   */
  public void createBucket(String bucketName) {
    S3Calls.createBucket(getS3Endpoint(), bucketName);
  }

  /**
   * Gives a bucket the {@code public-read-write} canned ACL.
   *
   * @param bucketName the bucket
   */
  public void makeBucketPublic(String bucketName) {
    S3Calls.makeBucketPublic(getS3Endpoint(), bucketName);
  }

  /**
   * Lists the keys of the objects in a bucket, at most one page.
   *
   * @param bucketName the bucket
   * @return the object keys
   */
  public List<String> listObjectKeys(String bucketName) {
    return S3Calls.listObjectKeys(getS3Endpoint(), bucketName);
  }

  /**
   * Returns the S3 endpoint reachable from the host through the published port.
   *
   * @return the endpoint, for example {@code http://localhost:32768}
   */
  public String getS3Endpoint() {
    return format("http://%s:%d", container.getHost(), getMappedPort(PORT));
  }

  /**
   * Returns the image as the module accepts it: a mirror or a custom tag stands in for {@code
   * localstack/localstack}.
   */
  static DockerImageName imageName(String image) {
    return DockerImageName.parse(image).asCompatibleSubstituteFor("localstack/localstack");
  }

  // The module's constructor asks Docker for its socket path, which it mounts for LocalStack's
  // Lambda and ECS emulation, so building this container needs Docker.
  private static LocalStackContainer module(String image) {
    // The module's log check ignores BaseContainer's startup timeout, so it gets the same budget.
    return new LocalStackContainer(imageName(image)).withStartupTimeout(Duration.ofMinutes(5));
  }

  // Keeps the SDK types out of the container class, so a module without the S3 SDK can still load
  // the container as long as it does not call these.
  private static final class S3Calls {
    private S3Calls() {}

    static void createBucket(String endpoint, String bucketName) {
      try (S3Client client = client(endpoint)) {
        client.createBucket(request -> request.bucket(bucketName));
      } catch (BucketAlreadyOwnedByYouException e) {
        LOG.info("LocalStack bucket {} already exists", bucketName);
      }
    }

    static void makeBucketPublic(String endpoint, String bucketName) {
      try (S3Client client = client(endpoint)) {
        client.putBucketAcl(
            request -> request.bucket(bucketName).acl(BucketCannedACL.PUBLIC_READ_WRITE));
      }
    }

    static List<String> listObjectKeys(String endpoint, String bucketName) {
      try (S3Client client = client(endpoint)) {
        return client.listObjectsV2(request -> request.bucket(bucketName)).contents().stream()
            .map(S3Object::key)
            .collect(Collectors.toList());
      }
    }

    private static S3Client client(String endpoint) {
      return S3Client.builder()
          .endpointOverride(URI.create(endpoint))
          .region(Region.US_EAST_1)
          .forcePathStyle(true)
          .credentialsProvider(
              StaticCredentialsProvider.create(AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
          .overrideConfiguration(config -> config.apiCallTimeout(Duration.ofSeconds(30)))
          .build();
    }
  }

  public static class Builder
      extends BaseContainer.Builder<
          GravitinoLocalStackContainer.Builder, GravitinoLocalStackContainer> {
    public Builder() {
      super();
      this.image = DEFAULT_IMAGE;
      this.hostName = HOST_NAME;
      this.exposePorts = ImmutableSet.of(PORT);
    }

    @Override
    public GravitinoLocalStackContainer build() {
      return new GravitinoLocalStackContainer(
          image, hostName, exposePorts, extraHosts, filesToMount, envVars, network);
    }
  }
}
