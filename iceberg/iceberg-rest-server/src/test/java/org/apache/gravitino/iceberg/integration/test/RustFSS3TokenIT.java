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
package org.apache.gravitino.iceberg.integration.test;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.credential.PathBasedCredentialContext;
import org.apache.gravitino.credential.S3TokenCredential;
import org.apache.gravitino.integration.test.container.RustFSContainer;
import org.apache.gravitino.s3.credential.S3TokenGenerator;
import org.apache.gravitino.storage.S3Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

/** Exercises Gravitino's generated inline STS policies through signed requests to RustFS. */
@Tag("gravitino-docker-test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RustFSS3TokenIT {

  private static final Logger LOG = LoggerFactory.getLogger(RustFSS3TokenIT.class);

  private final String bucket = "gravitino-policy-" + UUID.randomUUID();
  private final String otherBucket = "gravitino-other-" + UUID.randomUUID();
  private RustFSContainer container;
  private S3Client rootClient;

  @BeforeAll
  void startStorage() {
    container = RustFSContainer.builder().build();
    container.start();
    container.createBucket(bucket);
    container.createBucket(otherBucket);
    rootClient =
        client(AwsBasicCredentials.create(RustFSContainer.ACCESS_KEY, RustFSContainer.SECRET_KEY));
    for (String key : List.of("read/file", "write/file", "write_sibling/file", "other/file")) {
      put(rootClient, bucket, key);
    }
    put(rootClient, otherBucket, "write/file");
  }

  @AfterAll
  void stopStorage() {
    if (rootClient != null) {
      rootClient.close();
    }
    if (container != null) {
      container.close();
    }
  }

  @Test
  void testObjectResourcesAndReadOnlyPermissions() throws IOException {
    try (S3Client scoped = scopedClient(false)) {
      Assertions.assertEquals("data", read(scoped, bucket, "read/file"));
      Assertions.assertEquals("data", read(scoped, bucket, "write/file"));
      put(scoped, bucket, "write/new");
      Assertions.assertEquals("data", read(scoped, bucket, "write/new"));
      scoped.deleteObject(request -> request.bucket(bucket).key("write/new"));
      Assertions.assertTrue(
          rootClient
              .listObjectsV2(request -> request.bucket(bucket).prefix("write/new"))
              .contents()
              .isEmpty());

      assertAccessDenied(() -> put(scoped, bucket, "read/new"));
      assertAccessDenied(
          () -> scoped.deleteObject(request -> request.bucket(bucket).key("read/file")));
      for (String key : List.of("other/file", "write_sibling/file", "write")) {
        assertAccessDenied(() -> read(scoped, bucket, key));
        assertAccessDenied(() -> put(scoped, bucket, key));
        assertAccessDenied(() -> scoped.deleteObject(request -> request.bucket(bucket).key(key)));
      }
      assertAccessDenied(() -> read(scoped, otherBucket, "write/file"));
      assertAccessDenied(() -> put(scoped, otherBucket, "write/new"));
      assertAccessDenied(
          () -> scoped.deleteObject(request -> request.bucket(otherBucket).key("write/file")));
      scoped.getBucketLocation(request -> request.bucket(bucket));
      assertAccessDenied(() -> scoped.getBucketLocation(request -> request.bucket(otherBucket)));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testListPrefixes(boolean includeLocationPrefix) throws IOException {
    try (S3Client scoped = scopedClient(includeLocationPrefix)) {
      Assertions.assertEquals(
          List.of("read/file"),
          scoped.listObjects(request -> request.bucket(bucket).prefix("read/")).contents().stream()
              .map(S3Object::key)
              .collect(Collectors.toList()));
      Assertions.assertEquals(
          List.of("read/file"),
          scoped
              .listObjectsV2(request -> request.bucket(bucket).prefix("read/"))
              .contents()
              .stream()
              .map(S3Object::key)
              .collect(Collectors.toList()));
      scoped.listObjectsV2(request -> request.bucket(bucket).prefix("write/subdirectory/"));
      for (String prefix : List.of("", "other/", "write_sibling/")) {
        assertAccessDenied(
            () -> scoped.listObjects(request -> request.bucket(bucket).prefix(prefix)));
        assertAccessDenied(
            () -> scoped.listObjectsV2(request -> request.bucket(bucket).prefix(prefix)));
      }
      assertAccessDenied(() -> scoped.listObjects(request -> request.bucket(bucket)));
      assertAccessDenied(() -> scoped.listObjectsV2(request -> request.bucket(bucket)));
      assertAccessDenied(
          () -> scoped.listObjectsV2(request -> request.bucket(otherBucket).prefix("write/")));
      if (includeLocationPrefix) {
        // Hadoop's directory probe deliberately permits listing the bare location prefix. That
        // also reveals sibling key names sharing it, but grants no access to their object data.
        Assertions.assertTrue(
            scoped
                .listObjectsV2(request -> request.bucket(bucket).prefix("write"))
                .contents()
                .stream()
                .anyMatch(object -> object.key().equals("write_sibling/file")));
        scoped.listObjects(request -> request.bucket(bucket).prefix("write"));
        assertAccessDenied(() -> read(scoped, bucket, "write_sibling/file"));
      } else {
        assertAccessDenied(
            () -> scoped.listObjects(request -> request.bucket(bucket).prefix("write")));
        assertAccessDenied(
            () -> scoped.listObjectsV2(request -> request.bucket(bucket).prefix("write")));
      }
    }
  }

  @Test
  void testReadOnlySession() throws IOException {
    try (S3Client scoped = scopedClient(false, false)) {
      Assertions.assertEquals("data", read(scoped, bucket, "read/file"));
      Assertions.assertEquals("data", read(scoped, bucket, "write/file"));
      scoped.listObjectsV2(request -> request.bucket(bucket).prefix("write/"));
      assertAccessDenied(() -> put(scoped, bucket, "write/read-only"));
      assertAccessDenied(
          () -> scoped.deleteObject(request -> request.bucket(bucket).key("write/file")));
      assertAccessDenied(
          () ->
              scoped.createMultipartUpload(
                  request -> request.bucket(bucket).key("write/read-only")));
    }
  }

  @Test
  void testMultipartUploadWithGeneratedWritePolicy() throws IOException {
    String key = "write/multipart";
    try (S3Client scoped = scopedClient(false)) {
      assertAccessDenied(
          () ->
              scoped.createMultipartUpload(
                  request -> request.bucket(bucket).key("read/multipart")));
      assertAccessDenied(
          () ->
              scoped.createMultipartUpload(
                  request -> request.bucket(bucket).key("other/multipart")));
      String uploadId =
          scoped.createMultipartUpload(request -> request.bucket(bucket).key(key)).uploadId();
      boolean completed = false;
      try {
        byte[] firstPart = new byte[5 * 1024 * 1024];
        String firstETag =
            scoped
                .uploadPart(
                    request -> request.bucket(bucket).key(key).uploadId(uploadId).partNumber(1),
                    RequestBody.fromBytes(firstPart))
                .eTag();
        String secondETag =
            scoped
                .uploadPart(
                    request -> request.bucket(bucket).key(key).uploadId(uploadId).partNumber(2),
                    RequestBody.fromString("tail"))
                .eTag();
        scoped.completeMultipartUpload(
            request ->
                request
                    .bucket(bucket)
                    .key(key)
                    .uploadId(uploadId)
                    .multipartUpload(
                        upload ->
                            upload.parts(
                                CompletedPart.builder().partNumber(1).eTag(firstETag).build(),
                                CompletedPart.builder().partNumber(2).eTag(secondETag).build())));
        completed = true;
        Assertions.assertEquals(
            firstPart.length + 4,
            scoped
                .getObjectAsBytes(request -> request.bucket(bucket).key(key))
                .asByteArray()
                .length);
        scoped.deleteObject(request -> request.bucket(bucket).key(key));
      } finally {
        // Abort is not granted by Gravitino's generated policy. Root credentials clean up an
        // unfinished upload if any assertion or request above fails.
        if (!completed) {
          try {
            rootClient.abortMultipartUpload(
                request -> request.bucket(bucket).key(key).uploadId(uploadId));
          } catch (Exception cleanupFailure) {
            LOG.warn(
                "Failed to clean up multipart upload {} after a test failure",
                uploadId,
                cleanupFailure);
          }
        }
      }
    }
  }

  private S3Client scopedClient(boolean includeLocationPrefix) throws IOException {
    return scopedClient(includeLocationPrefix, true);
  }

  private S3Client scopedClient(boolean includeLocationPrefix, boolean writable)
      throws IOException {
    Map<String, String> properties = new HashMap<>();
    properties.put(S3Properties.GRAVITINO_S3_REGION, RustFSContainer.REGION);
    properties.put(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID, RustFSContainer.ACCESS_KEY);
    properties.put(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY, RustFSContainer.SECRET_KEY);
    properties.put(S3Properties.GRAVITINO_S3_ROLE_ARN, RustFSContainer.ROLE_ARN);
    properties.put(S3Properties.GRAVITINO_S3_STS_ENDPOINT, container.getS3Endpoint());
    properties.put(
        CredentialConstants.S3_CREDENTIAL_LIST_LOCATION_PREFIX,
        Boolean.toString(includeLocationPrefix));
    try (S3TokenGenerator generator = new S3TokenGenerator()) {
      generator.initialize(properties);
      S3TokenCredential credential =
          generator.generate(
              new PathBasedCredentialContext(
                  "rustfs-test",
                  writable ? Set.of("s3://" + bucket + "/write") : Set.of(),
                  Set.of("s3://" + bucket + "/read", "s3://" + bucket + "/write")));
      Assertions.assertNotEquals(RustFSContainer.ACCESS_KEY, credential.accessKeyId());
      Assertions.assertFalse(credential.sessionToken().isEmpty());
      return client(
          AwsSessionCredentials.create(
              credential.accessKeyId(), credential.secretAccessKey(), credential.sessionToken()));
    }
  }

  private S3Client client(AwsCredentials credentials) {
    return S3Client.builder()
        .endpointOverride(URI.create(container.getS3Endpoint()))
        .region(Region.of(RustFSContainer.REGION))
        .forcePathStyle(true)
        .credentialsProvider(StaticCredentialsProvider.create(credentials))
        .overrideConfiguration(
            config ->
                config
                    .apiCallTimeout(Duration.ofSeconds(30))
                    .apiCallAttemptTimeout(Duration.ofSeconds(10)))
        .build();
  }

  private static void put(S3Client client, String bucket, String key) {
    client.putObject(request -> request.bucket(bucket).key(key), RequestBody.fromString("data"));
  }

  private static String read(S3Client client, String bucket, String key) {
    return client.getObjectAsBytes(request -> request.bucket(bucket).key(key)).asUtf8String();
  }

  private static void assertAccessDenied(Executable request) {
    S3Exception error = Assertions.assertThrows(S3Exception.class, request);
    Assertions.assertEquals(403, error.statusCode());
    Assertions.assertEquals("AccessDenied", error.awsErrorDetails().errorCode());
  }
}
