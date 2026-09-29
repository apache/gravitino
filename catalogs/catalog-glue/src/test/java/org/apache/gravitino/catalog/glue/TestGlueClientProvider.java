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
package org.apache.gravitino.catalog.glue;

import static org.apache.gravitino.catalog.glue.GlueConstants.AWS_ACCESS_KEY_ID;
import static org.apache.gravitino.catalog.glue.GlueConstants.AWS_GLUE_ENDPOINT;
import static org.apache.gravitino.catalog.glue.GlueConstants.AWS_REGION;
import static org.apache.gravitino.catalog.glue.GlueConstants.AWS_SECRET_ACCESS_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.glue.GlueClient;

class TestGlueClientProvider {

  @Test
  void testBuildClientWithStaticCredentials() {
    Map<String, String> config = new HashMap<>();
    config.put(AWS_REGION, "us-east-1");
    config.put(AWS_ACCESS_KEY_ID, "test-access-key");
    config.put(AWS_SECRET_ACCESS_KEY, "test-secret-key");

    try (GlueClient client = GlueClientProvider.buildClient(config)) {
      assertNotNull(client);
    }
  }

  // Note: buildClient()'s default-credential-chain branch is not exercised end-to-end here with
  // no static credentials — whether it resolves depends on the machine's real AWS environment
  // (e.g. a developer's ~/.aws/credentials), which would make the test flaky. The fail-fast
  // validation logic itself (validateCredentials) is covered deterministically below with a
  // fake provider instead.

  @Test
  void testValidateCredentialsWithResolvableCredentialsSucceeds() {
    AwsCredentialsProvider provider =
        StaticCredentialsProvider.create(AwsBasicCredentials.create("ak", "sk"));

    GlueClientProvider.validateCredentials(provider);
  }

  @Test
  void testValidateCredentialsWithUnresolvableCredentialsThrows() {
    AwsCredentialsProvider provider = mock(AwsCredentialsProvider.class);
    doThrow(SdkClientException.create("Unable to load credentials from any of the providers"))
        .when(provider)
        .resolveCredentials();

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class, () -> GlueClientProvider.validateCredentials(provider));
    assertTrue(ex.getMessage().contains(AWS_ACCESS_KEY_ID));
    assertTrue(ex.getMessage().contains(AWS_SECRET_ACCESS_KEY));
  }

  @Test
  void testValidateCredentialsWithNonCredentialFailureDoesNotClaimNoCredentials() {
    // A network/IMDS error while resolving credentials is not the same as "no credentials
    // configured" — the message must not assert that no usable credentials exist.
    AwsCredentialsProvider provider = mock(AwsCredentialsProvider.class);
    SdkClientException cause = SdkClientException.create("connection refused");
    doThrow(cause).when(provider).resolveCredentials();

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class, () -> GlueClientProvider.validateCredentials(provider));
    assertEquals(cause, ex.getCause());
    assertTrue(ex.getMessage().contains("connection refused"));
    assertFalse(ex.getMessage().contains("No usable AWS credentials"));
  }

  @Test
  void testBuildClientWithEndpointOverride() {
    Map<String, String> config = new HashMap<>();
    config.put(AWS_REGION, "us-east-1");
    config.put(AWS_ACCESS_KEY_ID, "test");
    config.put(AWS_SECRET_ACCESS_KEY, "test");
    config.put(AWS_GLUE_ENDPOINT, "http://localhost:4566");

    try (GlueClient client = GlueClientProvider.buildClient(config)) {
      assertNotNull(client);
    }
  }

  @Test
  void testBuildClientMissingRegionThrows() {
    Map<String, String> config = new HashMap<>();

    assertThrows(IllegalArgumentException.class, () -> GlueClientProvider.buildClient(config));
  }

  @Test
  void testBuildClientBlankRegionThrows() {
    Map<String, String> config = new HashMap<>();
    config.put(AWS_REGION, "   ");

    assertThrows(IllegalArgumentException.class, () -> GlueClientProvider.buildClient(config));
  }

  @Test
  void testBuildClientOnlyAccessKeyThrows() {
    // Providing only one of the credential pair is always a misconfiguration — must fail fast.
    Map<String, String> config = new HashMap<>();
    config.put(AWS_REGION, "ap-southeast-1");
    config.put(AWS_ACCESS_KEY_ID, "AKIAIOSFODNN7EXAMPLE");
    // No AWS_SECRET_ACCESS_KEY.

    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> GlueClientProvider.buildClient(config));
    assertTrue(ex.getMessage().contains(AWS_SECRET_ACCESS_KEY));
  }

  @Test
  void testBuildClientOnlySecretKeyThrows() {
    // Providing only the secret without the access key is also a misconfiguration.
    Map<String, String> config = new HashMap<>();
    config.put(AWS_REGION, "ap-southeast-1");
    config.put(AWS_SECRET_ACCESS_KEY, "test-secret-key");
    // No AWS_ACCESS_KEY_ID.

    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> GlueClientProvider.buildClient(config));
    assertTrue(ex.getMessage().contains(AWS_ACCESS_KEY_ID));
  }

  @Test
  void testBuildClientInvalidEndpointThrows() {
    Map<String, String> config = new HashMap<>();
    config.put(AWS_REGION, "us-east-1");
    config.put(AWS_GLUE_ENDPOINT, "not a valid uri ://");

    assertThrows(IllegalArgumentException.class, () -> GlueClientProvider.buildClient(config));
  }
}
