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
package org.apache.gravitino.encryption.kms;

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.Config;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestKmsConfig {

  private static final String AWS_FACTORY =
      "org.apache.gravitino.encryption.kms.aws.AwsKmsClientFactory";
  private static final String GCP_FACTORY =
      "org.apache.gravitino.encryption.kms.gcp.GcpKmsClientFactory";

  @Test
  void testParsesProvidersAndProperties() {
    KmsConfig config =
        parse(
            Map.of(
                "gravitino.kms.providers",
                "primary,disaster-recovery",
                "gravitino.kms.provider.primary.className",
                AWS_FACTORY,
                "gravitino.kms.provider.primary.endpoint.region",
                "us-west-2",
                "gravitino.kms.provider.primary.credential.method",
                "default",
                "gravitino.kms.provider.disaster-recovery.className",
                GCP_FACTORY,
                "gravitino.kms.provider.disaster-recovery.endpoint.projectId",
                "backup-project",
                "gravitino.kms.provider.disaster-recovery.credential.method",
                "default"));

    Assertions.assertEquals(2, config.providers().size());
    Assertions.assertEquals(AWS_FACTORY, config.providers().get("primary").className());
    Assertions.assertEquals(
        Map.of("endpoint.region", "us-west-2", "credential.method", "default"),
        config.providers().get("primary").properties());
    Assertions.assertEquals(GCP_FACTORY, config.providers().get("disaster-recovery").className());
    Assertions.assertEquals(
        Map.of("endpoint.projectId", "backup-project", "credential.method", "default"),
        config.providers().get("disaster-recovery").properties());
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> config.providers().get("primary").properties().put("endpoint.region", "other"));
  }

  @Test
  void testAllowsNoConfiguredProviders() {
    Assertions.assertTrue(parse(Map.of()).providers().isEmpty());
    Assertions.assertTrue(parse(Map.of("gravitino.kms.providers", "  ")).providers().isEmpty());
  }

  @Test
  void testRejectsInvalidOrDuplicateProviderNames() {
    assertInvalid(Map.of("gravitino.kms.providers", "primary,"), "Invalid KMS provider name");
    assertInvalid(Map.of("gravitino.kms.providers", "bad.name"), "Invalid KMS provider name");
    assertInvalid(
        Map.of("gravitino.kms.providers", "primary,primary"), "Duplicate KMS provider 'primary'");
  }

  @Test
  void testRejectsMalformedOrUnlistedProviderProperties() {
    assertInvalid(Map.of("gravitino.kms.unexpected", "value"), "Invalid KMS configuration key");
    assertInvalid(
        Map.of("gravitino.kms.provider.primary", "value"), "Invalid KMS configuration key");
    assertInvalid(
        Map.of("gravitino.kms.provider..className", AWS_FACTORY), "Invalid KMS configuration key");
    assertInvalid(
        Map.of("gravitino.kms.provider.bad$name.className", AWS_FACTORY),
        "Invalid KMS configuration key");
    assertInvalid(
        Map.of(
            "gravitino.kms.providers",
            "primary",
            "gravitino.kms.provider.other.className",
            AWS_FACTORY),
        "unlisted provider 'other'");
    assertInvalid(
        Map.of(
            "gravitino.kms.providers", "primary",
            "gravitino.kms.provider.primary.", "value"),
        "Invalid KMS configuration key");
  }

  @Test
  void testRequiresClassName() {
    assertInvalid(
        Map.of("gravitino.kms.providers", "primary"),
        "gravitino.kms.provider.primary.className' cannot be blank");
    assertInvalid(
        Map.of(
            "gravitino.kms.providers", "primary",
            "gravitino.kms.provider.primary.className", " "),
        "gravitino.kms.provider.primary.className' cannot be blank");
  }

  @Test
  void testAllowsMoreThanOneProviderForAClass() {
    KmsConfig config =
        parse(
            Map.of(
                "gravitino.kms.providers",
                "primary,secondary",
                "gravitino.kms.provider.primary.className",
                AWS_FACTORY,
                "gravitino.kms.provider.secondary.className",
                AWS_FACTORY));

    Assertions.assertEquals(AWS_FACTORY, config.providers().get("primary").className());
    Assertions.assertEquals(AWS_FACTORY, config.providers().get("secondary").className());
  }

  @Test
  void testRejectsNullConfiguration() {
    Assertions.assertThrows(KmsConfigurationException.class, () -> new KmsConfig(null));
  }

  private static KmsConfig parse(Map<String, String> properties) {
    return new KmsConfig(new MapConfig(properties));
  }

  private static void assertInvalid(Map<String, String> properties, String expectedMessage) {
    KmsConfigurationException exception =
        Assertions.assertThrows(KmsConfigurationException.class, () -> parse(properties));
    Assertions.assertTrue(
        exception.getMessage().contains(expectedMessage),
        () -> String.format("Expected '%s' in '%s'", expectedMessage, exception.getMessage()));
  }

  private static final class MapConfig extends Config {
    private MapConfig(Map<String, String> properties) {
      super(false);
      loadFromMap(new HashMap<>(properties), key -> true);
    }
  }
}
