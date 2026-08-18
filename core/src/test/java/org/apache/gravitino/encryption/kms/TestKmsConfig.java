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

  private static final String AWS_API = "aws-kms";
  private static final String GCP_API = "google-cloud-kms";

  @Test
  void testParsesProvidersAndProperties() {
    KmsConfig config =
        parse(
            Map.of(
                "gravitino.kms.providers", "primary, disaster-recovery",
                "gravitino.kms.provider.primary.api", AWS_API,
                "gravitino.kms.provider.primary.endpoint.region", "us-west-2",
                "gravitino.kms.provider.primary.credential.method", "default",
                "gravitino.kms.provider.disaster-recovery.api", "google-cloud-kms",
                "gravitino.kms.provider.disaster-recovery.endpoint.projectId", "backup-project",
                "gravitino.kms.provider.disaster-recovery.credential.method", "default"));

    Assertions.assertEquals(2, config.providers().size());
    Assertions.assertEquals(AWS_API, config.providers().get("primary").api());
    Assertions.assertEquals(
        Map.of("endpoint.region", "us-west-2", "credential.method", "default"),
        config.providers().get("primary").properties());
    Assertions.assertEquals(GCP_API, config.providers().get("disaster-recovery").api());
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
    assertInvalid(Map.of("gravitino.kms.provider..api", AWS_API), "Invalid KMS configuration key");
    assertInvalid(
        Map.of("gravitino.kms.provider.bad$name.api", AWS_API), "Invalid KMS configuration key");
    assertInvalid(
        Map.of(
            "gravitino.kms.providers", "primary",
            "gravitino.kms.provider.other.api", "aws-kms"),
        "unlisted provider 'other'");
    assertInvalid(
        Map.of(
            "gravitino.kms.providers", "primary",
            "gravitino.kms.provider.primary.", "value"),
        "Invalid KMS configuration key");
  }

  @Test
  void testRequiresApi() {
    assertInvalid(
        Map.of("gravitino.kms.providers", "primary"),
        "gravitino.kms.provider.primary.api' cannot be blank");
    assertInvalid(
        Map.of(
            "gravitino.kms.providers", "primary",
            "gravitino.kms.provider.primary.api", " "),
        "gravitino.kms.provider.primary.api' cannot be blank");
    assertInvalid(
        Map.of(
            "gravitino.kms.providers", "primary",
            "gravitino.kms.provider.primary.api", "Custom-KMS"),
        "must be lowercase kebab-case");
  }

  @Test
  void testAllowsMoreThanOneProviderForAnApi() {
    KmsConfig config =
        parse(
            Map.of(
                "gravitino.kms.providers", "primary,secondary",
                "gravitino.kms.provider.primary.api", "aws-kms",
                "gravitino.kms.provider.secondary.api", "aws-kms"));

    Assertions.assertEquals(AWS_API, config.providers().get("primary").api());
    Assertions.assertEquals(AWS_API, config.providers().get("secondary").api());
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
