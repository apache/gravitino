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

  private static final KmsApi AWS_API = KmsApi.AWS_KMS;
  private static final KmsApi GCP_API = KmsApi.GOOGLE_CLOUD_KMS;

  @Test
  void testParsesSourcesAndProviderProperties() {
    KmsConfig config =
        parse(
            Map.of(
                "gravitino.kms.sources", "primary, disaster-recovery",
                "gravitino.kms.source.primary.api", " AWS-KMS ",
                "gravitino.kms.source.primary.endpoint.region", "us-west-2",
                "gravitino.kms.source.primary.credential.method", "default",
                "gravitino.kms.source.disaster-recovery.api", "google-cloud-kms",
                "gravitino.kms.source.disaster-recovery.endpoint.projectId", "backup-project",
                "gravitino.kms.source.disaster-recovery.credential.method", "default"));

    Assertions.assertEquals(2, config.sources().size());
    Assertions.assertEquals(AWS_API, config.sources().get("primary").api());
    Assertions.assertEquals(
        Map.of("endpoint.region", "us-west-2", "credential.method", "default"),
        config.sources().get("primary").properties());
    Assertions.assertEquals(GCP_API, config.sources().get("disaster-recovery").api());
    Assertions.assertEquals(
        Map.of("endpoint.projectId", "backup-project", "credential.method", "default"),
        config.sources().get("disaster-recovery").properties());
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> config.sources().get("primary").properties().put("endpoint.region", "other"));
  }

  @Test
  void testAllowsNoConfiguredSources() {
    Assertions.assertTrue(parse(Map.of()).sources().isEmpty());
    Assertions.assertTrue(parse(Map.of("gravitino.kms.sources", "  ")).sources().isEmpty());
  }

  @Test
  void testRejectsInvalidOrDuplicateSourceNames() {
    assertInvalid(Map.of("gravitino.kms.sources", "primary,"), "Invalid KMS source name");
    assertInvalid(Map.of("gravitino.kms.sources", "bad.name"), "Invalid KMS source name");
    assertInvalid(
        Map.of("gravitino.kms.sources", "primary,primary"), "Duplicate KMS source 'primary'");
  }

  @Test
  void testRejectsMalformedOrUnlistedSourceProperties() {
    assertInvalid(Map.of("gravitino.kms.unexpected", "value"), "Invalid KMS configuration key");
    assertInvalid(
        Map.of(
            "gravitino.kms.sources", "primary",
            "gravitino.kms.source.other.api", "aws-kms"),
        "unlisted source 'other'");
    assertInvalid(
        Map.of(
            "gravitino.kms.sources", "primary",
            "gravitino.kms.source.primary.", "value"),
        "Invalid KMS configuration key");
  }

  @Test
  void testRequiresApi() {
    assertInvalid(
        Map.of("gravitino.kms.sources", "primary"),
        "gravitino.kms.source.primary.api' cannot be blank");
    assertInvalid(
        Map.of(
            "gravitino.kms.sources", "primary",
            "gravitino.kms.source.primary.api", " "),
        "gravitino.kms.source.primary.api' cannot be blank");

    assertInvalid(
        Map.of(
            "gravitino.kms.sources", "primary",
            "gravitino.kms.source.primary.api", " Custom-KMS "),
        "Unsupported KMS API 'Custom-KMS'");
  }

  @Test
  void testAllowsMoreThanOneSourceForAnApi() {
    KmsConfig config =
        parse(
            Map.of(
                "gravitino.kms.sources", "primary,secondary",
                "gravitino.kms.source.primary.api", "aws-kms",
                "gravitino.kms.source.secondary.api", "aws-kms"));

    Assertions.assertEquals(AWS_API, config.sources().get("primary").api());
    Assertions.assertEquals(AWS_API, config.sources().get("secondary").api());
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
