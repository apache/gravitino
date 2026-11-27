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
package org.apache.gravitino.encryption.seal;

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.Config;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSealConfig {

  private static final String LOCAL_FACTORY =
      "org.apache.gravitino.encryption.seal.local.LocalSealClientFactory";
  private static final String AWS_FACTORY =
      "org.apache.gravitino.encryption.seal.aws.AwsSealClientFactory";

  @Test
  void testParsesProvidersAndProperties() {
    SealConfig config =
        parse(
            Map.of(
                "gravitino.seal.providers",
                "primary,disaster-recovery",
                "gravitino.seal.provider.primary.className",
                AWS_FACTORY,
                "gravitino.seal.provider.primary.endpoint.region",
                "us-west-2",
                "gravitino.seal.provider.primary.credential.method",
                "default",
                "gravitino.seal.provider.disaster-recovery.className",
                LOCAL_FACTORY,
                "gravitino.seal.provider.disaster-recovery.keyFile",
                "/tmp/seal-key.b64"));

    Assertions.assertEquals(2, config.providers().size());
    Assertions.assertEquals(AWS_FACTORY, config.providers().get("primary").className());
    Assertions.assertEquals(
        Map.of("endpoint.region", "us-west-2", "credential.method", "default"),
        config.providers().get("primary").properties());
    Assertions.assertEquals(LOCAL_FACTORY, config.providers().get("disaster-recovery").className());
    Assertions.assertEquals(
        Map.of("keyFile", "/tmp/seal-key.b64"),
        config.providers().get("disaster-recovery").properties());
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> config.providers().get("primary").properties().put("endpoint.region", "other"));
  }

  @Test
  void testAllowsNoConfiguredProviders() {
    Assertions.assertTrue(parse(Map.of()).providers().isEmpty());
    Assertions.assertTrue(parse(Map.of("gravitino.seal.providers", "  ")).providers().isEmpty());
  }

  @Test
  void testRejectsInvalidOrDuplicateProviderNames() {
    assertInvalid(Map.of("gravitino.seal.providers", "primary,"), "Invalid seal provider name");
    assertInvalid(Map.of("gravitino.seal.providers", "bad.name"), "Invalid seal provider name");
    assertInvalid(
        Map.of("gravitino.seal.providers", "primary,primary"), "Duplicate seal provider 'primary'");
  }

  @Test
  void testRejectsMalformedOrUnlistedProviderProperties() {
    assertInvalid(Map.of("gravitino.seal.unexpected", "value"), "Invalid seal configuration key");
    assertInvalid(
        Map.of("gravitino.seal.provider.primary", "value"), "Invalid seal configuration key");
    assertInvalid(
        Map.of("gravitino.seal.provider..className", AWS_FACTORY),
        "Invalid seal configuration key");
    assertInvalid(
        Map.of("gravitino.seal.provider.bad$name.className", AWS_FACTORY),
        "Invalid seal configuration key");
    assertInvalid(
        Map.of(
            "gravitino.seal.providers",
            "primary",
            "gravitino.seal.provider.other.className",
            AWS_FACTORY),
        "unlisted provider 'other'");
    assertInvalid(
        Map.of(
            "gravitino.seal.providers", "primary",
            "gravitino.seal.provider.primary.", "value"),
        "Invalid seal configuration key");
  }

  @Test
  void testRequiresClassName() {
    assertInvalid(
        Map.of("gravitino.seal.providers", "primary"),
        "gravitino.seal.provider.primary.className' cannot be blank");
    assertInvalid(
        Map.of(
            "gravitino.seal.providers", "primary",
            "gravitino.seal.provider.primary.className", " "),
        "gravitino.seal.provider.primary.className' cannot be blank");
  }

  @Test
  void testAllowsMoreThanOneProviderForAClass() {
    SealConfig config =
        parse(
            Map.of(
                "gravitino.seal.providers",
                "primary,secondary",
                "gravitino.seal.provider.primary.className",
                AWS_FACTORY,
                "gravitino.seal.provider.secondary.className",
                AWS_FACTORY));

    Assertions.assertEquals(AWS_FACTORY, config.providers().get("primary").className());
    Assertions.assertEquals(AWS_FACTORY, config.providers().get("secondary").className());
  }

  @Test
  void testRejectsNullConfiguration() {
    Assertions.assertThrows(SealConfigurationException.class, () -> new SealConfig(null));
  }

  private static SealConfig parse(Map<String, String> properties) {
    return new SealConfig(new MapConfig(properties));
  }

  private static void assertInvalid(Map<String, String> properties, String expectedMessage) {
    SealConfigurationException exception =
        Assertions.assertThrows(SealConfigurationException.class, () -> parse(properties));
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
