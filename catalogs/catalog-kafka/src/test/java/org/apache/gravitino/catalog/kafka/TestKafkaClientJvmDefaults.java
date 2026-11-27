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
package org.apache.gravitino.catalog.kafka;

import static org.apache.gravitino.Catalog.Type.MESSAGING;
import static org.apache.gravitino.StringIdentifier.ID_KEY;
import static org.apache.gravitino.catalog.kafka.KafkaCatalogOperations.AUTOMATIC_CONFIG_PROVIDERS_PROPERTY;
import static org.apache.gravitino.catalog.kafka.KafkaCatalogOperations.OAUTHBEARER_ALLOWED_URLS_PROPERTY;
import static org.apache.gravitino.catalog.kafka.KafkaCatalogPropertiesMetadata.BOOTSTRAP_SERVERS;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.connector.CatalogInfo;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Verifies the Kafka client JVM defaults that {@link KafkaCatalogOperations} sets. An operator sets
 * these properties with {@code -D} in {@code JAVA_OPTS}, {@code JAVA_TOOL_OPTIONS} or {@code
 * JDK_JAVA_OPTIONS} (including {@code @argfiles}). The JVM has merged them into system properties
 * before the server starts, so the catalog only has to leave a property that is already set alone.
 */
public class TestKafkaClientJvmDefaults {

  private static final String[] PROPERTIES = {
    AUTOMATIC_CONFIG_PROVIDERS_PROPERTY, OAUTHBEARER_ALLOWED_URLS_PROPERTY
  };
  private static final String PACKAGE = "org.apache.kafka.common.security.oauthbearer.";
  private static final String HANDLER = PACKAGE + "OAuthBearerLoginCallbackHandler";
  private static final String LEGACY_HANDLER = PACKAGE + "secured.OAuthBearerLoginCallbackHandler";
  private static final String[] HANDLERS = {HANDLER, LEGACY_HANDLER};

  @TempDir Path dir;

  private final Map<String, String> saved = new HashMap<>();

  @BeforeEach
  public void saveAndClearProperties() {
    for (String property : PROPERTIES) {
      saved.put(property, System.getProperty(property));
      System.clearProperty(property);
    }
  }

  @AfterEach
  public void restoreProperties() {
    for (String property : PROPERTIES) {
      String value = saved.get(property);
      if (value == null) {
        System.clearProperty(property);
      } else {
        System.setProperty(property, value);
      }
    }
  }

  @Test
  public void testDefaultsAreSetWhenAbsent() {
    KafkaCatalogOperations.applyClientJvmDefaults();

    Assertions.assertEquals("none", System.getProperty(AUTOMATIC_CONFIG_PROVIDERS_PROPERTY));
    Assertions.assertEquals("", System.getProperty(OAUTHBEARER_ALLOWED_URLS_PROPERTY));
  }

  @Test
  public void testConfiguredValuesAreKept() {
    System.setProperty(AUTOMATIC_CONFIG_PROVIDERS_PROPERTY, "file");
    System.setProperty(OAUTHBEARER_ALLOWED_URLS_PROPERTY, "https://idp.example.com/token");

    KafkaCatalogOperations.applyClientJvmDefaults();

    Assertions.assertEquals("file", System.getProperty(AUTOMATIC_CONFIG_PROVIDERS_PROPERTY));
    Assertions.assertEquals(
        "https://idp.example.com/token", System.getProperty(OAUTHBEARER_ALLOWED_URLS_PROPERTY));
  }

  @Test
  public void testEmptyConfiguredValueIsKept() {
    System.setProperty(OAUTHBEARER_ALLOWED_URLS_PROPERTY, "");

    KafkaCatalogOperations.applyClientJvmDefaults();

    Assertions.assertEquals("", System.getProperty(OAUTHBEARER_ALLOWED_URLS_PROPERTY));
    Assertions.assertEquals("none", System.getProperty(AUTOMATIC_CONFIG_PROVIDERS_PROPERTY));
  }

  @Test
  public void testInitializeRejectsFileTokenEndpointByDefault() throws IOException {
    Path tokenFile = writeTokenFile();

    for (String handler : HANDLERS) {
      Throwable thrown =
          Assertions.assertThrows(
              RuntimeException.class, () -> initialize(oauthBearerConfig(handler, tokenFile)));

      Assertions.assertTrue(
          messages(thrown).contains("is not allowed"),
          handler + " must reject the token endpoint by default, got: " + messages(thrown));
    }
  }

  @Test
  public void testInitializeAllowsConfiguredTokenEndpoint() throws IOException {
    Path tokenFile = writeTokenFile();
    System.setProperty(OAUTHBEARER_ALLOWED_URLS_PROPERTY, tokenFile.toUri().toString());

    for (String handler : HANDLERS) {
      // The allowed endpoint is fetched, and the login then fails because the file is not a token.
      Throwable thrown =
          Assertions.assertThrows(
              RuntimeException.class, () -> initialize(oauthBearerConfig(handler, tokenFile)));

      Assertions.assertFalse(
          messages(thrown).contains("is not allowed"),
          handler + " must accept the configured token endpoint, got: " + messages(thrown));
    }
  }

  private Path writeTokenFile() throws IOException {
    Path tokenFile = dir.resolve("token.json");
    Files.write(tokenFile, "not a token".getBytes(StandardCharsets.UTF_8));
    return tokenFile;
  }

  private static Map<String, String> oauthBearerConfig(String handler, Path tokenFile) {
    return ImmutableMap.<String, String>builder()
        .put(BOOTSTRAP_SERVERS, "127.0.0.1:1")
        .put(ID_KEY, "gravitino.v1.uid33220758755757000")
        .put("gravitino.bypass.security.protocol", "SASL_PLAINTEXT")
        .put("gravitino.bypass.sasl.mechanism", "OAUTHBEARER")
        .put("gravitino.bypass.sasl.login.callback.handler.class", handler)
        .put(
            "gravitino.bypass.sasl.jaas.config",
            "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;")
        .put("gravitino.bypass.sasl.oauthbearer.token.endpoint.url", tokenFile.toUri().toString())
        .build();
  }

  private static void initialize(Map<String, String> config) {
    CatalogEntity entity =
        CatalogEntity.builder()
            .withId(new RandomIdGenerator().nextId())
            .withName("test_kafka_catalog_jvm_defaults")
            .withNamespace(Namespace.of("metalake"))
            .withType(MESSAGING)
            .withProvider("kafka")
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator("testKafkaUser")
                    .withCreateTime(Instant.now())
                    .build())
            .withProperties(config)
            .build();
    CatalogInfo info = entity.toCatalogInfo();
    try (KafkaCatalogOperations ops = new KafkaCatalogOperations(null, new RandomIdGenerator())) {
      ops.initialize(config, info, null);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private static String messages(Throwable thrown) {
    StringBuilder builder = new StringBuilder();
    for (Throwable t = thrown; t != null; t = t.getCause()) {
      builder.append(t.getClass().getSimpleName()).append(": ").append(t.getMessage()).append("; ");
    }
    return builder.toString();
  }
}
