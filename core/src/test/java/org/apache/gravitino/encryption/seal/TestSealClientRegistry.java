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

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.gravitino.Config;
import org.apache.gravitino.encryption.seal.local.LocalSealClientFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestSealClientRegistry {

  private static final String AWS_FACTORY = "test.AwsSealClientFactory";
  private static final String GCP_FACTORY = "test.GcpSealClientFactory";
  private static final String AZURE_FACTORY = "test.AzureSealClientFactory";

  @TempDir Path tempDir;

  @Test
  void testEmptyRegistryDoesNotLoadFactories() {
    SealClientRegistry.FactoryLoader loader =
        className -> {
          throw new AssertionError("Factories must not be loaded without configured providers");
        };

    try (SealClientRegistry registry = new SealClientRegistry(config(), loader)) {
      IllegalArgumentException exception =
          Assertions.assertThrows(
              IllegalArgumentException.class, () -> registry.getClient("primary"));
      Assertions.assertEquals(
          "No seal client is configured for provider 'primary'", exception.getMessage());
    }
  }

  @Test
  void testCreatesAndDispatchesConfiguredClients() {
    RecordingFactory awsFactory = new RecordingFactory();
    RecordingFactory gcpFactory = new RecordingFactory();
    SealClientRegistry registry =
        new SealClientRegistry(
            config(
                "gravitino.seal.providers",
                "primary,analytics",
                "gravitino.seal.provider.primary.className",
                AWS_FACTORY,
                "gravitino.seal.provider.primary.endpoint.region",
                "us-west-2",
                "gravitino.seal.provider.analytics.className",
                GCP_FACTORY,
                "gravitino.seal.provider.analytics.endpoint.project",
                "data-project"),
            loader(Map.of(AWS_FACTORY, awsFactory, GCP_FACTORY, gcpFactory)));

    SealClient awsClient = registry.getClient("primary");
    SealClient gcpClient = registry.getClient("analytics");

    Assertions.assertSame(awsClient, registry.getClient("primary"));
    Assertions.assertSame(gcpClient, registry.getClient("analytics"));
    Assertions.assertEquals(Map.of("endpoint.region", "us-west-2"), awsFactory.properties);
    Assertions.assertEquals(Map.of("endpoint.project", "data-project"), gcpFactory.properties);
    Assertions.assertEquals("primary", awsFactory.createdProvider);
    Assertions.assertEquals("analytics", gcpFactory.createdProvider);
    Assertions.assertEquals(1, awsFactory.createCount.get());
    Assertions.assertEquals(1, gcpFactory.createCount.get());
  }

  @Test
  void testRejectsUnknownProvider() {
    SealClientRegistry registry =
        new SealClientRegistry(
            config(
                "gravitino.seal.providers",
                "primary",
                "gravitino.seal.provider.primary.className",
                AWS_FACTORY),
            loader(Map.of(AWS_FACTORY, new RecordingFactory())));

    Assertions.assertThrows(IllegalArgumentException.class, () -> registry.getClient("other"));
    Assertions.assertDoesNotThrow(() -> registry.getClient("primary"));
    Assertions.assertThrows(IllegalArgumentException.class, () -> registry.getClient(null));
    Assertions.assertThrows(IllegalArgumentException.class, () -> registry.getClient("  "));
  }

  @Test
  void testCreatesMultipleProvidersForSameClass() {
    RecordingFactory factory = new RecordingFactory();
    SealClientRegistry registry =
        new SealClientRegistry(
            config(
                "gravitino.seal.providers",
                "azure-eu,azure-us",
                "gravitino.seal.provider.azure-eu.className",
                AZURE_FACTORY,
                "gravitino.seal.provider.azure-us.className",
                AZURE_FACTORY),
            loader(Map.of(AZURE_FACTORY, factory)));

    SealClient euClient = registry.getClient("azure-eu");
    SealClient usClient = registry.getClient("azure-us");

    Assertions.assertSame(euClient, registry.getClient("azure-eu"));
    Assertions.assertSame(usClient, registry.getClient("azure-us"));
    Assertions.assertEquals(2, factory.createCount.get());
  }

  @Test
  void testRejectsMissingFactoryClass() {
    Config awsConfig =
        config(
            "gravitino.seal.providers",
            "primary",
            "gravitino.seal.provider.primary.className",
            AWS_FACTORY);

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new SealClientRegistry(awsConfig, loader(Map.of())));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new SealClientRegistry(awsConfig, (SealClientRegistry.FactoryLoader) null));
  }

  @Test
  void testPublicConstructorRejectsUnknownClass() {
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                new SealClientRegistry(
                    config(
                        "gravitino.seal.providers",
                        "primary",
                        "gravitino.seal.provider.primary.className",
                        "test.MissingSealClientFactory")));
    Assertions.assertTrue(exception.getMessage().contains("No seal client factory class"));
  }

  @Test
  void testPublicConstructorRejectsClassNotImplementingFactory() {
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                new SealClientRegistry(
                    config(
                        "gravitino.seal.providers",
                        "primary",
                        "gravitino.seal.provider.primary.className",
                        String.class.getName())));
    Assertions.assertTrue(exception.getMessage().contains("does not implement SealClientFactory"));
  }

  @Test
  void testPublicConstructorRejectsFactoryWithoutNoArgConstructor() {
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                new SealClientRegistry(
                    config(
                        "gravitino.seal.providers",
                        "primary",
                        "gravitino.seal.provider.primary.className",
                        Integer.class.getName())));
    Assertions.assertTrue(exception.getMessage().contains("Failed to create seal client factory"));
  }

  @Test
  void testPublicConstructorLoadsLocalSealFromConfiguration() throws Exception {
    byte[] keyBytes = new byte[32];
    new SecureRandom().nextBytes(keyBytes);
    Path keyFile = tempDir.resolve("seal-key.b64");
    Files.writeString(keyFile, Base64.getEncoder().encodeToString(keyBytes));

    try (SealClientRegistry registry =
        new SealClientRegistry(
            config(
                "gravitino.seal.providers",
                "local",
                "gravitino.seal.provider.local.className",
                LocalSealClientFactory.class.getName(),
                "gravitino.seal.provider.local.keyFile",
                keyFile.toString()))) {
      SealClient client = registry.getClient("local");
      byte[] masterKey = new byte[32];
      new SecureRandom().nextBytes(masterKey);
      Assertions.assertArrayEquals(masterKey, client.unwrap(client.wrap(masterKey)));
    }
  }

  @Test
  void testRejectsNullClientAndClosesPreviouslyCreatedClient() {
    CloseTrackingFactory awsFactory = new CloseTrackingFactory("aws", new ArrayList<>(), null);
    Config awsConfig =
        config(
            "gravitino.seal.providers",
            "primary,analytics",
            "gravitino.seal.provider.primary.className",
            AWS_FACTORY,
            "gravitino.seal.provider.analytics.className",
            GCP_FACTORY);

    SealClientFactory nullClientFactory = (provider, properties) -> null;

    Assertions.assertThrows(
        IllegalStateException.class,
        () ->
            new SealClientRegistry(
                awsConfig,
                loader(Map.of(AWS_FACTORY, awsFactory, GCP_FACTORY, nullClientFactory))));
    Assertions.assertEquals(1, awsFactory.closeCount.get());
  }

  @Test
  void testClosesClientsInReverseOrderAndIsIdempotent() {
    List<String> closeOrder = new ArrayList<>();
    CloseTrackingFactory awsFactory = new CloseTrackingFactory("aws", closeOrder, null);
    CloseTrackingFactory gcpFactory = new CloseTrackingFactory("gcp", closeOrder, null);
    SealClientRegistry registry =
        new SealClientRegistry(
            config(
                "gravitino.seal.providers",
                "primary,analytics",
                "gravitino.seal.provider.primary.className",
                AWS_FACTORY,
                "gravitino.seal.provider.analytics.className",
                GCP_FACTORY),
            loader(Map.of(AWS_FACTORY, awsFactory, GCP_FACTORY, gcpFactory)));

    registry.close();
    registry.close();

    Assertions.assertEquals(List.of("gcp", "aws"), closeOrder);
    Assertions.assertEquals(1, awsFactory.closeCount.get());
    Assertions.assertEquals(1, gcpFactory.closeCount.get());
    Assertions.assertThrows(IllegalStateException.class, () -> registry.getClient("primary"));
  }

  @Test
  void testClosesCreatedClientsAfterPartialInitializationFailure() {
    List<String> closeOrder = new ArrayList<>();
    CloseTrackingFactory awsFactory = new CloseTrackingFactory("aws", closeOrder, null);
    SealClientFactory failingFactory =
        (provider, properties) -> {
          throw new IllegalArgumentException("invalid GCP configuration");
        };

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            new SealClientRegistry(
                config(
                    "gravitino.seal.providers",
                    "primary,analytics",
                    "gravitino.seal.provider.primary.className",
                    AWS_FACTORY,
                    "gravitino.seal.provider.analytics.className",
                    GCP_FACTORY),
                loader(Map.of(AWS_FACTORY, awsFactory, GCP_FACTORY, failingFactory))));

    Assertions.assertEquals(1, awsFactory.closeCount.get());
    Assertions.assertEquals(List.of("aws"), closeOrder);
  }

  @Test
  void testPreservesInitializationFailureWhenCleanupFails() {
    RuntimeException closeFailure = new IllegalStateException("close failed");
    CloseTrackingFactory awsFactory =
        new CloseTrackingFactory("aws", new ArrayList<>(), closeFailure);
    IllegalArgumentException creationFailure =
        new IllegalArgumentException("invalid GCP configuration");
    SealClientFactory failingFactory =
        (provider, properties) -> {
          throw creationFailure;
        };

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                new SealClientRegistry(
                    config(
                        "gravitino.seal.providers",
                        "primary,analytics",
                        "gravitino.seal.provider.primary.className",
                        AWS_FACTORY,
                        "gravitino.seal.provider.analytics.className",
                        GCP_FACTORY),
                    loader(Map.of(AWS_FACTORY, awsFactory, GCP_FACTORY, failingFactory))));

    Assertions.assertSame(creationFailure, exception);
    Assertions.assertArrayEquals(new Throwable[] {closeFailure}, exception.getSuppressed());
  }

  @Test
  void testAggregatesCloseFailures() {
    RuntimeException awsFailure = new IllegalStateException("aws close failed");
    RuntimeException gcpFailure = new IllegalStateException("gcp close failed");
    CloseTrackingFactory awsFactory =
        new CloseTrackingFactory("aws", new ArrayList<>(), awsFailure);
    CloseTrackingFactory gcpFactory =
        new CloseTrackingFactory("gcp", new ArrayList<>(), gcpFailure);
    SealClientRegistry registry =
        new SealClientRegistry(
            config(
                "gravitino.seal.providers",
                "primary,analytics",
                "gravitino.seal.provider.primary.className",
                AWS_FACTORY,
                "gravitino.seal.provider.analytics.className",
                GCP_FACTORY),
            loader(Map.of(AWS_FACTORY, awsFactory, GCP_FACTORY, gcpFactory)));

    RuntimeException exception = Assertions.assertThrows(RuntimeException.class, registry::close);
    Assertions.assertSame(gcpFailure, exception);
    Assertions.assertArrayEquals(new Throwable[] {awsFailure}, exception.getSuppressed());
  }

  private static Config config(String... entries) {
    Map<String, String> properties = new HashMap<>();
    for (int index = 0; index < entries.length; index += 2) {
      properties.put(entries[index], entries[index + 1]);
    }
    return new MapConfig(properties);
  }

  private static SealClientRegistry.FactoryLoader loader(Map<String, SealClientFactory> factories) {
    return className -> {
      SealClientFactory factory = factories.get(className);
      if (factory == null) {
        throw new IllegalArgumentException(
            String.format("No seal client factory class '%s'", className));
      }
      return factory;
    };
  }

  private static final class RecordingFactory implements SealClientFactory {
    private String createdProvider;
    private Map<String, String> properties;
    private final AtomicInteger createCount = new AtomicInteger();

    @Override
    public SealClient create(String provider, Map<String, String> properties) {
      createCount.incrementAndGet();
      this.createdProvider = provider;
      this.properties = properties;
      return new SealClient() {
        @Override
        public byte[] wrap(byte[] plaintext) {
          return plaintext.clone();
        }

        @Override
        public byte[] unwrap(byte[] wrapped) {
          return wrapped.clone();
        }
      };
    }
  }

  private static final class CloseTrackingFactory implements SealClientFactory {
    private final String name;
    private final List<String> closeOrder;
    private final RuntimeException closeFailure;
    private final AtomicInteger closeCount = new AtomicInteger();

    private CloseTrackingFactory(
        String name, List<String> closeOrder, RuntimeException closeFailure) {
      this.name = name;
      this.closeOrder = closeOrder;
      this.closeFailure = closeFailure;
    }

    @Override
    public SealClient create(String provider, Map<String, String> properties) {
      return new SealClient() {
        @Override
        public byte[] wrap(byte[] plaintext) {
          return plaintext.clone();
        }

        @Override
        public byte[] unwrap(byte[] wrapped) {
          return wrapped.clone();
        }

        @Override
        public void close() {
          closeCount.incrementAndGet();
          closeOrder.add(name);
          if (closeFailure != null) {
            throw closeFailure;
          }
        }
      };
    }
  }

  private static final class MapConfig extends Config {
    private MapConfig(Map<String, String> properties) {
      super(false);
      loadFromMap(properties, key -> true);
    }
  }
}
