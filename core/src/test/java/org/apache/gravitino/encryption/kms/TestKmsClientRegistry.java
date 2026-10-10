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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.gravitino.Config;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestKmsClientRegistry {

  private static final String AWS_FACTORY = "test.AwsKmsClientFactory";
  private static final String GCP_FACTORY = "test.GcpKmsClientFactory";
  private static final String AZURE_FACTORY = "test.AzureKmsClientFactory";

  @Test
  void testEmptyRegistryDoesNotLoadFactories() {
    KmsClientRegistry.FactoryLoader loader =
        className -> {
          throw new AssertionError("Factories must not be loaded without configured providers");
        };
    KmsClientRegistry registry = new KmsClientRegistry(config(), loader);
    KmsReference reference = new KmsReference("primary", "alias/orders");

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> registry.getClient(reference));
    Assertions.assertEquals(
        "No KMS client is configured for provider 'primary'", exception.getMessage());
  }

  @Test
  void testCreatesAndDispatchesConfiguredClients() {
    RecordingFactory awsFactory = new RecordingFactory();
    RecordingFactory gcpFactory = new RecordingFactory();
    KmsClientRegistry registry =
        new KmsClientRegistry(
            config(
                "gravitino.kms.providers",
                "primary,analytics",
                "gravitino.kms.provider.primary.className",
                AWS_FACTORY,
                "gravitino.kms.provider.primary.endpoint.region",
                "us-west-2",
                "gravitino.kms.provider.analytics.className",
                GCP_FACTORY,
                "gravitino.kms.provider.analytics.endpoint.project",
                "data-project"),
            loader(Map.of(AWS_FACTORY, awsFactory, GCP_FACTORY, gcpFactory)));

    KmsReference awsReference = new KmsReference("primary", "alias/orders");
    KmsReference gcpReference =
        new KmsReference("analytics", "projects/p/locations/l/keyRings/r/cryptoKeys/k");

    KmsClient awsClient = registry.getClient(awsReference);
    KmsClient gcpClient = registry.getClient(gcpReference);

    Assertions.assertSame(awsClient, registry.getClient(awsReference));
    Assertions.assertSame(gcpClient, registry.getClient(gcpReference));
    Assertions.assertEquals(Map.of("endpoint.region", "us-west-2"), awsFactory.properties);
    Assertions.assertEquals(Map.of("endpoint.project", "data-project"), gcpFactory.properties);
    Assertions.assertEquals("primary", awsFactory.createdProvider);
    Assertions.assertEquals("analytics", gcpFactory.createdProvider);
    Assertions.assertEquals(1, awsFactory.createCount.get());
    Assertions.assertEquals(1, gcpFactory.createCount.get());
  }

  @Test
  void testRejectsUnknownProvider() {
    KmsClientRegistry registry =
        new KmsClientRegistry(
            config(
                "gravitino.kms.providers",
                "primary",
                "gravitino.kms.provider.primary.className",
                AWS_FACTORY),
            loader(Map.of(AWS_FACTORY, new RecordingFactory())));

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> registry.getClient(new KmsReference("other", "key")));
    Assertions.assertDoesNotThrow(() -> registry.getClient(new KmsReference("primary", "key")));
    Assertions.assertThrows(IllegalArgumentException.class, () -> registry.getClient(null));
  }

  @Test
  void testCreatesMultipleProvidersForSameClass() {
    RecordingFactory factory = new RecordingFactory();
    KmsClientRegistry registry =
        new KmsClientRegistry(
            config(
                "gravitino.kms.providers",
                "azure-eu,azure-us",
                "gravitino.kms.provider.azure-eu.className",
                AZURE_FACTORY,
                "gravitino.kms.provider.azure-us.className",
                AZURE_FACTORY),
            loader(Map.of(AZURE_FACTORY, factory)));

    KmsReference euReference = new KmsReference("azure-eu", "primary");
    KmsReference usReference = new KmsReference("azure-us", "primary");

    KmsClient euClient = registry.getClient(euReference);
    KmsClient usClient = registry.getClient(usReference);

    Assertions.assertSame(euClient, registry.getClient(euReference));
    Assertions.assertSame(usClient, registry.getClient(usReference));
    Assertions.assertEquals(2, factory.createCount.get());
  }

  @Test
  void testRejectsMissingFactoryClass() {
    Config awsConfig =
        config(
            "gravitino.kms.providers",
            "primary",
            "gravitino.kms.provider.primary.className",
            AWS_FACTORY);

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new KmsClientRegistry(awsConfig, loader(Map.of())));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new KmsClientRegistry(awsConfig, (KmsClientRegistry.FactoryLoader) null));
  }

  @Test
  void testPublicConstructorLoadsFactoryByClassName() {
    try (KmsClientRegistry registry =
        new KmsClientRegistry(
            config(
                "gravitino.kms.providers",
                "primary",
                "gravitino.kms.provider.primary.className",
                ClassLoadedFactory.class.getName()))) {
      KmsReference reference = new KmsReference("primary", "key");
      Assertions.assertNotNull(registry.getClient(reference));
    }
  }

  @Test
  void testPublicConstructorRejectsUnknownClass() {
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                new KmsClientRegistry(
                    config(
                        "gravitino.kms.providers",
                        "primary",
                        "gravitino.kms.provider.primary.className",
                        "test.MissingKmsClientFactory")));
    Assertions.assertTrue(exception.getMessage().contains("No KMS client factory class"));
  }

  @Test
  void testRejectsNullClientAndClosesPreviouslyCreatedClient() {
    CloseTrackingFactory awsFactory = new CloseTrackingFactory("aws", new ArrayList<>(), null);
    Config awsConfig =
        config(
            "gravitino.kms.providers",
            "primary,analytics",
            "gravitino.kms.provider.primary.className",
            AWS_FACTORY,
            "gravitino.kms.provider.analytics.className",
            GCP_FACTORY);

    KmsClientFactory nullClientFactory = factory((provider, properties) -> null);

    Assertions.assertThrows(
        IllegalStateException.class,
        () ->
            new KmsClientRegistry(
                awsConfig,
                loader(Map.of(AWS_FACTORY, awsFactory, GCP_FACTORY, nullClientFactory))));
    Assertions.assertEquals(1, awsFactory.closeCount.get());
  }

  @Test
  void testClosesClientsInReverseOrderAndIsIdempotent() {
    List<String> closeOrder = new ArrayList<>();
    CloseTrackingFactory awsFactory = new CloseTrackingFactory("aws", closeOrder, null);
    CloseTrackingFactory gcpFactory = new CloseTrackingFactory("gcp", closeOrder, null);
    KmsClientRegistry registry =
        new KmsClientRegistry(
            config(
                "gravitino.kms.providers",
                "primary,analytics",
                "gravitino.kms.provider.primary.className",
                AWS_FACTORY,
                "gravitino.kms.provider.analytics.className",
                GCP_FACTORY),
            loader(Map.of(AWS_FACTORY, awsFactory, GCP_FACTORY, gcpFactory)));
    KmsReference awsReference = new KmsReference("primary", "key");

    registry.close();
    registry.close();

    Assertions.assertEquals(List.of("gcp", "aws"), closeOrder);
    Assertions.assertEquals(1, awsFactory.closeCount.get());
    Assertions.assertEquals(1, gcpFactory.closeCount.get());
    Assertions.assertThrows(IllegalStateException.class, () -> registry.getClient(awsReference));
  }

  @Test
  void testClosesCreatedClientsAfterPartialInitializationFailure() {
    List<String> closeOrder = new ArrayList<>();
    CloseTrackingFactory awsFactory = new CloseTrackingFactory("aws", closeOrder, null);
    KmsClientFactory failingFactory =
        factory(
            (provider, properties) -> {
              throw new IllegalArgumentException("invalid GCP configuration");
            });

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            new KmsClientRegistry(
                config(
                    "gravitino.kms.providers",
                    "primary,analytics",
                    "gravitino.kms.provider.primary.className",
                    AWS_FACTORY,
                    "gravitino.kms.provider.analytics.className",
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
    KmsClientFactory failingFactory =
        factory(
            (provider, properties) -> {
              throw creationFailure;
            });

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                new KmsClientRegistry(
                    config(
                        "gravitino.kms.providers",
                        "primary,analytics",
                        "gravitino.kms.provider.primary.className",
                        AWS_FACTORY,
                        "gravitino.kms.provider.analytics.className",
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
    KmsClientRegistry registry =
        new KmsClientRegistry(
            config(
                "gravitino.kms.providers",
                "primary,analytics",
                "gravitino.kms.provider.primary.className",
                AWS_FACTORY,
                "gravitino.kms.provider.analytics.className",
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

  private static KmsClientRegistry.FactoryLoader loader(Map<String, KmsClientFactory> factories) {
    return className -> {
      KmsClientFactory factory = factories.get(className);
      if (factory == null) {
        throw new IllegalArgumentException(
            String.format("No KMS client factory class '%s'", className));
      }
      return factory;
    };
  }

  private static KmsClientFactory factory(ClientCreator creator) {
    return creator::create;
  }

  private interface ClientCreator {
    KmsClient create(String provider, Map<String, String> properties);
  }

  private static final class RecordingFactory implements KmsClientFactory {
    private String createdProvider;
    private Map<String, String> properties;
    private final AtomicInteger createCount = new AtomicInteger();

    @Override
    public KmsClient create(String provider, Map<String, String> properties) {
      createCount.incrementAndGet();
      this.createdProvider = provider;
      this.properties = properties;
      return reference -> Optional.of(new Properties(reference));
    }
  }

  private static final class CloseTrackingFactory implements KmsClientFactory {
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
    public KmsClient create(String provider, Map<String, String> properties) {
      return new KmsClient() {
        @Override
        public Optional<KmsKeyProperties> getKeyProperties(KmsReference reference) {
          return Optional.of(new Properties(reference));
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

  private static final class Properties implements KmsKeyProperties {
    private final KmsReference reference;

    private Properties(KmsReference reference) {
      this.reference = reference;
    }

    @Override
    public KmsReference reference() {
      return reference;
    }

    @Override
    public boolean enabled() {
      return true;
    }

    @Override
    public boolean supportsWrapping() {
      return true;
    }

    @Override
    public boolean supportsUnwrapping() {
      return true;
    }
  }

  private static final class MapConfig extends Config {
    private MapConfig(Map<String, String> properties) {
      super(false);
      loadFromMap(properties, key -> true);
    }
  }

  /** Factory loaded by {@link Class#forName(String)} in the public-constructor test. */
  public static final class ClassLoadedFactory implements KmsClientFactory {

    /** Creates a test factory. */
    public ClassLoadedFactory() {}

    /** {@inheritDoc} */
    @Override
    public KmsClient create(String provider, Map<String, String> properties) {
      return reference -> Optional.of(new Properties(reference));
    }
  }
}
