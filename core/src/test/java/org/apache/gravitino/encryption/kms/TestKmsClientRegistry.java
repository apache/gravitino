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

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.gravitino.Config;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestKmsClientRegistry {

  private static final String AWS_API = "aws-kms";
  private static final String GCP_API = "google-cloud-kms";
  private static final String AZURE_API = "azure-key-vault";

  @Test
  void testEmptyRegistryDoesNotEnumerateFactories() {
    Iterable<KmsClientFactory> factories =
        () -> {
          throw new AssertionError("Factories must not be enumerated without configured providers");
        };
    KmsClientRegistry registry = new KmsClientRegistry(config(), factories);
    KmsReference reference = new KmsReference("primary", "alias/orders");

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> registry.getClient(reference));
    Assertions.assertEquals(
        "No KMS client is configured for provider 'primary'", exception.getMessage());
  }

  @Test
  void testCreatesAndDispatchesConfiguredClients() {
    RecordingFactory awsFactory = new RecordingFactory(AWS_API);
    RecordingFactory gcpFactory = new RecordingFactory(GCP_API);
    KmsClientRegistry registry =
        new KmsClientRegistry(
            config(
                "gravitino.kms.providers", "primary,analytics",
                "gravitino.kms.provider.primary.api", AWS_API,
                "gravitino.kms.provider.primary.endpoint.region", "us-west-2",
                "gravitino.kms.provider.analytics.api", "google-cloud-kms",
                "gravitino.kms.provider.analytics.endpoint.project", "data-project"),
            List.of(awsFactory, gcpFactory));

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
                "gravitino.kms.providers", "primary",
                "gravitino.kms.provider.primary.api", "aws-kms"),
            List.of(new RecordingFactory(AWS_API)));

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> registry.getClient(new KmsReference("other", "key")));
    Assertions.assertDoesNotThrow(() -> registry.getClient(new KmsReference("primary", "key")));
    Assertions.assertThrows(IllegalArgumentException.class, () -> registry.getClient(null));
  }

  @Test
  void testCreatesMultipleProvidersForSameApi() {
    RecordingFactory factory = new RecordingFactory(AZURE_API);
    KmsClientRegistry registry =
        new KmsClientRegistry(
            config(
                "gravitino.kms.providers", "azure-eu,azure-us",
                "gravitino.kms.provider.azure-eu.api", "azure-key-vault",
                "gravitino.kms.provider.azure-us.api", "azure-key-vault"),
            List.of(factory));

    KmsReference euReference = new KmsReference("azure-eu", "primary");
    KmsReference usReference = new KmsReference("azure-us", "primary");

    KmsClient euClient = registry.getClient(euReference);
    KmsClient usClient = registry.getClient(usReference);

    Assertions.assertSame(euClient, registry.getClient(euReference));
    Assertions.assertSame(usClient, registry.getClient(usReference));
    Assertions.assertEquals(2, factory.createCount.get());
  }

  @Test
  void testRoutesCustomApi() {
    String customApi = "custom-kms";
    KmsClientRegistry registry =
        new KmsClientRegistry(
            config(
                "gravitino.kms.providers",
                "custom",
                "gravitino.kms.provider.custom.api",
                customApi),
            List.of(new RecordingFactory(customApi)));
    KmsReference reference = new KmsReference("custom", "key");

    Assertions.assertNotNull(registry.getClient(reference));
  }

  @Test
  void testMatchesApiIdentifiersByValue() {
    KmsClientRegistry registry =
        new KmsClientRegistry(
            config(
                "gravitino.kms.providers",
                "primary",
                "gravitino.kms.provider.primary.api",
                new String(AWS_API)),
            List.of(new RecordingFactory(new String(AWS_API))));
    KmsReference reference = new KmsReference("primary", "key");

    Assertions.assertNotNull(registry.getClient(reference));
  }

  @Test
  void testRejectsMissingDuplicateAndInvalidFactories() {
    Config awsConfig =
        config(
            "gravitino.kms.providers", "primary",
            "gravitino.kms.provider.primary.api", "aws-kms");

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new KmsClientRegistry(awsConfig, List.of()));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            new KmsClientRegistry(
                awsConfig, List.of(new RecordingFactory(AWS_API), new RecordingFactory(AWS_API))));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new KmsClientRegistry(awsConfig, List.of(new RecordingFactory(null))));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new KmsClientRegistry(awsConfig, List.of(new RecordingFactory(" "))));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new KmsClientRegistry(awsConfig, List.of(new RecordingFactory(" aws-kms"))));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new KmsClientRegistry(awsConfig, List.of(new RecordingFactory("AWS-KMS"))));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new KmsClientRegistry(awsConfig, java.util.Arrays.asList((KmsClientFactory) null)));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new KmsClientRegistry(awsConfig, null));
  }

  @Test
  void testRejectsConfiguredApiWithoutFactory() {
    Config customConfig =
        config(
            "gravitino.kms.providers", "primary",
            "gravitino.kms.provider.primary.api", "custom-kms");

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new KmsClientRegistry(customConfig, List.of(new RecordingFactory(AWS_API))));
    Assertions.assertTrue(
        exception.getMessage().contains("No KMS client factory supports API 'custom-kms'"));
  }

  @Test
  void testPublicConstructorUsesContextClassLoader(@TempDir Path tempDirectory) throws Exception {
    Path serviceFile =
        tempDirectory.resolve(
            "META-INF/services/org.apache.gravitino.encryption.kms.KmsClientFactory");
    Files.createDirectories(serviceFile.getParent());
    Files.write(serviceFile, ServiceLoadedFactory.class.getName().getBytes(StandardCharsets.UTF_8));

    ClassLoader originalClassLoader = Thread.currentThread().getContextClassLoader();
    try (URLClassLoader serviceClassLoader =
        new URLClassLoader(new URL[] {tempDirectory.toUri().toURL()}, originalClassLoader)) {
      Thread.currentThread().setContextClassLoader(serviceClassLoader);
      try (KmsClientRegistry registry =
          new KmsClientRegistry(
              config(
                  "gravitino.kms.providers", "primary",
                  "gravitino.kms.provider.primary.api", "aws-kms"))) {
        KmsReference reference = new KmsReference("primary", "key");
        Assertions.assertNotNull(registry.getClient(reference));
      }
    } finally {
      Thread.currentThread().setContextClassLoader(originalClassLoader);
    }
  }

  @Test
  void testRejectsNullClientAndClosesPreviouslyCreatedClient() {
    CloseTrackingFactory awsFactory =
        new CloseTrackingFactory(AWS_API, "aws", new ArrayList<>(), null);
    Config awsConfig =
        config(
            "gravitino.kms.providers", "primary,analytics",
            "gravitino.kms.provider.primary.api", "aws-kms",
            "gravitino.kms.provider.analytics.api", "google-cloud-kms");

    KmsClientFactory nullClientFactory = factory(GCP_API, (source, properties) -> null);

    Assertions.assertThrows(
        IllegalStateException.class,
        () -> new KmsClientRegistry(awsConfig, List.of(awsFactory, nullClientFactory)));
    Assertions.assertEquals(1, awsFactory.closeCount.get());
  }

  @Test
  void testClosesClientsInReverseOrderAndIsIdempotent() {
    List<String> closeOrder = new ArrayList<>();
    CloseTrackingFactory awsFactory = new CloseTrackingFactory(AWS_API, "aws", closeOrder, null);
    CloseTrackingFactory gcpFactory = new CloseTrackingFactory(GCP_API, "gcp", closeOrder, null);
    KmsClientRegistry registry =
        new KmsClientRegistry(
            config(
                "gravitino.kms.providers", "primary,analytics",
                "gravitino.kms.provider.primary.api", "aws-kms",
                "gravitino.kms.provider.analytics.api", "google-cloud-kms"),
            List.of(awsFactory, gcpFactory));
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
    CloseTrackingFactory awsFactory = new CloseTrackingFactory(AWS_API, "aws", closeOrder, null);
    KmsClientFactory failingFactory =
        factory(
            GCP_API,
            (source, properties) -> {
              throw new IllegalArgumentException("invalid GCP configuration");
            });

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            new KmsClientRegistry(
                config(
                    "gravitino.kms.providers", "primary,analytics",
                    "gravitino.kms.provider.primary.api", "aws-kms",
                    "gravitino.kms.provider.analytics.api", "google-cloud-kms"),
                List.of(awsFactory, failingFactory)));

    Assertions.assertEquals(1, awsFactory.closeCount.get());
    Assertions.assertEquals(List.of("aws"), closeOrder);
  }

  @Test
  void testPreservesInitializationFailureWhenCleanupFails() {
    RuntimeException closeFailure = new IllegalStateException("close failed");
    CloseTrackingFactory awsFactory =
        new CloseTrackingFactory(AWS_API, "aws", new ArrayList<>(), closeFailure);
    IllegalArgumentException creationFailure =
        new IllegalArgumentException("invalid GCP configuration");
    KmsClientFactory failingFactory =
        factory(
            GCP_API,
            (source, properties) -> {
              throw creationFailure;
            });

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                new KmsClientRegistry(
                    config(
                        "gravitino.kms.providers", "primary,analytics",
                        "gravitino.kms.provider.primary.api", "aws-kms",
                        "gravitino.kms.provider.analytics.api", "google-cloud-kms"),
                    List.of(awsFactory, failingFactory)));

    Assertions.assertSame(creationFailure, exception);
    Assertions.assertArrayEquals(new Throwable[] {closeFailure}, exception.getSuppressed());
  }

  @Test
  void testAggregatesCloseFailures() {
    RuntimeException awsFailure = new IllegalStateException("aws close failed");
    RuntimeException gcpFailure = new IllegalStateException("gcp close failed");
    CloseTrackingFactory awsFactory =
        new CloseTrackingFactory(AWS_API, "aws", new ArrayList<>(), awsFailure);
    CloseTrackingFactory gcpFactory =
        new CloseTrackingFactory(GCP_API, "gcp", new ArrayList<>(), gcpFailure);
    KmsClientRegistry registry =
        new KmsClientRegistry(
            config(
                "gravitino.kms.providers", "primary,analytics",
                "gravitino.kms.provider.primary.api", "aws-kms",
                "gravitino.kms.provider.analytics.api", "google-cloud-kms"),
            List.of(awsFactory, gcpFactory));

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

  private static KmsClientFactory factory(String api, ClientCreator creator) {
    return new KmsClientFactory() {
      @Override
      public String api() {
        return api;
      }

      @Override
      public KmsClient create(String provider, Map<String, String> properties) {
        return creator.create(provider, properties);
      }
    };
  }

  private interface ClientCreator {
    KmsClient create(String provider, Map<String, String> properties);
  }

  private static final class RecordingFactory implements KmsClientFactory {
    private final String api;
    private String createdProvider;
    private Map<String, String> properties;
    private final AtomicInteger createCount = new AtomicInteger();

    private RecordingFactory(String api) {
      this.api = api;
    }

    @Override
    public String api() {
      return api;
    }

    @Override
    public KmsClient create(String provider, Map<String, String> properties) {
      createCount.incrementAndGet();
      this.createdProvider = provider;
      this.properties = properties;
      return reference -> Optional.of(new Properties(reference));
    }
  }

  private static final class CloseTrackingFactory implements KmsClientFactory {
    private final String api;
    private final String name;
    private final List<String> closeOrder;
    private final RuntimeException closeFailure;
    private final AtomicInteger closeCount = new AtomicInteger();

    private CloseTrackingFactory(
        String api, String name, List<String> closeOrder, RuntimeException closeFailure) {
      this.api = api;
      this.name = name;
      this.closeOrder = closeOrder;
      this.closeFailure = closeFailure;
    }

    @Override
    public String api() {
      return api;
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

  /** Factory exposed for the context-classloader ServiceLoader test. */
  public static final class ServiceLoadedFactory implements KmsClientFactory {

    /** Creates a test service-loaded factory. */
    public ServiceLoadedFactory() {}

    /** {@inheritDoc} */
    @Override
    public String api() {
      return AWS_API;
    }

    /** {@inheritDoc} */
    @Override
    public KmsClient create(String provider, Map<String, String> properties) {
      return reference -> Optional.of(new Properties(reference));
    }
  }
}
