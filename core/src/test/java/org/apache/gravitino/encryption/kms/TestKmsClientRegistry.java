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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.gravitino.Config;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestKmsClientRegistry {

  private static final KmsApi AWS_API = KmsApi.AWS_KMS;
  private static final KmsApi GCP_API = KmsApi.GOOGLE_CLOUD_KMS;
  private static final KmsApi AZURE_API = KmsApi.AZURE_KEY_VAULT;

  @Test
  void testCreatesAndDispatchesConfiguredClients() {
    RecordingFactory awsFactory = new RecordingFactory(AWS_API);
    RecordingFactory gcpFactory = new RecordingFactory(GCP_API);
    KmsClientRegistry registry =
        new KmsClientRegistry(
            config(
                "gravitino.kms.sources", "primary,analytics",
                "gravitino.kms.source.primary.api", " AWS-KMS ",
                "gravitino.kms.source.primary.endpoint.region", "us-west-2",
                "gravitino.kms.source.analytics.api", "google-cloud-kms",
                "gravitino.kms.source.analytics.endpoint.project", "data-project"),
            List.of(awsFactory, gcpFactory));

    KmsReference awsReference = new KmsReference(AWS_API, "primary", "alias/orders");
    KmsReference gcpReference =
        new KmsReference(GCP_API, "analytics", "projects/p/locations/l/keyRings/r/cryptoKeys/k");

    Assertions.assertEquals(
        awsReference, registry.getKeyProperties(awsReference).orElseThrow().reference());
    Assertions.assertEquals(
        awsReference, registry.getKeyProperties(awsReference).orElseThrow().reference());
    Assertions.assertEquals(
        gcpReference, registry.getKeyProperties(gcpReference).orElseThrow().reference());
    Assertions.assertEquals(Map.of("endpoint.region", "us-west-2"), awsFactory.properties);
    Assertions.assertEquals(Map.of("endpoint.project", "data-project"), gcpFactory.properties);
    Assertions.assertEquals("alias/orders", awsFactory.providerKeyId);
    Assertions.assertEquals(
        "projects/p/locations/l/keyRings/r/cryptoKeys/k", gcpFactory.providerKeyId);
    Assertions.assertEquals("primary", awsFactory.source);
    Assertions.assertEquals("analytics", gcpFactory.source);
    Assertions.assertEquals(1, awsFactory.createCount.get());
    Assertions.assertEquals(1, gcpFactory.createCount.get());
  }

  @Test
  void testRejectsUnknownSourceAndApiMismatch() {
    KmsClientRegistry registry =
        new KmsClientRegistry(
            config(
                "gravitino.kms.sources", "primary",
                "gravitino.kms.source.primary.api", "aws-kms"),
            List.of(new RecordingFactory(AWS_API)));

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> registry.getKeyProperties(new KmsReference(AWS_API, "other", "key")));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> registry.getKeyProperties(new KmsReference(GCP_API, "primary", "key")));
    Assertions.assertThrows(IllegalArgumentException.class, () -> registry.getKeyProperties(null));
  }

  @Test
  void testCreatesMultipleSourcesForSameApi() {
    RecordingFactory factory = new RecordingFactory(AZURE_API);
    KmsClientRegistry registry =
        new KmsClientRegistry(
            config(
                "gravitino.kms.sources", "azure-eu,azure-us",
                "gravitino.kms.source.azure-eu.api", "azure-key-vault",
                "gravitino.kms.source.azure-us.api", "azure-key-vault"),
            List.of(factory));

    Assertions.assertEquals(
        "azure-eu",
        registry
            .getKeyProperties(new KmsReference(AZURE_API, "azure-eu", "primary"))
            .orElseThrow()
            .reference()
            .source());
    Assertions.assertEquals(
        "azure-us",
        registry
            .getKeyProperties(new KmsReference(AZURE_API, "azure-us", "primary"))
            .orElseThrow()
            .reference()
            .source());
  }

  @Test
  void testRejectsMissingDuplicateAndInvalidFactories() {
    Config awsConfig =
        config(
            "gravitino.kms.sources", "primary",
            "gravitino.kms.source.primary.api", "aws-kms");

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
        () -> new KmsClientRegistry(awsConfig, java.util.Arrays.asList((KmsClientFactory) null)));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new KmsClientRegistry(awsConfig, null));
  }

  @Test
  void testRejectsUnknownConfiguredApi() {
    Config customConfig =
        config(
            "gravitino.kms.sources", "primary",
            "gravitino.kms.source.primary.api", "custom-kms");

    KmsConfigurationException exception =
        Assertions.assertThrows(
            KmsConfigurationException.class,
            () -> new KmsClientRegistry(customConfig, List.of(new RecordingFactory(AWS_API))));
    Assertions.assertTrue(exception.getMessage().contains("Unsupported KMS API 'custom-kms'"));
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
                  "gravitino.kms.sources", "primary",
                  "gravitino.kms.source.primary.api", "aws-kms"))) {
        KmsReference reference = new KmsReference(AWS_API, "primary", "key");
        Assertions.assertEquals(
            reference, registry.getKeyProperties(reference).orElseThrow().reference());
      }
    } finally {
      Thread.currentThread().setContextClassLoader(originalClassLoader);
    }
  }

  @Test
  void testRejectsInvalidProviderResults() {
    KmsReference reference = new KmsReference(AWS_API, "primary", "key");
    Config awsConfig =
        config(
            "gravitino.kms.sources", "primary",
            "gravitino.kms.source.primary.api", "aws-kms");

    KmsClientFactory nullResultFactory = factory(AWS_API, (source, properties) -> ignored -> null);
    KmsClientRegistry nullResultRegistry =
        new KmsClientRegistry(awsConfig, List.of(nullResultFactory));
    Assertions.assertThrows(
        IllegalStateException.class, () -> nullResultRegistry.getKeyProperties(reference));

    KmsReference otherReference = new KmsReference(AWS_API, "primary", "other");
    KmsClientFactory wrongReferenceFactory =
        factory(
            AWS_API,
            (source, properties) -> ignored -> Optional.of(new Properties(otherReference)));
    KmsClientRegistry wrongReferenceRegistry =
        new KmsClientRegistry(awsConfig, List.of(wrongReferenceFactory));
    Assertions.assertThrows(
        IllegalStateException.class, () -> wrongReferenceRegistry.getKeyProperties(reference));

    KmsClientFactory missingKeyFactory =
        factory(AWS_API, (source, properties) -> ignored -> Optional.empty());
    KmsClientRegistry missingKeyRegistry =
        new KmsClientRegistry(awsConfig, List.of(missingKeyFactory));
    Assertions.assertTrue(missingKeyRegistry.getKeyProperties(reference).isEmpty());

    KmsClientFactory nullClientFactory = factory(AWS_API, (source, properties) -> null);
    Assertions.assertThrows(
        IllegalStateException.class,
        () -> new KmsClientRegistry(awsConfig, List.of(nullClientFactory)));
  }

  @Test
  void testClosesClientsInReverseOrderAndIsIdempotent() {
    List<String> closeOrder = new ArrayList<>();
    CloseTrackingFactory awsFactory = new CloseTrackingFactory(AWS_API, "aws", closeOrder, null);
    CloseTrackingFactory gcpFactory = new CloseTrackingFactory(GCP_API, "gcp", closeOrder, null);
    KmsClientRegistry registry =
        new KmsClientRegistry(
            config(
                "gravitino.kms.sources", "primary,analytics",
                "gravitino.kms.source.primary.api", "aws-kms",
                "gravitino.kms.source.analytics.api", "google-cloud-kms"),
            List.of(awsFactory, gcpFactory));

    registry.close();
    registry.close();

    Assertions.assertEquals(List.of("gcp", "aws"), closeOrder);
    Assertions.assertEquals(1, awsFactory.closeCount.get());
    Assertions.assertEquals(1, gcpFactory.closeCount.get());
    Assertions.assertThrows(
        IllegalStateException.class,
        () -> registry.getKeyProperties(new KmsReference(AWS_API, "primary", "key")));
  }

  @Test
  void testClosesCreatedClientsAfterPartialInitializationFailure() {
    CloseTrackingFactory awsFactory =
        new CloseTrackingFactory(AWS_API, "aws", new ArrayList<>(), null);
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
                    "gravitino.kms.sources", "primary,analytics",
                    "gravitino.kms.source.primary.api", "aws-kms",
                    "gravitino.kms.source.analytics.api", "google-cloud-kms"),
                List.of(awsFactory, failingFactory)));
    Assertions.assertEquals(1, awsFactory.closeCount.get());
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
                        "gravitino.kms.sources", "primary,analytics",
                        "gravitino.kms.source.primary.api", "aws-kms",
                        "gravitino.kms.source.analytics.api", "google-cloud-kms"),
                    List.of(awsFactory, failingFactory)));

    Assertions.assertSame(creationFailure, exception);
    Assertions.assertArrayEquals(new Throwable[] {closeFailure}, exception.getSuppressed());
  }

  @Test
  void testDispatchesDifferentSourcesConcurrently() throws Exception {
    CountDownLatch entered = new CountDownLatch(2);
    CountDownLatch release = new CountDownLatch(1);
    KmsClientFactory awsFactory = blockingFactory(AWS_API, entered, release);
    KmsClientFactory gcpFactory = blockingFactory(GCP_API, entered, release);
    KmsClientRegistry registry =
        new KmsClientRegistry(
            config(
                "gravitino.kms.sources", "primary,analytics",
                "gravitino.kms.source.primary.api", "aws-kms",
                "gravitino.kms.source.analytics.api", "google-cloud-kms"),
            List.of(awsFactory, gcpFactory));
    ExecutorService executor = Executors.newFixedThreadPool(2);

    try {
      Future<Optional<KmsKeyProperties>> awsResult =
          executor.submit(
              () -> registry.getKeyProperties(new KmsReference(AWS_API, "primary", "key")));
      Future<Optional<KmsKeyProperties>> gcpResult =
          executor.submit(
              () -> registry.getKeyProperties(new KmsReference(GCP_API, "analytics", "key")));

      Assertions.assertTrue(entered.await(5, TimeUnit.SECONDS));
      release.countDown();
      Assertions.assertEquals(
          "primary", awsResult.get(5, TimeUnit.SECONDS).orElseThrow().reference().source());
      Assertions.assertEquals(
          "analytics", gcpResult.get(5, TimeUnit.SECONDS).orElseThrow().reference().source());
    } finally {
      release.countDown();
      executor.shutdownNow();
      registry.close();
    }
  }

  @Test
  void testAggregatesCloseFailures() {
    RuntimeException awsFailure = new IllegalStateException("aws close failed");
    RuntimeException gcpFailure = new IllegalStateException("gcp close failed");
    KmsClientRegistry registry =
        new KmsClientRegistry(
            config(
                "gravitino.kms.sources", "primary,analytics",
                "gravitino.kms.source.primary.api", "aws-kms",
                "gravitino.kms.source.analytics.api", "google-cloud-kms"),
            List.of(
                new CloseTrackingFactory(AWS_API, "aws", new ArrayList<>(), awsFailure),
                new CloseTrackingFactory(GCP_API, "gcp", new ArrayList<>(), gcpFailure)));

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

  private static KmsClientFactory factory(KmsApi api, ClientCreator creator) {
    return new KmsClientFactory() {
      @Override
      public KmsApi api() {
        return api;
      }

      @Override
      public KmsClient create(String source, Map<String, String> properties) {
        return creator.create(source, properties);
      }
    };
  }

  private static KmsClientFactory blockingFactory(
      KmsApi api, CountDownLatch entered, CountDownLatch release) {
    return factory(
        api,
        (source, properties) ->
            reference -> {
              entered.countDown();
              try {
                if (!release.await(5, TimeUnit.SECONDS)) {
                  throw new IllegalStateException("Timed out waiting to release KMS request");
                }
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while inspecting KMS key", e);
              }
              return Optional.of(new Properties(reference));
            });
  }

  private interface ClientCreator {
    KmsClient create(String source, Map<String, String> properties);
  }

  private static final class RecordingFactory implements KmsClientFactory {
    private final KmsApi api;
    private String source;
    private Map<String, String> properties;
    private String providerKeyId;
    private final AtomicInteger createCount = new AtomicInteger();

    private RecordingFactory(KmsApi api) {
      this.api = api;
    }

    @Override
    public KmsApi api() {
      return api;
    }

    @Override
    public KmsClient create(String source, Map<String, String> properties) {
      createCount.incrementAndGet();
      this.source = source;
      this.properties = properties;
      return reference -> {
        this.source = reference.source();
        this.providerKeyId = reference.keyId();
        return Optional.of(new Properties(reference));
      };
    }
  }

  private static final class CloseTrackingFactory implements KmsClientFactory {
    private final KmsApi api;
    private final String name;
    private final List<String> closeOrder;
    private final RuntimeException closeFailure;
    private final AtomicInteger closeCount = new AtomicInteger();

    private CloseTrackingFactory(
        KmsApi api, String name, List<String> closeOrder, RuntimeException closeFailure) {
      this.api = api;
      this.name = name;
      this.closeOrder = closeOrder;
      this.closeFailure = closeFailure;
    }

    @Override
    public KmsApi api() {
      return api;
    }

    @Override
    public KmsClient create(String source, Map<String, String> properties) {
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
    public KmsApi api() {
      return AWS_API;
    }

    /** {@inheritDoc} */
    @Override
    public KmsClient create(String source, Map<String, String> properties) {
      return reference -> Optional.of(new Properties(reference));
    }
  }
}
