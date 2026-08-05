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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.gravitino.Config;

/** Creates, resolves, and owns server-private KMS clients by configured source. */
public final class KmsClientRegistry implements AutoCloseable {

  private final Map<String, ConfiguredClient> clients;
  private volatile boolean closed;

  /**
   * Loads configuration and available {@link KmsClientFactory} implementations, then creates one
   * client for each configured source.
   *
   * @param config Gravitino server configuration
   * @throws IllegalArgumentException if configuration or factory discovery is invalid
   */
  public KmsClientRegistry(Config config) {
    this(config, loadFactories());
  }

  KmsClientRegistry(Config config, Iterable<KmsClientFactory> factories) {
    KmsConfig kmsConfig = new KmsConfig(config);
    if (kmsConfig.sources().isEmpty()) {
      this.clients = Collections.emptyMap();
      return;
    }

    if (factories == null) {
      throw new IllegalArgumentException("KMS client factories cannot be null");
    }

    Map<String, KmsClientFactory> factoriesByApi = indexFactories(factories);
    this.clients = createClients(kmsConfig.sources(), factoriesByApi);
  }

  /**
   * Resolves the client configured for a key reference.
   *
   * <p>The registry owns the returned client. Callers must not close it or use it after the
   * registry is closed.
   *
   * @param reference key whose source and API select the client
   * @return client configured for the reference
   * @throws IllegalArgumentException if the source is unknown or configured for another API
   * @throws IllegalStateException if the registry is closed
   */
  public KmsClient getClient(KmsReference reference) {
    checkOpen();
    return resolveClient(reference).client;
  }

  /** Closes all configured clients. This operation is idempotent. */
  @Override
  public synchronized void close() {
    if (closed) {
      return;
    }
    closed = true;
    RuntimeException failure = closeClients(new ArrayList<>(clients.values()));
    if (failure != null) {
      throw failure;
    }
  }

  private static Map<String, KmsClientFactory> indexFactories(
      Iterable<KmsClientFactory> factories) {
    Map<String, KmsClientFactory> factoriesByApi = new LinkedHashMap<>();
    for (KmsClientFactory factory : factories) {
      if (factory == null) {
        throw new IllegalArgumentException("KMS client factory cannot be null");
      }
      String api = KmsApiIdentifiers.requireValid(factory.api());
      KmsClientFactory existing = factoriesByApi.putIfAbsent(api, factory);
      if (existing != null) {
        throw new IllegalArgumentException(
            String.format("Multiple KMS client factories support API '%s'", api));
      }
    }
    return factoriesByApi;
  }

  private static Iterable<KmsClientFactory> loadFactories() {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    if (classLoader == null) {
      classLoader = KmsClientRegistry.class.getClassLoader();
    }
    return ServiceLoader.load(KmsClientFactory.class, classLoader);
  }

  private static Map<String, ConfiguredClient> createClients(
      Map<String, KmsConfig.SourceConfig> sourceConfigs,
      Map<String, KmsClientFactory> factoriesByApi) {
    Map<String, ConfiguredClient> clients = new LinkedHashMap<>();
    try {
      sourceConfigs.forEach(
          (source, sourceConfig) -> {
            KmsClientFactory factory = factoriesByApi.get(sourceConfig.api());
            if (factory == null) {
              throw new IllegalArgumentException(
                  String.format(
                      "No KMS client factory supports API '%s' for source '%s'",
                      sourceConfig.api(), source));
            }
            KmsClient client = factory.create(source, sourceConfig.properties());
            if (client == null) {
              throw new IllegalStateException(
                  String.format(
                      "KMS client factory for API '%s' returned null", sourceConfig.api()));
            }
            clients.put(source, new ConfiguredClient(sourceConfig.api(), client));
          });
      return Collections.unmodifiableMap(clients);
    } catch (RuntimeException | Error e) {
      RuntimeException closeFailure = closeClients(new ArrayList<>(clients.values()));
      if (closeFailure != null) {
        e.addSuppressed(closeFailure);
      }
      throw e;
    }
  }

  private static RuntimeException closeClients(List<ConfiguredClient> clients) {
    RuntimeException failure = null;
    for (int index = clients.size() - 1; index >= 0; index--) {
      try {
        clients.get(index).client.close();
      } catch (RuntimeException e) {
        if (failure == null) {
          failure = e;
        } else {
          failure.addSuppressed(e);
        }
      }
    }
    return failure;
  }

  private ConfiguredClient resolveClient(KmsReference reference) {
    if (reference == null) {
      throw new IllegalArgumentException("KMS reference cannot be null");
    }

    ConfiguredClient configuredClient = clients.get(reference.source());
    if (configuredClient == null) {
      throw new IllegalArgumentException(
          String.format("No KMS client is configured for source '%s'", reference.source()));
    }
    if (!configuredClient.api.equals(reference.api())) {
      throw new IllegalArgumentException(
          String.format(
              "KMS source '%s' uses API '%s', not '%s'",
              reference.source(), configuredClient.api, reference.api()));
    }
    return configuredClient;
  }

  private void checkOpen() {
    if (closed) {
      throw new IllegalStateException("KMS client registry is closed");
    }
  }

  private static final class ConfiguredClient {
    private final String api;
    private final KmsClient client;

    private ConfiguredClient(String api, KmsClient client) {
      this.api = api;
      this.client = client;
    }
  }
}
