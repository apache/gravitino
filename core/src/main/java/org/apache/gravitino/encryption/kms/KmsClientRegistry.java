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
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.gravitino.Config;

/** Creates server-private KMS clients and dispatches key inspection by configured source. */
public final class KmsClientRegistry implements KmsClient {

  private final ReadWriteLock lifecycleLock = new ReentrantReadWriteLock();
  private final Map<String, ConfiguredClient> clients;
  private boolean closed;

  /**
   * Loads available {@link KmsClientFactory} implementations and creates configured clients.
   *
   * @param config Gravitino server configuration
   * @throws IllegalArgumentException if configuration or factory discovery is invalid
   */
  public KmsClientRegistry(Config config) {
    this(config, loadFactories());
  }

  KmsClientRegistry(Config config, Iterable<KmsClientFactory> factories) {
    if (factories == null) {
      throw new IllegalArgumentException("KMS client factories cannot be null");
    }

    KmsConfig kmsConfig = new KmsConfig(config);
    Map<KmsApi, KmsClientFactory> factoriesByApi = indexFactories(factories);
    this.clients = createClients(kmsConfig.sources(), factoriesByApi);
  }

  /**
   * Dispatches the request to the client configured for the reference source.
   *
   * @param reference key to inspect
   * @return normalized key properties, or empty if the provider authoritatively reports that the
   *     key does not exist
   * @throws IllegalArgumentException if the source is unknown or configured for another API
   * @throws IllegalStateException if the registry is closed or a provider violates its contract
   */
  @Override
  public Optional<KmsKeyProperties> getKeyProperties(KmsReference reference) {
    Lock readLock = lifecycleLock.readLock();
    readLock.lock();
    try {
      if (closed) {
        throw new IllegalStateException("KMS client registry is closed");
      }
      if (reference == null) {
        throw new IllegalArgumentException("KMS reference cannot be null");
      }

      ConfiguredClient configuredClient = clients.get(reference.source());
      if (configuredClient == null) {
        throw new IllegalArgumentException(
            String.format("Unknown KMS source '%s'", reference.source()));
      }
      if (configuredClient.api != reference.api()) {
        throw new IllegalArgumentException(
            String.format(
                "KMS source '%s' uses API '%s', not '%s'",
                reference.source(), configuredClient.api.wireValue(), reference.api().wireValue()));
      }

      Optional<KmsKeyProperties> properties = configuredClient.client.getKeyProperties(reference);
      if (properties == null) {
        throw new IllegalStateException(
            String.format("KMS client for source '%s' returned null", reference.source()));
      }
      if (properties.isPresent() && !reference.equals(properties.get().reference())) {
        throw new IllegalStateException(
            String.format(
                "KMS client for source '%s' returned properties for a different reference",
                reference.source()));
      }
      return properties;
    } finally {
      readLock.unlock();
    }
  }

  /** Closes all configured clients. This operation is idempotent. */
  @Override
  public void close() {
    Lock writeLock = lifecycleLock.writeLock();
    writeLock.lock();
    try {
      if (closed) {
        return;
      }
      closed = true;
      RuntimeException failure = closeClients(new ArrayList<>(clients.values()));
      if (failure != null) {
        throw failure;
      }
    } finally {
      writeLock.unlock();
    }
  }

  private static Map<KmsApi, KmsClientFactory> indexFactories(
      Iterable<KmsClientFactory> factories) {
    Map<KmsApi, KmsClientFactory> factoriesByApi = new LinkedHashMap<>();
    for (KmsClientFactory factory : factories) {
      if (factory == null) {
        throw new IllegalArgumentException("KMS client factory cannot be null");
      }
      KmsApi api = factory.api();
      if (api == null) {
        throw new IllegalArgumentException("KMS client factory API cannot be null");
      }
      KmsClientFactory existing = factoriesByApi.putIfAbsent(api, factory);
      if (existing != null) {
        throw new IllegalArgumentException(
            String.format("Multiple KMS client factories support API '%s'", api.wireValue()));
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
      Map<KmsApi, KmsClientFactory> factoriesByApi) {
    Map<String, ConfiguredClient> clients = new LinkedHashMap<>();
    try {
      sourceConfigs.forEach(
          (source, sourceConfig) -> {
            KmsClientFactory factory = factoriesByApi.get(sourceConfig.api());
            if (factory == null) {
              throw new IllegalArgumentException(
                  String.format(
                      "No KMS client factory supports API '%s' for source '%s'",
                      sourceConfig.api().wireValue(), source));
            }
            KmsClient client = factory.create(source, sourceConfig.properties());
            if (client == null) {
              throw new IllegalStateException(
                  String.format(
                      "KMS client factory for API '%s' returned null",
                      sourceConfig.api().wireValue()));
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

  private static final class ConfiguredClient {
    private final KmsApi api;
    private final KmsClient client;

    private ConfiguredClient(KmsApi api, KmsClient client) {
      this.api = api;
      this.client = client;
    }
  }
}
