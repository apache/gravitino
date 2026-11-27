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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Config;

/**
 * Creates, resolves, and owns server-private seal clients by configured provider.
 *
 * <p>Configuration follows the established instance style:
 *
 * <pre>{@code
 * gravitino.seal.providers=local
 * gravitino.seal.provider.local.className=org.apache.gravitino.encryption.seal.local.LocalSealClientFactory
 * gravitino.seal.provider.local.keyFile=/path/to/key.b64
 * }</pre>
 *
 * <p>When no provider is configured the registry is empty and no factory is loaded, so behavior is
 * unchanged for servers that do not use a seal.
 */
public final class SealClientRegistry implements AutoCloseable {

  private final Map<String, SealClient> clients;
  private volatile boolean closed;

  /**
   * Loads configuration and creates one client for each configured provider by instantiating {@code
   * gravitino.seal.provider.<name>.className}.
   *
   * @param config Gravitino server configuration
   * @throws IllegalArgumentException if configuration or factory construction is invalid
   * @throws IllegalStateException if a factory returns null or produces a null client
   */
  public SealClientRegistry(Config config) {
    this(config, SealClientRegistry::loadFactory);
  }

  SealClientRegistry(Config config, FactoryLoader loader) {
    SealConfig sealConfig = new SealConfig(config);
    if (sealConfig.providers().isEmpty()) {
      this.clients = Collections.emptyMap();
      return;
    }

    if (loader == null) {
      throw new IllegalArgumentException("Seal client factory loader cannot be null");
    }

    this.clients = createClients(sealConfig.providers(), loader);
  }

  /**
   * Resolves the client configured for a provider name.
   *
   * <p>The registry owns the returned client. Callers must not close it or use it after the
   * registry is closed. Lookup is by provider name only; the provider's factory was loaded at
   * startup from {@code gravitino.seal.provider.<name>.className}.
   *
   * @param provider configured provider name selecting the client
   * @return client configured for the provider
   * @throws IllegalArgumentException if the provider is null, blank, or unknown
   * @throws IllegalStateException if the registry is closed
   */
  public SealClient getClient(String provider) {
    checkOpen();
    return resolveClient(provider);
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

  /** Loads a {@link SealClientFactory} from a configured class name. */
  @FunctionalInterface
  interface FactoryLoader {
    /**
     * Instantiates the factory named by {@code className}.
     *
     * @param className factory class name
     * @return the factory
     */
    SealClientFactory load(String className);
  }

  private static SealClientFactory loadFactory(String className) {
    try {
      Object instance = Class.forName(className).getDeclaredConstructor().newInstance();
      if (!(instance instanceof SealClientFactory)) {
        throw new IllegalArgumentException(
            String.format(
                "Seal factory class '%s' does not implement SealClientFactory", className));
      }
      return (SealClientFactory) instance;
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(
          String.format("No seal client factory class '%s'", className), e);
    } catch (ReflectiveOperationException e) {
      throw new IllegalArgumentException(
          String.format("Failed to create seal client factory '%s'", className), e);
    }
  }

  private static Map<String, SealClient> createClients(
      Map<String, SealConfig.ProviderConfig> providerConfigs, FactoryLoader loader) {
    Map<String, SealClient> clients = new LinkedHashMap<>();
    try {
      providerConfigs.forEach(
          (provider, providerConfig) -> {
            SealClientFactory factory = loader.load(providerConfig.className());
            if (factory == null) {
              throw new IllegalStateException(
                  String.format(
                      "Seal client factory '%s' returned null", providerConfig.className()));
            }
            SealClient client = factory.create(provider, providerConfig.properties());
            if (client == null) {
              throw new IllegalStateException(
                  String.format(
                      "Seal client factory '%s' returned a null client",
                      providerConfig.className()));
            }
            clients.put(provider, client);
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

  private static RuntimeException closeClients(List<SealClient> clients) {
    RuntimeException failure = null;
    for (int index = clients.size() - 1; index >= 0; index--) {
      try {
        clients.get(index).close();
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

  private SealClient resolveClient(String provider) {
    if (provider == null || provider.trim().isEmpty()) {
      throw new IllegalArgumentException("Seal provider name cannot be null or blank");
    }

    SealClient client = clients.get(provider);
    if (client == null) {
      throw new IllegalArgumentException(
          String.format("No seal client is configured for provider '%s'", provider));
    }
    return client;
  }

  private void checkOpen() {
    if (closed) {
      throw new IllegalStateException("Seal client registry is closed");
    }
  }
}
