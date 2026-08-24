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
import org.apache.gravitino.Config;

/** Creates, resolves, and owns server-private KMS clients by configured provider. */
public final class KmsClientRegistry implements AutoCloseable {

  private final Map<String, KmsClient> clients;
  private volatile boolean closed;

  /**
   * Loads configuration and creates one client for each configured provider by instantiating {@code
   * gravitino.kms.provider.<name>.className}.
   *
   * @param config Gravitino server configuration
   * @throws IllegalArgumentException if configuration or factory construction is invalid
   */
  public KmsClientRegistry(Config config) {
    this(config, KmsClientRegistry::loadFactory);
  }

  KmsClientRegistry(Config config, FactoryLoader loader) {
    KmsConfig kmsConfig = new KmsConfig(config);
    if (kmsConfig.providers().isEmpty()) {
      this.clients = Collections.emptyMap();
      return;
    }

    if (loader == null) {
      throw new IllegalArgumentException("KMS client factory loader cannot be null");
    }

    this.clients = createClients(kmsConfig.providers(), loader);
  }

  /**
   * Resolves the client configured for a key reference.
   *
   * <p>The registry owns the returned client. Callers must not close it or use it after the
   * registry is closed. Lookup is by {@link KmsReference#provider()} only; the provider's factory
   * was loaded at startup from {@code gravitino.kms.provider.<name>.className}.
   *
   * @param reference key whose provider name selects the client
   * @return client configured for the reference
   * @throws IllegalArgumentException if the provider is unknown
   * @throws IllegalStateException if the registry is closed
   */
  public KmsClient getClient(KmsReference reference) {
    checkOpen();
    return resolveClient(reference);
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

  /** Loads a {@link KmsClientFactory} from a configured class name. */
  @FunctionalInterface
  interface FactoryLoader {
    /**
     * Instantiates the factory named by {@code className}.
     *
     * @param className factory class name
     * @return the factory
     */
    KmsClientFactory load(String className);
  }

  private static KmsClientFactory loadFactory(String className) {
    try {
      Object instance = Class.forName(className).getDeclaredConstructor().newInstance();
      if (!(instance instanceof KmsClientFactory)) {
        throw new IllegalArgumentException(
            String.format("KMS factory class '%s' does not implement KmsClientFactory", className));
      }
      return (KmsClientFactory) instance;
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(
          String.format("No KMS client factory class '%s'", className), e);
    } catch (ReflectiveOperationException e) {
      throw new IllegalArgumentException(
          String.format("Failed to create KMS client factory '%s'", className), e);
    }
  }

  private static Map<String, KmsClient> createClients(
      Map<String, KmsConfig.ProviderConfig> providerConfigs, FactoryLoader loader) {
    Map<String, KmsClient> clients = new LinkedHashMap<>();
    try {
      providerConfigs.forEach(
          (provider, providerConfig) -> {
            KmsClientFactory factory = loader.load(providerConfig.className());
            if (factory == null) {
              throw new IllegalStateException(
                  String.format(
                      "KMS client factory '%s' returned null", providerConfig.className()));
            }
            KmsClient client = factory.create(provider, providerConfig.properties());
            if (client == null) {
              throw new IllegalStateException(
                  String.format(
                      "KMS client factory '%s' returned a null client",
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

  private static RuntimeException closeClients(List<KmsClient> clients) {
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

  private KmsClient resolveClient(KmsReference reference) {
    if (reference == null) {
      throw new IllegalArgumentException("KMS reference cannot be null");
    }

    KmsClient client = clients.get(reference.provider());
    if (client == null) {
      throw new IllegalArgumentException(
          String.format("No KMS client is configured for provider '%s'", reference.provider()));
    }
    return client;
  }

  private void checkOpen() {
    if (closed) {
      throw new IllegalStateException("KMS client registry is closed");
    }
  }
}
