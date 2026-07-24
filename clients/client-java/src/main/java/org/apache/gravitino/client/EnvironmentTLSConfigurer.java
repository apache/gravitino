/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.client;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import org.apache.commons.lang3.StringUtils;

final class EnvironmentTLSConfigurer implements TLSConfigurer {

  static final String KEY_STORE_PATH = "GRAVITINO_CLIENT_TLS_KEY_STORE_PATH";
  static final String KEY_STORE_PASSWORD = "GRAVITINO_CLIENT_TLS_KEY_STORE_PASSWORD";
  static final String KEY_STORE_TYPE = "GRAVITINO_CLIENT_TLS_KEY_STORE_TYPE";
  static final String TRUST_STORE_PATH = "GRAVITINO_CLIENT_TLS_TRUST_STORE_PATH";
  static final String TRUST_STORE_PASSWORD = "GRAVITINO_CLIENT_TLS_TRUST_STORE_PASSWORD";
  static final String TRUST_STORE_TYPE = "GRAVITINO_CLIENT_TLS_TRUST_STORE_TYPE";

  private static final String DEFAULT_STORE_TYPE = "PKCS12";

  private final SSLContext sslContext;

  private EnvironmentTLSConfigurer(SSLContext sslContext) {
    this.sslContext = sslContext;
  }

  static Optional<TLSConfigurer> fromEnvironment(Map<String, String> environment) {
    boolean hasKeyStoreConfig =
        hasAnyValue(environment, KEY_STORE_PATH, KEY_STORE_PASSWORD, KEY_STORE_TYPE);
    boolean hasTrustStoreConfig =
        hasAnyValue(environment, TRUST_STORE_PATH, TRUST_STORE_PASSWORD, TRUST_STORE_TYPE);

    if (!hasKeyStoreConfig && !hasTrustStoreConfig) {
      return Optional.empty();
    }

    @Nullable
    KeyManager[] keyManagers =
        hasKeyStoreConfig
            ? createKeyManagers(
                requiredValue(environment, KEY_STORE_PATH),
                requiredValue(environment, KEY_STORE_PASSWORD),
                storeType(environment, KEY_STORE_TYPE))
            : null;
    @Nullable
    TrustManager[] trustManagers =
        hasTrustStoreConfig
            ? createTrustManagers(
                requiredValue(environment, TRUST_STORE_PATH),
                requiredValue(environment, TRUST_STORE_PASSWORD),
                storeType(environment, TRUST_STORE_TYPE))
            : null;

    try {
      SSLContext sslContext = SSLContext.getInstance("TLS");
      sslContext.init(keyManagers, trustManagers, null);
      return Optional.of(new EnvironmentTLSConfigurer(sslContext));
    } catch (GeneralSecurityException e) {
      throw new IllegalArgumentException("Failed to initialize the client TLS context", e);
    }
  }

  @Override
  public SSLContext sslContext() {
    return sslContext;
  }

  private static boolean hasAnyValue(Map<String, String> environment, String... keys) {
    for (String key : keys) {
      if (StringUtils.isNotBlank(environment.get(key))) {
        return true;
      }
    }
    return false;
  }

  private static String requiredValue(Map<String, String> environment, String key) {
    String value = environment.get(key);
    if (StringUtils.isBlank(value)) {
      throw new IllegalArgumentException(
          String.format("Environment variable %s is required for client TLS", key));
    }
    return value;
  }

  private static String storeType(Map<String, String> environment, String key) {
    return StringUtils.defaultIfBlank(environment.get(key), DEFAULT_STORE_TYPE);
  }

  private static KeyManager[] createKeyManagers(String path, String password, String type) {
    try {
      KeyStore keyStore = loadStore(Paths.get(path), password, type, "key store");
      KeyManagerFactory factory =
          KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      factory.init(keyStore, password.toCharArray());
      return factory.getKeyManagers();
    } catch (GeneralSecurityException e) {
      throw new IllegalArgumentException(
          String.format("Failed to initialize the client TLS key store at %s", path), e);
    }
  }

  private static TrustManager[] createTrustManagers(String path, String password, String type) {
    try {
      KeyStore trustStore = loadStore(Paths.get(path), password, type, "trust store");
      TrustManagerFactory factory =
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      factory.init(trustStore);
      return factory.getTrustManagers();
    } catch (GeneralSecurityException e) {
      throw new IllegalArgumentException(
          String.format("Failed to initialize the client TLS trust store at %s", path), e);
    }
  }

  private static KeyStore loadStore(Path path, String password, String type, String description) {
    try {
      KeyStore store = KeyStore.getInstance(type);
      try (InputStream input = Files.newInputStream(path)) {
        store.load(input, password.toCharArray());
      }
      return store;
    } catch (GeneralSecurityException | IOException e) {
      throw new IllegalArgumentException(
          String.format("Failed to load the client TLS %s at %s", description, path), e);
    }
  }
}
