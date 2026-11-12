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

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

/** Provides utility methods for creating {@link TLSConfigurer} instances. */
public final class TLSConfigurers {

  private static final String DEFAULT_STORE_TYPE = "PKCS12";

  private TLSConfigurers() {}

  /**
   * Returns a new builder for creating a {@link TLSConfigurer}.
   *
   * @return a new builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder for creating a {@link TLSConfigurer} from truststore and keystore files. */
  public static final class Builder {
    private Path trustStorePath;
    private String trustStorePassword;
    private Path keyStorePath;
    private String keyStorePassword;
    private String storeType = DEFAULT_STORE_TYPE;

    private Builder() {}

    /**
     * Configures the truststore used to verify the server certificate.
     *
     * @param path path to the truststore
     * @param password password for the truststore
     * @return this builder
     */
    public Builder trustStore(Path path, String password) {
      this.trustStorePath = path;
      this.trustStorePassword = password;
      return this;
    }

    /**
     * Configures the client keystore used for mutual TLS authentication.
     *
     * @param path path to the client keystore
     * @param password password for the client keystore
     * @return this builder
     */
    public Builder keyStore(Path path, String password) {
      this.keyStorePath = path;
      this.keyStorePassword = password;
      return this;
    }

    /**
     * Configures the store type used for both the truststore and client keystore.
     *
     * <p>The default store type is {@code PKCS12}.
     *
     * @param storeType keystore and truststore type
     * @return this builder
     */
    public Builder storeType(String storeType) {
      this.storeType = storeType;
      return this;
    }

    /**
     * Builds a {@link TLSConfigurer} using the configured truststore and optional client keystore.
     *
     * @return the configured TLS configurer
     * @throws IllegalArgumentException if required configuration is missing or the stores cannot be
     *     loaded
     */
    public TLSConfigurer build() {
      Preconditions.checkArgument(trustStorePath != null, "Truststore path must be provided");
      Preconditions.checkArgument(
          trustStorePassword != null, "Truststore password must be provided");
      Preconditions.checkArgument(storeType != null, "Store type must be provided");
      Preconditions.checkArgument(
          keyStorePath == null || keyStorePassword != null,
          "Keystore password must be provided when a keystore is configured");

      SSLContext sslContext = buildSslContext();

      return new TLSConfigurer() {
        @Override
        public SSLContext sslContext() {
          return sslContext;
        }
      };
    }

    /**
     * Creates an SSL context from the configured truststore and optional client keystore.
     *
     * @return the configured SSL context
     * @throws IllegalArgumentException if the SSL context cannot be created
     */
    private SSLContext buildSslContext() {
      try {
        KeyStore trustStore = loadStore(trustStorePath, trustStorePassword, "truststore");

        TrustManagerFactory trustManagerFactory =
            TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(trustStore);

        SSLContext sslContext = SSLContext.getInstance("TLS");

        if (keyStorePath == null) {
          sslContext.init(null, trustManagerFactory.getTrustManagers(), null);
          return sslContext;
        }

        KeyStore keyStore = loadStore(keyStorePath, keyStorePassword, "keystore");

        KeyManagerFactory keyManagerFactory =
            KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, keyStorePassword.toCharArray());

        sslContext.init(
            keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), null);

        return sslContext;
      } catch (GeneralSecurityException e) {
        throw new IllegalArgumentException(
            "Failed to configure TLS from the configured truststore or keystore", e);
      }
    }

    /**
     * Loads a keystore from the provided path.
     *
     * @param path path to the keystore
     * @param password password for the keystore
     * @param storeName name of the store for error messages
     * @return the loaded keystore
     */
    private KeyStore loadStore(Path path, String password, String storeName) {
      try {
        KeyStore keyStore = KeyStore.getInstance(storeType);

        try (InputStream inputStream = Files.newInputStream(path)) {
          keyStore.load(inputStream, password.toCharArray());
        }

        return keyStore;
      } catch (IOException | GeneralSecurityException e) {
        throw new IllegalArgumentException("Failed to load TLS " + storeName + " from " + path, e);
      }
    }
  }
}
