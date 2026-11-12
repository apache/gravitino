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

  /**
   * Creates a TLS configurer using a custom truststore.
   *
   * @param path path to the truststore
   * @param password truststore password
   * @return the configured TLS configurer
   */
  public static TLSConfigurer withTrustStore(Path path, String password) {
    return builder().trustStore(path, password).build();
  }

  /**
   * Creates a TLS configurer using a default JVM truststore and a custom client keystore.
   *
   * @param path path to the client keystore
   * @param password keystore password
   * @return the configured TLS configurer
   */
  public static TLSConfigurer withKeyStore(Path path, String password) {
    return builder().keyStore(path, password).build();
  }

  /**
   * Creates a TLS configurer using a custom truststore and client keystore for mutual TLS
   * authentication.
   *
   * @param trustStorePath path to the truststore
   * @param trustPassword truststore password
   * @param keyStorePath path to the client keystore
   * @param keyPassword keystore password
   * @return the configured TLS configurer
   */
  public static TLSConfigurer withMutualTls(
      Path trustStorePath, String trustPassword, Path keyStorePath, String keyPassword) {
    return builder()
        .trustStore(trustStorePath, trustPassword)
        .keyStore(keyStorePath, keyPassword)
        .build();
  }

  /** Builder for creating a {@link TLSConfigurer} from truststore and keystore files. */
  public static final class Builder {
    private Path trustStorePath;
    private String trustStorePassword;
    private Path keyStorePath;
    private String keyStorePassword;
    private String storeType = DEFAULT_STORE_TYPE;

    private String[] protocols = new String[0];
    private String[] cipherSuites = new String[0];

    private Builder() {}

    /**
     * Configures the allowed TLS protocols.
     *
     * <p>If no protocols are specified, the default TLS protocols are used.
     *
     * @param protocols allowed TLS protocols, such as {@code TLSv1.3}
     * @return this builder
     */
    public Builder protocols(String... protocols) {
      Preconditions.checkArgument(protocols != null, "Protocols must not be null");

      for (String protocol : protocols) {
        Preconditions.checkArgument(
            protocol != null && !protocol.isEmpty(), "Protocol must not be null or empty");
      }

      this.protocols = protocols.clone();
      return this;
    }

    /**
     * Configures the allowed TLS cipher suites.
     *
     * <p>If no cipher suites are specified, the default TLS cipher suites are used.
     *
     * @param cipherSuites allowed TLS cipher suites
     * @return this builder
     */
    public Builder cipherSuites(String... cipherSuites) {
      Preconditions.checkArgument(cipherSuites != null, "Cipher suites must not be null");

      for (String cipherSuite : cipherSuites) {
        Preconditions.checkArgument(
            cipherSuite != null && !cipherSuite.isEmpty(),
            "Cipher suite must not be null or empty");
      }

      this.cipherSuites = cipherSuites.clone();
      return this;
    }

    /**
     * Configures the truststore used to verify the server certificate.
     *
     * @param path path to the truststore
     * @param password password for the truststore
     * @return this builder
     */
    public Builder trustStore(Path path, String password) {
      Preconditions.checkArgument(path != null, "Truststore path must not be null");
      Preconditions.checkArgument(password != null, "Truststore password must not be null");

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
      Preconditions.checkArgument(path != null, "Keystore path must not be null");
      Preconditions.checkArgument(password != null, "Keystore password must not be null");

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
      Preconditions.checkArgument(
          storeType != null && !storeType.isEmpty(), "Store type must not be null or empty");
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
      SSLContext sslContext = buildSslContext();
      String[] configuredProtocols = protocols.clone();
      String[] configuredCipherSuites = cipherSuites.clone();

      return new TLSConfigurer() {
        @Override
        public SSLContext sslContext() {
          return sslContext;
        }

        @Override
        public String[] supportedProtocols() {
          return configuredProtocols.clone();
        }

        @Override
        public String[] supportedCipherSuites() {
          return configuredCipherSuites.clone();
        }
      };
    }

    /**
     * Creates an SSL context from the configured or default truststore and optional client
     * keystore.
     *
     * @return the configured SSL context
     * @throws IllegalArgumentException if the SSL context cannot be created
     */
    private SSLContext buildSslContext() {
      try {
        TrustManagerFactory trustManagerFactory =
            TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());

        // Use the configured truststore, or fall back to the default JVM truststore.
        if (trustStorePath != null) {
          KeyStore trustStore = loadStore(trustStorePath, trustStorePassword, "truststore");
          trustManagerFactory.init(trustStore);
        } else {
          trustManagerFactory.init((KeyStore) null);
        }

        SSLContext sslContext = SSLContext.getInstance("TLS");

        // Configure client certificate authentication if a keystore is provided.
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
