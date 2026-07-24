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

import javax.annotation.Nullable;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import org.apache.hc.client5.http.ssl.HttpsSupport;
import org.apache.hc.core5.ssl.SSLContexts;

/**
 * Configures TLS settings for connections created by the Gravitino client.
 *
 * <p>An explicit configurer takes precedence over environment configuration. Without one, the
 * client reads the following environment variables:
 *
 * <ul>
 *   <li>{@code GRAVITINO_CLIENT_TLS_KEY_STORE_PATH}, {@code
 *       GRAVITINO_CLIENT_TLS_KEY_STORE_PASSWORD}, and optional {@code
 *       GRAVITINO_CLIENT_TLS_KEY_STORE_TYPE}
 *   <li>{@code GRAVITINO_CLIENT_TLS_TRUST_STORE_PATH}, {@code
 *       GRAVITINO_CLIENT_TLS_TRUST_STORE_PASSWORD}, and optional {@code
 *       GRAVITINO_CLIENT_TLS_TRUST_STORE_TYPE}
 * </ul>
 *
 * <p>Store types default to {@code PKCS12}. There is no default file location. When no explicit or
 * environment configuration is present, the JVM TLS defaults are used.
 */
public interface TLSConfigurer {

  /**
   * Returns the SSL context used for TLS connections.
   *
   * @return the configured SSL context
   */
  default SSLContext sslContext() {
    return SSLContexts.createDefault();
  }

  /**
   * Returns the hostname verifier used for TLS connections.
   *
   * @return the hostname verifier
   */
  default HostnameVerifier hostnameVerifier() {
    return HttpsSupport.getDefaultHostnameVerifier();
  }

  /**
   * Returns the supported TLS protocols, or null to use the client defaults.
   *
   * @return the supported TLS protocols
   */
  @Nullable
  default String[] supportedProtocols() {
    return null;
  }

  /**
   * Returns the supported cipher suites, or null to use the client defaults.
   *
   * @return the supported cipher suites
   */
  @Nullable
  default String[] supportedCipherSuites() {
    return null;
  }
}
