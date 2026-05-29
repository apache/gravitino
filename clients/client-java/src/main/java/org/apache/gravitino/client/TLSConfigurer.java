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

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import org.apache.hc.client5.http.ssl.HttpsSupport;
import org.apache.hc.core5.ssl.SSLContexts;

/** Configures TLS settings for the HTTP client. */
public interface TLSConfigurer {

  /**
   * Returns the SSL context used for TLS connections.
   *
   * @return SSL context
   */
  default SSLContext sslContext() {
    return SSLContexts.createDefault();
  }

  /**
   * Returns the hostname verifier used for TLS connections.
   *
   * @return hostname verifier
   */
  default HostnameVerifier hostnameVerifier() {
    return HttpsSupport.getDefaultHostnameVerifier();
  }

  /**
   * Returns the supported TLS protocols.
   *
   * @return supported TLS protocols
   */
  default String[] supportedProtocols() {
    return null;
  }

  /**
   * Returns the supported cipher suites.
   *
   * @return supported cipher suites
   */
  default String[] supportedCipherSuites() {
    return null;
  }
}
