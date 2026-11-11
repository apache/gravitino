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

package org.apache.gravitino.client;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

/**
 * Authentication method used by the OAuth2 client-credentials flow.
 *
 * <p>Config values use the RFC 6749 style names {@code client_secret_post} and {@code
 * client_secret_basic}. Matching is case-insensitive.
 */
public enum OAuth2ClientAuthenticationMethod {

  /** Client credentials are sent in the request body. */
  CLIENT_SECRET_POST("client_secret_post"),

  /** Client credentials are sent using HTTP Basic authentication. */
  CLIENT_SECRET_BASIC("client_secret_basic");

  private final String configValue;

  OAuth2ClientAuthenticationMethod(String configValue) {
    this.configValue = configValue;
  }

  /**
   * Returns the canonical RFC-style config value for this method.
   *
   * @return {@code client_secret_post} or {@code client_secret_basic}
   */
  public String configValue() {
    return configValue;
  }

  /**
   * Parses an RFC-style OAuth2 client authentication method config value.
   *
   * <p>Accepted values are {@code client_secret_post} and {@code client_secret_basic}
   * (case-insensitive).
   *
   * @param value the config value
   * @return the matching authentication method
   * @throws IllegalArgumentException if {@code value} is blank or unsupported
   */
  public static OAuth2ClientAuthenticationMethod fromConfigValue(String value) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(value), "OAuth2 client authentication method must not be blank");
    String normalized = value.trim();
    for (OAuth2ClientAuthenticationMethod method : values()) {
      if (method.configValue.equalsIgnoreCase(normalized)) {
        return method;
      }
    }
    throw new IllegalArgumentException("Unsupported OAuth2 client authentication method: " + value);
  }
}
