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

import com.google.common.collect.ImmutableSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.gravitino.utils.MapUtils;

/** Configuration class for Gravitino Java client */
public class GravitinoClientConfiguration {
  /** The value of messages used to indicate that the configuration should be a positive number. */
  public static final String POSITIVE_NUMBER_ERROR_MSG = "The value must be a positive number";
  /** The configuration key prefix for the Gravitino client config. */
  public static final String GRAVITINO_CLIENT_CONFIG_PREFIX = "gravitino.client.";

  /** A default value for http connection timeout in milliseconds. */
  public static final long CLIENT_CONNECTION_TIMEOUT_MS_DEFAULT = 180_000L;

  /** A default value for http socket timeout in milliseconds. */
  public static final int CLIENT_SOCKET_TIMEOUT_MS_DEFAULT = 180_000;

  /** An optional http connection timeout in milliseconds. */
  public static final String CLIENT_CONNECTION_TIMEOUT_MS = "gravitino.client.connectionTimeoutMs";

  /** An optional http socket timeout in milliseconds. */
  public static final String CLIENT_SOCKET_TIMEOUT_MS = "gravitino.client.socketTimeoutMs";

  private static final Set<String> SUPPORT_CLIENT_CONFIG_KEYS =
      ImmutableSet.of(CLIENT_CONNECTION_TIMEOUT_MS, CLIENT_SOCKET_TIMEOUT_MS);

  private Map<String, String> properties;

  private GravitinoClientConfiguration(Map<String, String> properties) {
    this.properties = properties;
  }

  /**
   * Build GravitinoClientConfiguration from properties.
   *
   * @param properties The properties object containing configuration key-value pairs.
   * @return GravitinoClientConfiguration instance
   */
  public static GravitinoClientConfiguration buildFromProperties(Map<String, String> properties) {
    for (String key : properties.keySet()) {
      if (!SUPPORT_CLIENT_CONFIG_KEYS.contains(key)) {
        throw new IllegalArgumentException(String.format("Invalid property for client: %s", key));
      }
    }
    return new GravitinoClientConfiguration(properties);
  }

  /**
   * Extract connection timeout from the properties map
   *
   * @return connection timeout
   */
  public long getClientConnectionTimeoutMs() {
    long connectionTimeoutMillis =
        MapUtils.propertyAsLong(
            properties, CLIENT_CONNECTION_TIMEOUT_MS, CLIENT_CONNECTION_TIMEOUT_MS_DEFAULT);
    checkValue(
        value -> value >= 0,
        CLIENT_CONNECTION_TIMEOUT_MS,
        connectionTimeoutMillis,
        POSITIVE_NUMBER_ERROR_MSG);
    return connectionTimeoutMillis;
  }

  /**
   * Extract socket timeout from the properties map
   *
   * @return socket timeout
   */
  public int getClientSocketTimeoutMs() {
    int socketTimeoutMillis =
        MapUtils.propertyAsInt(
            properties, CLIENT_SOCKET_TIMEOUT_MS, CLIENT_SOCKET_TIMEOUT_MS_DEFAULT);
    checkValue(
        value -> value >= 0,
        CLIENT_SOCKET_TIMEOUT_MS,
        socketTimeoutMillis,
        POSITIVE_NUMBER_ERROR_MSG);
    return socketTimeoutMillis;
  }

  private static <T> void checkValue(
      Function<T, Boolean> checkValueFunc, String key, T value, String errorMsg) {
    if (!checkValueFunc.apply(value)) {
      throw new IllegalArgumentException(
          String.format("%s in %s is invalid. %s", value, key, errorMsg));
    }
  }
}
