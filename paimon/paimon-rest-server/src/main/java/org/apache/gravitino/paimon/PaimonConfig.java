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
package org.apache.gravitino.paimon;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Config;
import org.apache.gravitino.OverwriteDefaultConfig;
import org.apache.gravitino.config.ConfigBuilder;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.config.ConfigEntry;

/**
 * Configuration of the Paimon REST catalog server. In auxiliary mode the Gravitino server passes
 * all properties with the {@code gravitino.paimon-rest.} prefix (the prefix itself is stripped
 * before this class sees them); in standalone mode the properties are loaded from {@code
 * gravitino-paimon-rest-server.conf} with {@link #PAIMON_REST_CONFIG_PREFIX} stripped by the
 * caller.
 *
 * <p>Every property that is not one of the typed entries below is kept verbatim in the config map
 * and forwarded to the backend Paimon catalogs and the clients via the {@code /v1/config} response.
 */
public class PaimonConfig extends Config implements OverwriteDefaultConfig {

  public static final String PAIMON_REST_CONFIG_PREFIX = "gravitino.paimon-rest.";

  public static final int DEFAULT_PAIMON_REST_SERVICE_HTTP_PORT = 9002;
  public static final int DEFAULT_PAIMON_REST_SERVICE_HTTPS_PORT = 9543;

  @VisibleForTesting
  public static final String PAIMON_REST_EXTENSION_PACKAGES = "extension-packages";

  public static final ConfigEntry<String> BEARER_TOKEN =
      new ConfigBuilder("token")
          .doc(
              "Shared bearer token expected in the Authorization header of REST catalog "
                  + "requests. When set, requests without a matching token are rejected with "
                  + "401; when unset, validation is delegated to the Gravitino authenticator "
                  + "framework")
          .version(ConfigConstants.VERSION_1_0_0)
          .stringConf()
          .createWithDefault("");

  /** Prefix of per-principal bearer token entries: {@code token.<principal> = <token>}. */
  public static final String TOKEN_PRINCIPAL_PREFIX = "token.";

  public static final ConfigEntry<List<String>> REST_API_EXTENSION_PACKAGES =
      new ConfigBuilder(PAIMON_REST_EXTENSION_PACKAGES)
          .doc(
              "Comma separated list of JAX-RS resource packages to extend the Paimon REST "
                  + "catalog server")
          .version(ConfigConstants.VERSION_1_0_0)
          .stringConf()
          .toSequence()
          .createWithDefault(Collections.emptyList());

  public PaimonConfig(Map<String, String> properties) {
    super(false);
    loadFromMap(properties, k -> true);
  }

  public PaimonConfig() {
    super(false);
  }

  /**
   * Returns the per-principal bearer tokens declared with {@code token.<principal> = <token>}
   * entries, mapping principal name to token value.
   *
   * @return the principal to token mapping
   */
  public Map<String, String> getPrincipalTokens() {
    Map<String, String> tokens = new java.util.HashMap<>();
    getAllConfig()
        .forEach(
            (key, value) -> {
              if (key.startsWith(TOKEN_PRINCIPAL_PREFIX)
                  && key.length() > TOKEN_PRINCIPAL_PREFIX.length()) {
                tokens.put(key.substring(TOKEN_PRINCIPAL_PREFIX.length()), value);
              }
            });
    return tokens;
  }

  @Override
  public Map<String, String> getOverwriteDefaultConfig() {
    return ImmutableMap.of(
        ConfigConstants.WEBSERVER_HTTP_PORT,
        String.valueOf(DEFAULT_PAIMON_REST_SERVICE_HTTP_PORT),
        ConfigConstants.WEBSERVER_HTTPS_PORT,
        String.valueOf(DEFAULT_PAIMON_REST_SERVICE_HTTPS_PORT));
  }
}
