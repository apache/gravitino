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
package org.apache.gravitino.lance.common.config;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.Config;
import org.apache.gravitino.OverwriteDefaultConfig;
import org.apache.gravitino.auth.AuthProperties;
import org.apache.gravitino.config.ConfigBuilder;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.config.ConfigEntry;

/** Base Lance REST configuration. */
public class LanceConfig extends Config implements OverwriteDefaultConfig {

  public static final String LANCE_CONFIG_PREFIX = "gravitino.lance-rest.";
  public static final String CONFIG_NAMESPACE_BACKEND = "namespace-backend";
  public static final String CONFIG_METALAKE = "metalake";
  public static final String CONFIG_URI = "uri";
  public static final String CONFIG_AUTH_TYPE = "auth-type";
  public static final String DEFAULT_SIMPLE_USERNAME = "lance-rest-server";

  public static final int DEFAULT_LANCE_REST_SERVICE_HTTP_PORT = 9101;
  public static final int DEFAULT_LANCE_REST_SERVICE_HTTPS_PORT = 9533;
  public static final String GRAVITINO_NAMESPACE_BACKEND = "gravitino";
  public static final String GRAVITINO_URI = "http://localhost:8090";

  public static final ConfigEntry<String> NAMESPACE_BACKEND =
      new ConfigBuilder(CONFIG_NAMESPACE_BACKEND)
          .doc("The backend implementation for namespace operations")
          .version(ConfigConstants.VERSION_1_1_0)
          .stringConf()
          .createWithDefault(GRAVITINO_NAMESPACE_BACKEND);

  public static final ConfigEntry<String> METALAKE_NAME =
      new ConfigBuilder(GRAVITINO_NAMESPACE_BACKEND + "-" + CONFIG_METALAKE)
          .doc("The Metalake name for Lance Gravitino namespace backend")
          .version(ConfigConstants.VERSION_1_1_0)
          .stringConf()
          .create();

  public static final ConfigEntry<String> NAMESPACE_BACKEND_URI =
      new ConfigBuilder(GRAVITINO_NAMESPACE_BACKEND + "-" + CONFIG_URI)
          .doc("The URI of the namespace backend, e.g., Gravitino server URI")
          .version(ConfigConstants.VERSION_1_1_0)
          .stringConf()
          .createWithDefault(GRAVITINO_URI);

  public static final ConfigEntry<String> GRAVITINO_AUTH_TYPE =
      new ConfigBuilder(GRAVITINO_NAMESPACE_BACKEND + "-" + CONFIG_AUTH_TYPE)
          .doc(
              "The auth type used when the Lance REST service communicates with the Gravitino "
                  + "server. Supported values are `simple` and `oauth2`.")
          .version(ConfigConstants.VERSION_1_3_0)
          .stringConf()
          .createWithDefault(AuthProperties.SIMPLE_AUTH_TYPE);

  public static final ConfigEntry<String> GRAVITINO_SIMPLE_USERNAME =
      new ConfigBuilder(GRAVITINO_NAMESPACE_BACKEND + "-simple.user-name")
          .doc("The user name used when the auth type is `simple`")
          .version(ConfigConstants.VERSION_1_3_0)
          .stringConf()
          .createWithDefault(DEFAULT_SIMPLE_USERNAME);

  public static final ConfigEntry<String> GRAVITINO_OAUTH2_SERVER_URI =
      new ConfigBuilder(GRAVITINO_NAMESPACE_BACKEND + "-oauth2.server-uri")
          .doc("The OAuth2 server URI, required when the auth type is `oauth2`")
          .version(ConfigConstants.VERSION_1_3_0)
          .stringConf()
          .create();

  public static final ConfigEntry<String> GRAVITINO_OAUTH2_CREDENTIAL =
      new ConfigBuilder(GRAVITINO_NAMESPACE_BACKEND + "-oauth2.credential")
          .doc("The credential used to request the OAuth2 token, required for `oauth2`")
          .version(ConfigConstants.VERSION_1_3_0)
          .stringConf()
          .create();

  public static final ConfigEntry<String> GRAVITINO_OAUTH2_TOKEN_PATH =
      new ConfigBuilder(GRAVITINO_NAMESPACE_BACKEND + "-oauth2.token-path")
          .doc("The path on the OAuth2 server used to request the token, required for `oauth2`")
          .version(ConfigConstants.VERSION_1_3_0)
          .stringConf()
          .create();

  public static final ConfigEntry<String> GRAVITINO_OAUTH2_SCOPE =
      new ConfigBuilder(GRAVITINO_NAMESPACE_BACKEND + "-oauth2.scope")
          .doc("The scope of the requested OAuth2 token, required for `oauth2`")
          .version(ConfigConstants.VERSION_1_3_0)
          .stringConf()
          .create();

  public LanceConfig(Map<String, String> properties) {
    super(false);
    loadFromMap(properties, key -> true);
  }

  public LanceConfig() {
    super(false);
  }

  public String getNamespaceBackend() {
    return get(NAMESPACE_BACKEND);
  }

  public String getNamespaceBackendUri() {
    return get(NAMESPACE_BACKEND_URI);
  }

  public String getGravitinoMetalake() {
    return get(METALAKE_NAME);
  }

  /** Returns whether the Gravitino metalake is configured with a non-blank name. */
  public boolean isGravitinoMetalakeConfigured() {
    String metalake = getGravitinoMetalake();
    return metalake != null && !metalake.isBlank();
  }

  public String getGravitinoAuthType() {
    return get(GRAVITINO_AUTH_TYPE);
  }

  @Override
  public Map<String, String> getOverwriteDefaultConfig() {
    return ImmutableMap.of(
        ConfigConstants.WEBSERVER_HTTP_PORT,
        String.valueOf(DEFAULT_LANCE_REST_SERVICE_HTTP_PORT),
        ConfigConstants.WEBSERVER_HTTPS_PORT,
        String.valueOf(DEFAULT_LANCE_REST_SERVICE_HTTPS_PORT));
  }
}
