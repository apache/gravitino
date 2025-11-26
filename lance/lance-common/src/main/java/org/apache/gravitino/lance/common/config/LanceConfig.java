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
import org.apache.gravitino.config.ConfigBuilder;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.config.ConfigEntry;

/** Base Lance REST configuration. */
public class LanceConfig extends Config implements OverwriteDefaultConfig {

  public static final String LANCE_CONFIG_PREFIX = "gravitino.lance-rest.";
  public static final String CONFIG_NAMESPACE_BACKEND = "namespace-backend";
  public static final String CONFIG_METALAKE = "metalake-name";
  public static final String CONFIG_URI = "uri";

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
      new ConfigBuilder(GRAVITINO_NAMESPACE_BACKEND + "." + CONFIG_METALAKE)
          .doc("The Metalake name for Lance Gravitino namespace backend")
          .version(ConfigConstants.VERSION_1_1_0)
          .stringConf()
          .create();

  public static final ConfigEntry<String> NAMESPACE_BACKEND_URI =
      new ConfigBuilder(GRAVITINO_NAMESPACE_BACKEND + "." + CONFIG_URI)
          .doc("The URI of the namespace backend, e.g., Gravitino server URI")
          .version(ConfigConstants.VERSION_1_1_0)
          .stringConf()
          .createWithDefault(GRAVITINO_URI);

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

  @Override
  public Map<String, String> getOverwriteDefaultConfig() {
    return ImmutableMap.of(
        ConfigConstants.WEBSERVER_HTTP_PORT,
        String.valueOf(DEFAULT_LANCE_REST_SERVICE_HTTP_PORT),
        ConfigConstants.WEBSERVER_HTTPS_PORT,
        String.valueOf(DEFAULT_LANCE_REST_SERVICE_HTTPS_PORT));
  }
}
