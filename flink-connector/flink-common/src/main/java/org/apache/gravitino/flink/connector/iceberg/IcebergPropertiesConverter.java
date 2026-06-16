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

package org.apache.gravitino.flink.connector.iceberg;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergPropertiesUtils;
import org.apache.gravitino.flink.connector.CatalogPropertiesConverter;
import org.apache.gravitino.flink.connector.SchemaAndTablePropertiesConverter;
import org.apache.gravitino.flink.connector.store.GravitinoCatalogStoreFactoryOptions;
import org.apache.iceberg.rest.auth.AuthProperties;
import org.apache.iceberg.rest.auth.OAuth2Properties;

public class IcebergPropertiesConverter
    implements CatalogPropertiesConverter, SchemaAndTablePropertiesConverter {
  public static IcebergPropertiesConverter INSTANCE = new IcebergPropertiesConverter();

  private IcebergPropertiesConverter() {}

  private static final Map<String, String> GRAVITINO_CONFIG_TO_FLINK_ICEBERG =
      ImmutableMap.of(
          IcebergConstants.CATALOG_BACKEND, IcebergPropertiesConstants.ICEBERG_CATALOG_TYPE);

  // Simple key renames from Gravitino client auth config to Iceberg REST client auth properties.
  // The auth type value, and the OAuth2 token endpoint (built from server uri + token path), need
  // special handling and are not listed.
  private static final Map<String, String> GRAVITINO_AUTH_TO_ICEBERG_REST =
      ImmutableMap.of(
          GravitinoCatalogStoreFactoryOptions.OAUTH2_CREDENTIAL, OAuth2Properties.CREDENTIAL,
          GravitinoCatalogStoreFactoryOptions.OAUTH2_SCOPE, OAuth2Properties.SCOPE,
          GravitinoCatalogStoreFactoryOptions.BASIC_USERNAME, AuthProperties.BASIC_USERNAME,
          GravitinoCatalogStoreFactoryOptions.BASIC_PASSWORD, AuthProperties.BASIC_PASSWORD);

  @Override
  public String transformPropertyToGravitinoCatalog(String configKey) {
    return IcebergPropertiesUtils.ICEBERG_CATALOG_CONFIG_TO_GRAVITINO.get(configKey);
  }

  @Override
  public String transformPropertyToFlinkCatalog(String configKey) {

    String icebergConfigKey = null;
    if (IcebergPropertiesUtils.GRAVITINO_CONFIG_TO_ICEBERG.containsKey(configKey)) {
      icebergConfigKey = IcebergPropertiesUtils.GRAVITINO_CONFIG_TO_ICEBERG.get(configKey);
    }
    if (GRAVITINO_CONFIG_TO_FLINK_ICEBERG.containsKey(configKey)) {
      icebergConfigKey = GRAVITINO_CONFIG_TO_FLINK_ICEBERG.get(configKey);
    }
    return icebergConfigKey;
  }

  @Override
  public Map<String, String> toGravitinoTableProperties(Map<String, String> properties) {
    return new HashMap<>(properties);
  }

  @Override
  public String getFlinkCatalogType() {
    return GravitinoIcebergCatalogFactoryOptions.IDENTIFIER;
  }

  /**
   * Maps the Gravitino client's authentication config to the Iceberg REST client's auth properties,
   * so the Iceberg REST catalog can authenticate against the configured Iceberg REST service.
   *
   * @param gravitinoClientConfig the Gravitino client config carrying the authentication entries
   * @return Iceberg REST auth properties, empty when no supported auth is configured
   */
  public Map<String, String> toRestAuthProperties(Map<String, String> gravitinoClientConfig) {
    Map<String, String> authProperties = Maps.newHashMap();
    String authType = gravitinoClientConfig.get(GravitinoCatalogStoreFactoryOptions.AUTH_TYPE);
    if (GravitinoCatalogStoreFactoryOptions.OAUTH2.equalsIgnoreCase(authType)) {
      authProperties.put(AuthProperties.AUTH_TYPE, AuthProperties.AUTH_TYPE_OAUTH2);
      // Iceberg expects a single token endpoint URI, while Gravitino splits it into server uri and
      // token path, so they are joined here with a single separating slash.
      String serverUri =
          gravitinoClientConfig.get(GravitinoCatalogStoreFactoryOptions.OAUTH2_SERVER_URI);
      String tokenPath =
          gravitinoClientConfig.get(GravitinoCatalogStoreFactoryOptions.OAUTH2_TOKEN_PATH);
      if (StringUtils.isNotBlank(serverUri)) {
        authProperties.put(
            OAuth2Properties.OAUTH2_SERVER_URI,
            StringUtils.isNotBlank(tokenPath)
                ? StringUtils.stripEnd(serverUri, "/")
                    + "/"
                    + StringUtils.stripStart(tokenPath, "/")
                : serverUri);
      }
    } else if (GravitinoCatalogStoreFactoryOptions.BASIC.equalsIgnoreCase(authType)) {
      authProperties.put(AuthProperties.AUTH_TYPE, AuthProperties.AUTH_TYPE_BASIC);
    } else {
      // No auth or an auth type not applicable to the Iceberg REST client (e.g. kerberos).
      return authProperties;
    }
    GRAVITINO_AUTH_TO_ICEBERG_REST.forEach(
        (gravitinoKey, icebergKey) -> {
          String value = gravitinoClientConfig.get(gravitinoKey);
          if (StringUtils.isNotBlank(value)) {
            authProperties.put(icebergKey, value);
          }
        });
    return authProperties;
  }
}
