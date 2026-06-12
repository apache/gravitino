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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.gravitino.flink.connector.CatalogPropertiesConverter;
import org.apache.gravitino.flink.connector.DefaultPartitionConverter;
import org.apache.gravitino.flink.connector.PartitionConverter;
import org.apache.gravitino.flink.connector.SchemaAndTablePropertiesConverter;
import org.apache.gravitino.flink.connector.catalog.BaseCatalogFactory;
import org.apache.gravitino.flink.connector.catalog.GravitinoCatalogManager;
import org.apache.gravitino.flink.connector.store.GravitinoCatalogStoreFactoryOptions;
import org.apache.gravitino.flink.connector.utils.FactoryUtils;
import org.apache.iceberg.rest.auth.AuthProperties;
import org.apache.iceberg.rest.auth.OAuth2Properties;

public class GravitinoIcebergCatalogFactory implements BaseCatalogFactory {

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
  public Catalog createCatalog(Context context) {
    final FactoryUtil.CatalogFactoryHelper helper =
        FactoryUtils.createCatalogFactoryHelper(this, context);
    return newCatalog(
        context.getName(),
        helper.getOptions().get(GravitinoIcebergCatalogFactoryOptions.DEFAULT_DATABASE),
        schemaAndTablePropertiesConverter(),
        partitionConverter(),
        context.getOptions(),
        toIcebergCatalogOptions(context.getOptions()));
  }

  protected Catalog newCatalog(
      String catalogName,
      String defaultDatabase,
      SchemaAndTablePropertiesConverter schemaAndTablePropertiesConverter,
      PartitionConverter partitionConverter,
      Map<String, String> catalogOptions,
      Map<String, String> icebergCatalogProperties) {
    return new GravitinoIcebergCatalog(
        catalogName,
        defaultDatabase,
        schemaAndTablePropertiesConverter,
        partitionConverter,
        catalogOptions,
        icebergCatalogProperties);
  }

  @Override
  public String factoryIdentifier() {
    return GravitinoIcebergCatalogFactoryOptions.IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return Collections.emptySet();
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    return Collections.emptySet();
  }

  /**
   * Define gravitino catalog provider.
   *
   * @return The name of the Gravitino catalog provider, which is "lakehouse-iceberg" for this
   *     implementation.
   */
  @Override
  public String gravitinoCatalogProvider() {
    return "lakehouse-iceberg";
  }

  /**
   * Define gravitino catalog type.
   *
   * @return The type of the Gravitino catalog, which is RELATIONAL for this implementation.
   */
  @Override
  public org.apache.gravitino.Catalog.Type gravitinoCatalogType() {
    return org.apache.gravitino.Catalog.Type.RELATIONAL;
  }

  @Override
  public CatalogPropertiesConverter catalogPropertiesConverter() {
    return IcebergPropertiesConverter.INSTANCE;
  }

  public SchemaAndTablePropertiesConverter schemaAndTablePropertiesConverter() {
    return IcebergPropertiesConverter.INSTANCE;
  }

  @Override
  public PartitionConverter partitionConverter() {
    return DefaultPartitionConverter.INSTANCE;
  }

  private Map<String, String> toIcebergCatalogOptions(Map<String, String> catalogOptions) {
    Map<String, String> icebergCatalogOptions = Maps.newHashMap(catalogOptions);
    String catalogBackend =
        catalogOptions.get(IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND);
    if (catalogBackend != null
        && !icebergCatalogOptions.containsKey(IcebergPropertiesConstants.ICEBERG_CATALOG_TYPE)) {
      icebergCatalogOptions.put(IcebergPropertiesConstants.ICEBERG_CATALOG_TYPE, catalogBackend);
    }
    injectRestAuth(
        icebergCatalogOptions,
        catalogBackend,
        GravitinoCatalogManager.get().getGravitinoClientConfig());
    // The outer Flink factory is `gravitino-iceberg`, but the nested Iceberg factory still expects
    // `catalog-type=iceberg` when building the native Iceberg catalog instance.
    icebergCatalogOptions.put(CommonCatalogOptions.CATALOG_TYPE.key(), "iceberg");
    return icebergCatalogOptions;
  }

  /**
   * Propagates the Gravitino client's authentication to the Iceberg REST catalog options. A REST
   * backend connects directly to the Iceberg REST service, bypassing the Gravitino server's auth
   * proxy, so the REST client needs its own credentials. Does nothing for non-REST backends, or
   * when the user has already configured REST auth explicitly.
   *
   * @param icebergCatalogOptions the Iceberg catalog options to inject auth into
   * @param catalogBackend the Gravitino Iceberg catalog backend (e.g. {@code rest}, {@code hive})
   * @param gravitinoClientConfig the Gravitino client config carrying the authentication entries
   */
  @VisibleForTesting
  static void injectRestAuth(
      Map<String, String> icebergCatalogOptions,
      String catalogBackend,
      Map<String, String> gravitinoClientConfig) {
    if (IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_REST.equalsIgnoreCase(catalogBackend)
        && !icebergCatalogOptions.containsKey(AuthProperties.AUTH_TYPE)) {
      icebergCatalogOptions.putAll(toRestAuthProperties(gravitinoClientConfig));
    }
  }

  /**
   * Maps the Gravitino client's authentication config to the Iceberg REST client's auth properties,
   * so the Iceberg REST catalog can authenticate against the configured Iceberg REST service.
   *
   * @param gravitinoClientConfig the Gravitino client config carrying the authentication entries
   * @return Iceberg REST auth properties, empty when no supported auth is configured
   */
  @VisibleForTesting
  static Map<String, String> toRestAuthProperties(Map<String, String> gravitinoClientConfig) {
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
