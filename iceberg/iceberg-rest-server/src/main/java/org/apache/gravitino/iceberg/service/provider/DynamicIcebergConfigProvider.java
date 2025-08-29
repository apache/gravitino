/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.iceberg.service.provider;

import static org.apache.gravitino.connector.BaseCatalog.CATALOG_BYPASS_PREFIX;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.auth.AuthProperties;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergPropertiesUtils;
import org.apache.gravitino.client.DefaultOAuth2TokenProvider;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.client.GravitinoClient.ClientBuilder;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.utils.MapUtils;

/**
 * This provider proxy Gravitino lakehouse-iceberg catalogs.
 *
 * <p>For example, there are one catalog named iceberg_catalog in metalake
 *
 * <p>The catalogName is iceberg_catalog
 */
public class DynamicIcebergConfigProvider implements IcebergConfigProvider {

  private String gravitinoMetalake;
  private String gravitinoUri;
  private Optional<String> defaultDynamicCatalogName;
  private Map<String, String> properties;

  private volatile GravitinoClient client;

  @Override
  public void initialize(Map<String, String> properties) {
    String uri = properties.get(IcebergConstants.GRAVITINO_URI);
    String metalake = properties.get(IcebergConstants.GRAVITINO_METALAKE);

    Preconditions.checkArgument(
        StringUtils.isNotBlank(uri), IcebergConstants.GRAVITINO_URI + " is blank");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(metalake), IcebergConstants.GRAVITINO_METALAKE + " is blank");

    this.gravitinoMetalake = metalake;
    this.gravitinoUri = uri;
    this.defaultDynamicCatalogName =
        Optional.ofNullable(
            properties.get(IcebergConstants.ICEBERG_REST_DEFAULT_DYNAMIC_CATALOG_NAME));
    this.properties = properties;
  }

  @Override
  public Optional<IcebergConfig> getIcebergCatalogConfig(String catalogName) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(catalogName), "blank catalogName is illegal");
    if (catalogName.equals(IcebergConstants.ICEBERG_REST_DEFAULT_CATALOG)) {
      catalogName =
          defaultDynamicCatalogName.orElseThrow(
              () ->
                  new IllegalArgumentException(
                      String.format(
                          "For dynamic config provider, Please use `%s` in iceberg client side to specify "
                              + "the catalog name or setting `%s` in REST server side to specify the "
                              + "default catalog name.",
                          IcebergConstants.WAREHOUSE,
                          IcebergConfig.ICEBERG_CONFIG_PREFIX
                              + IcebergConstants.ICEBERG_REST_DEFAULT_DYNAMIC_CATALOG_NAME)));
    }
    Catalog catalog;
    try {
      catalog = getGravitinoClient().loadMetalake(gravitinoMetalake).loadCatalog(catalogName);
    } catch (NoSuchCatalogException e) {
      return Optional.empty();
    }

    Preconditions.checkArgument(
        "lakehouse-iceberg".equals(catalog.provider()),
        String.format("%s.%s is not iceberg catalog", gravitinoMetalake, catalogName));

    Map<String, String> catalogProperties = catalog.properties();
    Map<String, String> properties = new HashMap<>();
    properties.putAll(IcebergPropertiesUtils.toIcebergCatalogProperties(catalogProperties));
    properties.putAll(MapUtils.getPrefixMap(catalogProperties, CATALOG_BYPASS_PREFIX));

    return Optional.of(new IcebergConfig(properties));
  }

  @VisibleForTesting
  void setClient(GravitinoClient client) {
    this.client = client;
  }

  @Override
  public void close() {
    if (client != null) {
      client.close();
    }
  }

  @Override
  public String getMetalakeName() {
    return gravitinoMetalake;
  }

  // client is lazy loaded because the Gravitino server may not be started yet when the provider is
  // initialized.
  private GravitinoClient getGravitinoClient() {
    if (client != null) {
      return client;
    }
    synchronized (this) {
      if (client == null) {
        client = createGravitinoClient(gravitinoUri, gravitinoMetalake, properties);
      }
    }
    return client;
  }

  private GravitinoClient createGravitinoClient(
      String uri, String metalake, Map<String, String> properties) {
    ClientBuilder builder = GravitinoClient.builder(uri).withMetalake(metalake);
    String authType =
        properties.getOrDefault(
            IcebergConstants.GRAVITINO_AUTH_TYPE, AuthProperties.SIMPLE_AUTH_TYPE);
    if (AuthProperties.isSimple(authType)) {
      String userName =
          properties.getOrDefault(
              IcebergConstants.GRAVITINO_SIMPLE_USERNAME, "iceberg-rest-server");
      builder.withSimpleAuth(userName);
    } else if (AuthProperties.isOAuth2(authType)) {
      String oAuthUri = getRequiredConfig(properties, IcebergConstants.GRAVITINO_OAUTH2_SERVER_URI);
      String credential =
          getRequiredConfig(properties, IcebergConstants.GRAVITINO_OAUTH2_CREDENTIAL);
      String path = getRequiredConfig(properties, IcebergConstants.GRAVITINO_OAUTH2_TOKEN_PATH);
      String scope = getRequiredConfig(properties, IcebergConstants.GRAVITINO_OAUTH2_SCOPE);
      DefaultOAuth2TokenProvider oAuth2TokenProvider =
          DefaultOAuth2TokenProvider.builder()
              .withUri(oAuthUri)
              .withCredential(credential)
              .withPath(path)
              .withScope(scope)
              .build();
      builder.withOAuth(oAuth2TokenProvider);
    } else {
      throw new UnsupportedOperationException("Unsupported auth type: " + authType);
    }
    return builder.build();
  }

  private String getRequiredConfig(Map<String, String> properties, String key) {
    String configValue = properties.get(key);
    Preconditions.checkArgument(StringUtils.isNotBlank(configValue), key + " should not be empty");
    return configValue;
  }
}
