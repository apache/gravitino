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

package org.apache.gravitino.iceberg.service;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.credential.CatalogCredentialManager;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.credential.CredentialPropertyUtils;
import org.apache.gravitino.credential.PathBasedCredentialContext;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.common.ops.IcebergCatalogWrapper;
import org.apache.gravitino.storage.GCSProperties;
import org.apache.gravitino.utils.MapUtils;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ServiceUnavailableException;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;

/** Process Iceberg REST specific operations, like credential vending. */
public class CatalogWrapperForREST extends IcebergCatalogWrapper {

  private final CatalogCredentialManager catalogCredentialManager;

  private final Map<String, String> catalogConfigToClients;

  private static final Set<String> catalogPropertiesToClientKeys =
      ImmutableSet.of(
          IcebergConstants.IO_IMPL,
          IcebergConstants.AWS_S3_REGION,
          IcebergConstants.ICEBERG_S3_ENDPOINT,
          IcebergConstants.ICEBERG_OSS_ENDPOINT);

  @SuppressWarnings("deprecation")
  private static Map<String, String> deprecatedProperties =
      ImmutableMap.of(
          CredentialConstants.CREDENTIAL_PROVIDER_TYPE,
          CredentialConstants.CREDENTIAL_PROVIDERS,
          "gcs-credential-file-path",
          GCSProperties.GRAVITINO_GCS_SERVICE_ACCOUNT_FILE);

  public CatalogWrapperForREST(String catalogName, IcebergConfig config) {
    super(config);
    this.catalogConfigToClients =
        MapUtils.getFilteredMap(
            config.getIcebergCatalogProperties(),
            key -> catalogPropertiesToClientKeys.contains(key));
    // To be compatible with old properties
    Map<String, String> catalogProperties =
        checkForCompatibility(config.getAllConfig(), deprecatedProperties);
    this.catalogCredentialManager = new CatalogCredentialManager(catalogName, catalogProperties);
  }

  public LoadTableResponse createTable(
      Namespace namespace, CreateTableRequest request, boolean requestCredential) {
    LoadTableResponse loadTableResponse = super.createTable(namespace, request);
    if (requestCredential) {
      return injectCredentialConfig(
          TableIdentifier.of(namespace, request.name()), loadTableResponse);
    }
    return loadTableResponse;
  }

  public LoadTableResponse loadTable(TableIdentifier identifier, boolean requestCredential) {
    LoadTableResponse loadTableResponse = super.loadTable(identifier);
    if (requestCredential) {
      return injectCredentialConfig(identifier, loadTableResponse);
    }
    return loadTableResponse;
  }

  @Override
  public void close() {
    if (catalogCredentialManager != null) {
      catalogCredentialManager.close();
    }
  }

  public Map<String, String> getCatalogConfigToClient() {
    return catalogConfigToClients;
  }

  private LoadTableResponse injectCredentialConfig(
      TableIdentifier tableIdentifier, LoadTableResponse loadTableResponse) {
    TableMetadata tableMetadata = loadTableResponse.tableMetadata();
    String[] path =
        Stream.of(
                tableMetadata.location(),
                tableMetadata.property(TableProperties.WRITE_DATA_LOCATION, ""),
                tableMetadata.property(TableProperties.WRITE_METADATA_LOCATION, ""))
            .filter(StringUtils::isNotBlank)
            .toArray(String[]::new);

    PathBasedCredentialContext context =
        new PathBasedCredentialContext(
            PrincipalUtils.getCurrentUserName(), ImmutableSet.copyOf(path), Collections.emptySet());
    Credential credential = catalogCredentialManager.getCredential(context);
    if (credential == null) {
      throw new ServiceUnavailableException("Couldn't generate credential, %s", context);
    }

    LOG.info(
        "Generate credential: {} for Iceberg table: {}",
        credential.credentialType(),
        tableIdentifier);

    Map<String, String> credentialConfig = CredentialPropertyUtils.toIcebergProperties(credential);
    return LoadTableResponse.builder()
        .withTableMetadata(loadTableResponse.tableMetadata())
        .addAllConfig(loadTableResponse.config())
        .addAllConfig(getCatalogConfigToClient())
        .addAllConfig(credentialConfig)
        .build();
  }

  @VisibleForTesting
  static Map<String, String> checkForCompatibility(
      Map<String, String> properties, Map<String, String> deprecatedProperties) {
    Map<String, String> newProperties = new HashMap<>(properties);
    deprecatedProperties.forEach(
        (deprecatedProperty, newProperty) -> {
          replaceDeprecatedProperties(newProperties, deprecatedProperty, newProperty);
        });
    return newProperties;
  }

  private static void replaceDeprecatedProperties(
      Map<String, String> properties, String deprecatedProperty, String newProperty) {
    String deprecatedValue = properties.get(deprecatedProperty);
    String newValue = properties.get(newProperty);
    if (StringUtils.isNotBlank(deprecatedValue) && StringUtils.isNotBlank(newValue)) {
      throw new IllegalArgumentException(
          String.format("Should not set both %s and %s", deprecatedProperty, newProperty));
    }

    if (StringUtils.isNotBlank(deprecatedValue)) {
      LOG.warn("{} is deprecated, please use {} instead.", deprecatedProperty, newProperty);
      properties.remove(deprecatedProperty);
      properties.put(newProperty, deprecatedValue);
    }
  }
}
