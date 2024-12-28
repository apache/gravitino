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

  public CatalogWrapperForREST(String catalogName, IcebergConfig config) {
    super(config);
    this.catalogConfigToClients =
        MapUtils.getFilteredMap(
            config.getIcebergCatalogProperties(),
            key -> catalogPropertiesToClientKeys.contains(key));
    // To compatibility with old version
    Map<String, String> catalogProperties =
        adjustCredentialPropertiesForCompatibility(config.getAllConfig());
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

  private Map<String, String> getCatalogConfigToClient() {
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

  @SuppressWarnings("deprecation")
  private Map<String, String> adjustCredentialPropertiesForCompatibility(
      Map<String, String> properties) {
    HashMap<String, String> normalizedProperties = new HashMap<>(properties);
    String credentialProviderType = properties.get(CredentialConstants.CREDENTIAL_PROVIDER_TYPE);
    String credentialProviders = properties.get(CredentialConstants.CREDENTIAL_PROVIDERS);
    if (StringUtils.isNotBlank(credentialProviders)
        && StringUtils.isNotBlank(credentialProviderType)) {
      throw new IllegalArgumentException(
          String.format(
              "Should not set both %s and %s",
              CredentialConstants.CREDENTIAL_PROVIDER_TYPE,
              CredentialConstants.CREDENTIAL_PROVIDERS));
    }

    if (StringUtils.isNotBlank(credentialProviderType)) {
      LOG.warn(
          "%s is deprecated, please use %s instead.",
          CredentialConstants.CREDENTIAL_PROVIDER_TYPE, CredentialConstants.CREDENTIAL_PROVIDERS);
      normalizedProperties.put(CredentialConstants.CREDENTIAL_PROVIDERS, credentialProviderType);
    }

    return normalizedProperties;
  }
}
