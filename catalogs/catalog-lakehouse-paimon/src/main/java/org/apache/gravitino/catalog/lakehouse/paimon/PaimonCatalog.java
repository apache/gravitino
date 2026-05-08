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
package org.apache.gravitino.catalog.lakehouse.paimon;

import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.credential.JdbcCredential;

/**
 * Implementation of {@link Catalog} that represents an Apache Paimon catalog in Apache Gravitino.
 */
public class PaimonCatalog extends BaseCatalog<PaimonCatalog> {

  static final PaimonCatalogPropertiesMetadata CATALOG_PROPERTIES_META =
      new PaimonCatalogPropertiesMetadata();

  static final PaimonSchemaPropertiesMetadata SCHEMA_PROPERTIES_META =
      new PaimonSchemaPropertiesMetadata();

  static final PaimonTablePropertiesMetadata TABLE_PROPERTIES_META =
      new PaimonTablePropertiesMetadata();

  /**
   * @return The short name of the catalog.
   */
  @Override
  public String shortName() {
    return "lakehouse-paimon";
  }

  /**
   * Creates a new instance of {@link PaimonCatalogOperations} with the provided configuration.
   *
   * @param config The configuration map for the Paimon catalog operations.
   * @return A new instance of {@link PaimonCatalogOperations}.
   */
  @Override
  protected CatalogOperations newOps(Map<String, String> config) {
    return new PaimonCatalogOperations();
  }

  @Override
  public Capability newCapability() {
    return new PaimonCatalogCapability();
  }

  @Override
  public PropertiesMetadata tablePropertiesMetadata() throws UnsupportedOperationException {
    return TABLE_PROPERTIES_META;
  }

  @Override
  public PropertiesMetadata catalogPropertiesMetadata() throws UnsupportedOperationException {
    return CATALOG_PROPERTIES_META;
  }

  @Override
  public PropertiesMetadata schemaPropertiesMetadata() throws UnsupportedOperationException {
    return SCHEMA_PROPERTIES_META;
  }

  @Override
  public Map<String, String> propertiesWithCredentialProviders() {
    Map<String, String> properties = Maps.newHashMap(super.propertiesWithCredentialProviders());
    return buildCredentialProvidersIfNecessary(properties);
  }

  private Map<String, String> buildCredentialProvidersIfNecessary(Map<String, String> properties) {
    // If credential providers already set, return as is
    if (StringUtils.isNotBlank(properties.get(CredentialConstants.CREDENTIAL_PROVIDERS))) {
      return properties;
    }

    List<String> credentialProviders = new ArrayList<>();

    // Add JDBC credential provider if backend is JDBC and jdbc-user/jdbc-password are set
    String catalogBackend = properties.get(PaimonConstants.CATALOG_BACKEND);
    if (catalogBackend != null
        && PaimonCatalogBackend.JDBC.name().equalsIgnoreCase(catalogBackend)) {
      String jdbcUser = properties.get(PaimonConstants.GRAVITINO_JDBC_USER);
      String jdbcPassword = properties.get(PaimonConstants.GRAVITINO_JDBC_PASSWORD);
      if (StringUtils.isNotBlank(jdbcUser) && StringUtils.isNotBlank(jdbcPassword)) {
        credentialProviders.add(JdbcCredential.JDBC_CREDENTIAL_TYPE);
      }
    }

    addStorageCredentialProviders(properties, credentialProviders);

    if (!credentialProviders.isEmpty()) {
      properties.put(
          CredentialConstants.CREDENTIAL_PROVIDERS, String.join(",", credentialProviders));
    }

    return properties;
  }
}
