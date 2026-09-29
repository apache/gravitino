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

import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.credential.JdbcCredential;
import org.apache.gravitino.rel.ViewCatalog;

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
  public ViewCatalog asViewCatalog() {
    return (ViewCatalog) ops();
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

  /**
   * Adds a JDBC credential provider when the backend is JDBC and credentials are configured, then
   * delegates to the parent for storage (S3/OSS/Azure/GCS) credential provider detection.
   *
   * @param properties the raw catalog properties
   * @param credentialProviders the list to append detected provider names to
   */
  @Override
  protected void addCatalogSpecificCredentialProviders(
      Map<String, String> properties, List<String> credentialProviders) {
    String catalogBackend = properties.get(PaimonConstants.CATALOG_BACKEND);
    if (catalogBackend != null
        && PaimonCatalogBackend.JDBC.name().equalsIgnoreCase(catalogBackend)) {
      String jdbcUser = properties.get(PaimonConstants.GRAVITINO_JDBC_USER);
      String jdbcPassword = properties.get(PaimonConstants.GRAVITINO_JDBC_PASSWORD);
      if (StringUtils.isNotBlank(jdbcUser) && jdbcPassword != null) {
        credentialProviders.add(JdbcCredential.JDBC_CREDENTIAL_TYPE);
      }
    }
    addStorageCredentialProviders(properties, credentialProviders);
  }
}
