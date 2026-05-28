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
package org.apache.gravitino.catalog.lakehouse.iceberg;

import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.credential.JdbcCredential;
import org.apache.gravitino.rel.ViewCatalog;
import org.apache.gravitino.storage.AzureProperties;
import org.apache.gravitino.storage.OSSProperties;
import org.apache.gravitino.storage.S3Properties;
import org.apache.gravitino.utils.HierarchicalSchemaUtil;

/** Implementation of an Apache Iceberg catalog in Apache Gravitino. */
public class IcebergCatalog extends BaseCatalog<IcebergCatalog> {

  static final IcebergCatalogPropertiesMetadata CATALOG_PROPERTIES_META =
      new IcebergCatalogPropertiesMetadata();

  static final IcebergSchemaPropertiesMetadata SCHEMA_PROPERTIES_META =
      new IcebergSchemaPropertiesMetadata();

  static final IcebergTablePropertiesMetadata TABLE_PROPERTIES_META =
      new IcebergTablePropertiesMetadata();

  /**
   * @return The short name of the catalog.
   */
  @Override
  public String shortName() {
    return "lakehouse-iceberg";
  }

  /**
   * Creates a new instance of {@link IcebergCatalogOperations} with the provided configuration.
   *
   * @param config The configuration map for the Iceberg catalog operations.
   * @return A new instance of {@link IcebergCatalogOperations}.
   */
  @Override
  protected CatalogOperations newOps(Map<String, String> config) {
    IcebergCatalogOperations ops = new IcebergCatalogOperations();
    return ops;
  }

  @Override
  public ViewCatalog asViewCatalog() {
    return (ViewCatalog) ops();
  }

  @Override
  public Capability newCapability() {
    return new IcebergCatalogCapability(HierarchicalSchemaUtil.schemaSeparator());
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
  @Evolving
  public Map<String, String> propertiesWithCredentialProviders() {
    Map<String, String> props = super.propertiesWithCredentialProviders();
    // Iceberg is security-first: disable s3:ListBucket on bare location prefix so a vended
    // credential cannot enumerate sibling keys. This is catalog-type policy, not user-configurable.
    props.put(CredentialConstants.S3_CREDENTIAL_LIST_LOCATION_PREFIX, "false");
    return props;
  }

  @Override
  protected void addCatalogSpecificCredentialProviders(
      Map<String, String> properties, List<String> credentialProviders) {
    String catalogBackend = properties.get(IcebergConstants.CATALOG_BACKEND);
    if (catalogBackend != null
        && IcebergCatalogBackend.JDBC.name().equalsIgnoreCase(catalogBackend)) {
      String jdbcUser = properties.get(IcebergConstants.GRAVITINO_JDBC_USER);
      String jdbcPassword = properties.get(IcebergConstants.GRAVITINO_JDBC_PASSWORD);
      if (StringUtils.isNotBlank(jdbcUser) && jdbcPassword != null) {
        credentialProviders.add(JdbcCredential.JDBC_CREDENTIAL_TYPE);
      }
    }
    addStorageCredentialProviders(properties, credentialProviders);
  }

  @Override
  protected List<String> hiddenCredentialKeys() {
    return List.of(
        IcebergConstants.GRAVITINO_JDBC_USER,
        IcebergConstants.GRAVITINO_JDBC_PASSWORD,
        S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY,
        OSSProperties.GRAVITINO_OSS_ACCESS_KEY_SECRET,
        AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_KEY);
  }
}
