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

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.credential.AzureAccountKeyCredential;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.credential.JdbcCredential;
import org.apache.gravitino.credential.OSSSecretKeyCredential;
import org.apache.gravitino.credential.S3SecretKeyCredential;
import org.apache.gravitino.storage.AzureProperties;
import org.apache.gravitino.storage.OSSProperties;
import org.apache.gravitino.storage.S3Properties;

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
  public Map<String, String> properties() {
    Map<String, String> properties = super.properties();
    return buildCredentialProvidersIfNecessary(properties);
  }

  private Map<String, String> buildCredentialProvidersIfNecessary(Map<String, String> properties) {
    // If credential providers already set, return as is
    if (StringUtils.isNotBlank(properties.get(CredentialConstants.CREDENTIAL_PROVIDERS))) {
      return properties;
    }

    String catalogBackend = properties.get(PaimonConstants.CATALOG_BACKEND);
    if (catalogBackend == null) {
      return properties;
    }

    // Check if it's JDBC backend
    if (!PaimonCatalogBackend.JDBC
        .name()
        .equalsIgnoreCase(catalogBackend.toUpperCase(Locale.ROOT))) {
      return properties;
    }

    // Build default credential providers for JDBC backend
    List<String> credentialProviders = new ArrayList<>();

    // Add JDBC credential provider if jdbc-user and jdbc-password are set
    String jdbcUser = properties.get(PaimonConstants.GRAVITINO_JDBC_USER);
    String jdbcPassword = properties.get(PaimonConstants.GRAVITINO_JDBC_PASSWORD);
    if (StringUtils.isNotBlank(jdbcUser) && StringUtils.isNotBlank(jdbcPassword)) {
      credentialProviders.add(JdbcCredential.JDBC_CREDENTIAL_TYPE);
    }

    // Add S3 secret key credential provider if S3 access key and secret key are set
    String s3AccessKeyId = properties.get(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID);
    String s3SecretAccessKey = properties.get(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY);
    if (StringUtils.isNotBlank(s3AccessKeyId) && StringUtils.isNotBlank(s3SecretAccessKey)) {
      credentialProviders.add(S3SecretKeyCredential.S3_SECRET_KEY_CREDENTIAL_TYPE);
    }

    // Add OSS secret key credential provider if OSS access key and secret key are set
    String ossAccessKeyId = properties.get(OSSProperties.GRAVITINO_OSS_ACCESS_KEY_ID);
    String ossSecretAccessKey = properties.get(OSSProperties.GRAVITINO_OSS_ACCESS_KEY_SECRET);
    if (StringUtils.isNotBlank(ossAccessKeyId) && StringUtils.isNotBlank(ossSecretAccessKey)) {
      credentialProviders.add(OSSSecretKeyCredential.OSS_SECRET_KEY_CREDENTIAL_TYPE);
    }

    // Add Azure account key credential provider if Azure account name and key are set
    String azureAccountName = properties.get(AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_NAME);
    String azureAccountKey = properties.get(AzureProperties.GRAVITINO_AZURE_STORAGE_ACCOUNT_KEY);
    if (StringUtils.isNotBlank(azureAccountName) && StringUtils.isNotBlank(azureAccountKey)) {
      credentialProviders.add(AzureAccountKeyCredential.AZURE_ACCOUNT_KEY_CREDENTIAL_TYPE);
    }

    if (!credentialProviders.isEmpty()) {
      properties.put(
          CredentialConstants.CREDENTIAL_PROVIDERS, String.join(",", credentialProviders));
    }

    return properties;
  }
}
