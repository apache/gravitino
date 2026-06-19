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
package org.apache.gravitino.catalog.glue;

import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.storage.S3Properties;

/**
 * Implementation of an AWS Glue Data Catalog connector in Apache Gravitino.
 *
 * <p>Exposes all Glue table types (Hive, Iceberg, Delta, Parquet) through a single catalog using
 * {@code provider=glue}.
 */
public class GlueCatalog extends BaseCatalog<GlueCatalog> {

  static final GlueCatalogPropertiesMetadata CATALOG_PROPERTIES_METADATA =
      new GlueCatalogPropertiesMetadata();

  static final GlueSchemaPropertiesMetadata SCHEMA_PROPERTIES_METADATA =
      new GlueSchemaPropertiesMetadata();

  static final GlueTablePropertiesMetadata TABLE_PROPERTIES_METADATA =
      new GlueTablePropertiesMetadata();

  /**
   * Returns the short name of the Glue catalog.
   *
   * @return The short name of the catalog.
   */
  @Override
  public String shortName() {
    return "glue";
  }

  /**
   * Creates a new instance of {@link GlueCatalogOperations} with the provided configuration.
   *
   * @param config The configuration map for the Glue catalog operations.
   * @return A new instance of {@link GlueCatalogOperations}.
   */
  @Override
  protected CatalogOperations newOps(Map<String, String> config) {
    return new GlueCatalogOperations();
  }

  @Override
  public Capability newCapability() {
    return new GlueCatalogCapability();
  }

  @Override
  public PropertiesMetadata catalogPropertiesMetadata() throws UnsupportedOperationException {
    return CATALOG_PROPERTIES_METADATA;
  }

  @Override
  public PropertiesMetadata schemaPropertiesMetadata() throws UnsupportedOperationException {
    return SCHEMA_PROPERTIES_METADATA;
  }

  @Override
  public PropertiesMetadata tablePropertiesMetadata() throws UnsupportedOperationException {
    return TABLE_PROPERTIES_METADATA;
  }

  @Override
  public Map<String, String> propertiesWithCredentialProviders() {
    Map<String, String> props = super.propertiesWithCredentialProviders();
    // super() skips addCatalogSpecificCredentialProviders() when credential-providers is already
    // set, so the aws-* → s3-* key mapping never runs. Apply it unconditionally here so that
    // S3SecretKeyProvider.initialize() can read s3-access-key-id regardless of how the catalog
    // was configured.
    String accessKeyId = props.get(GlueConstants.AWS_ACCESS_KEY_ID);
    String secretAccessKey = props.get(GlueConstants.AWS_SECRET_ACCESS_KEY);
    if (StringUtils.isNotBlank(accessKeyId) && StringUtils.isNotBlank(secretAccessKey)) {
      props.putIfAbsent(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID, accessKeyId);
      props.putIfAbsent(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY, secretAccessKey);
    }
    return props;
  }

  @Override
  protected void addCatalogSpecificCredentialProviders(
      Map<String, String> properties, List<String> credentialProviders) {
    String accessKeyId = properties.get(GlueConstants.AWS_ACCESS_KEY_ID);
    String secretAccessKey = properties.get(GlueConstants.AWS_SECRET_ACCESS_KEY);
    if (StringUtils.isNotBlank(accessKeyId) && StringUtils.isNotBlank(secretAccessKey)) {
      properties.putIfAbsent(S3Properties.GRAVITINO_S3_ACCESS_KEY_ID, accessKeyId);
      properties.putIfAbsent(S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY, secretAccessKey);
    }
    addStorageCredentialProviders(properties, credentialProviders);
  }
}
