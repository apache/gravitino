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
package org.apache.gravitino.trino.connector.catalog.iceberg;

import static java.util.Collections.emptyList;

import io.trino.spi.session.PropertyMetadata;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.catalog.property.PropertyConverter;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorAdapter;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import org.apache.gravitino.trino.connector.metadata.GravitinoCatalog;

/**
 * Transforming Apache Iceberg connector configuration and components into Apache Gravitino
 * connector.
 */
public class IcebergConnectorAdapter implements CatalogConnectorAdapter {

  private static final int MIN_SUPPORTED_TRINO_VERSION = 435;
  private static final String CONNECTOR_ICEBERG = "iceberg";
  private final IcebergPropertyMeta propertyMetadata;
  private final PropertyConverter catalogConverter;

  /**
   * Constructs a new IcebergConnectorAdapter. Initializes the property metadata and catalog
   * converter for handling Iceberg-specific configurations.
   */
  public IcebergConnectorAdapter() {
    this.propertyMetadata = new IcebergPropertyMeta();
    this.catalogConverter = new IcebergCatalogPropertyConverter();
  }

  @Override
  public Map<String, String> buildInternalConnectorConfig(
      GravitinoCatalog catalog, Credential[] credentials) throws Exception {
    Map<String, String> config =
        new HashMap<>(catalogConverter.gravitinoToEngineProperties(catalog.getProperties()));
    IcebergCatalogPropertyConverter.applyCredentials(credentials, config);
    return config;
  }

  @Override
  public String internalConnectorName() {
    return CONNECTOR_ICEBERG;
  }

  @Override
  public CatalogConnectorMetadataAdapter getMetadataAdapter() {
    return getMetadataAdapter(MIN_SUPPORTED_TRINO_VERSION);
  }

  @Override
  public CatalogConnectorMetadataAdapter getMetadataAdapter(int trinoVersion) {
    // TODO yuhui Need to improve schema table and column properties
    return new IcebergMetadataAdapter(
        getSchemaProperties(), getTableProperties(), emptyList(), trinoVersion);
  }

  @Override
  public List<PropertyMetadata<?>> getTableProperties() {
    return propertyMetadata.getTablePropertyMetadata();
  }

  @Override
  public List<PropertyMetadata<?>> getSchemaProperties() {
    return propertyMetadata.getSchemaPropertyMetadata();
  }
}
