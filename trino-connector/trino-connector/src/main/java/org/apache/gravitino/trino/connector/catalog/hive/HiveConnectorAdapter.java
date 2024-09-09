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
package org.apache.gravitino.trino.connector.catalog.hive;

import io.trino.spi.session.PropertyMetadata;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.catalog.property.PropertyConverter;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorAdapter;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import org.apache.gravitino.trino.connector.catalog.HasPropertyMeta;
import org.apache.gravitino.trino.connector.metadata.GravitinoCatalog;

/**
 * Transforming Apache Hive connector configuration and components into Apache Gravitino connector.
 */
public class HiveConnectorAdapter implements CatalogConnectorAdapter {

  private static final String CONNECTOR_HIVE = "hive";
  private final HasPropertyMeta propertyMetadata;
  private final PropertyConverter catalogConverter;

  public HiveConnectorAdapter() {
    this.propertyMetadata = new HivePropertyMeta();
    this.catalogConverter = new HiveCatalogPropertyConverter();
  }

  @Override
  public Map<String, String> buildInternalConnectorConfig(GravitinoCatalog catalog)
      throws Exception {
    Map<String, String> config = new HashMap<>();
    config.put("hive.metastore.uri", catalog.getRequiredProperty("metastore.uris"));
    config.put("hive.security", "allow-all");
    Map<String, String> trinoProperty =
        catalogConverter.gravitinoToEngineProperties(catalog.getProperties());
    config.putAll(trinoProperty);
    return config;
  }

  @Override
  public String internalConnectorName() {
    return CONNECTOR_HIVE;
  }

  @Override
  public List<PropertyMetadata<?>> getTableProperties() {
    return propertyMetadata.getTablePropertyMetadata();
  }

  @Override
  public List<PropertyMetadata<?>> getSchemaProperties() {
    return propertyMetadata.getSchemaPropertyMetadata();
  }

  @Override
  public CatalogConnectorMetadataAdapter getMetadataAdapter() {
    // TODO yuhui Need to improve schema table and column properties
    return new HiveMetadataAdapter(
        getSchemaProperties(), getTableProperties(), getColumnProperties());
  }

  @Override
  public List<PropertyMetadata<?>> getColumnProperties() {
    return Collections.emptyList();
  }
}
