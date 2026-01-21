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
package org.apache.gravitino.trino.connector.catalog.bigquery;

import io.trino.spi.session.PropertyMetadata;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.catalog.property.PropertyConverter;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorAdapter;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import org.apache.gravitino.trino.connector.catalog.CatalogPropertyConverter;
import org.apache.gravitino.trino.connector.catalog.HasPropertyMeta;
import org.apache.gravitino.trino.connector.metadata.GravitinoCatalog;

/** Adapter for BigQuery catalog integration with Trino. */
public class BigQueryConnectorAdapter implements CatalogConnectorAdapter {

  private static final String CONNECTOR_BIGQUERY = "bigquery";
  private static final String PROPERTY_PROJECT_ID = "project-id";

  private final PropertyConverter catalogConverter;
  private final HasPropertyMeta propertyMetadata;

  public BigQueryConnectorAdapter() {
    this.catalogConverter = new CatalogPropertyConverter();
    this.propertyMetadata = new BigQueryPropertyMeta();
  }

  @Override
  public Map<String, String> buildInternalConnectorConfig(GravitinoCatalog catalog)
      throws Exception {
    Map<String, String> config =
        new HashMap<>(catalogConverter.gravitinoToEngineProperties(catalog.getProperties()));
    config.put("bigquery.project-id", catalog.getRequiredProperty(PROPERTY_PROJECT_ID));
    // config.put("bigquery.credentials-key", catalog.getRequiredProperty("jdbc-user"));
    config.put("bigquery.credentials-file", catalog.getRequiredProperty("jdbc-password"));
    return config;
  }

  @Override
  public String internalConnectorName() {
    return CONNECTOR_BIGQUERY;
  }

  @Override
  public CatalogConnectorMetadataAdapter getMetadataAdapter() {
    return new BigQueryMetadataAdapter(
        getSchemaProperties(), getTableProperties(), getColumnProperties());
  }

  @Override
  public List<PropertyMetadata<?>> getTableProperties() {
    return propertyMetadata.getTablePropertyMetadata();
  }

  @Override
  public List<PropertyMetadata<?>> getColumnProperties() {
    return propertyMetadata.getColumnPropertyMetadata();
  }

  @Override
  public List<PropertyMetadata<?>> getSchemaProperties() {
    return propertyMetadata.getSchemaPropertyMetadata();
  }
}
