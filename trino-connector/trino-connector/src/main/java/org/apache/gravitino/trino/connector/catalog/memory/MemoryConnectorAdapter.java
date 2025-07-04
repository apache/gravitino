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
package org.apache.gravitino.trino.connector.catalog.memory;

import io.trino.spi.session.PropertyMetadata;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.catalog.property.PropertyConverter;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorAdapter;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import org.apache.gravitino.trino.connector.catalog.CatalogPropertyConverter;
import org.apache.gravitino.trino.connector.catalog.HasPropertyMeta;
import org.apache.gravitino.trino.connector.metadata.GravitinoCatalog;

/**
 * Support Trino Memory connector for testing. Transforming Memory connector configuration and
 * components into Apache Gravitino connector.
 */
public class MemoryConnectorAdapter implements CatalogConnectorAdapter {

  private static final String CONNECTOR_MEMORY = "memory";
  private final PropertyConverter catalogConverter;
  private final HasPropertyMeta propertyMetadata;

  /**
   * Constructs a new MemoryConnectorAdapter. Initializes the property metadata for handling
   * memory-specific configurations.
   */
  public MemoryConnectorAdapter() {
    this.catalogConverter = new CatalogPropertyConverter();
    this.propertyMetadata = new MemoryPropertyMeta();
  }

  @Override
  public Map<String, String> buildInternalConnectorConfig(GravitinoCatalog catalog) {
    return catalogConverter.gravitinoToEngineProperties(catalog.getProperties());
  }

  @Override
  public String internalConnectorName() {
    return CONNECTOR_MEMORY;
  }

  @Override
  public CatalogConnectorMetadataAdapter getMetadataAdapter() {
    return new MemoryMetadataAdapter(
        getTableProperties(), Collections.emptyList(), Collections.emptyList());
  }

  @Override
  public List<PropertyMetadata<?>> getTableProperties() {
    return propertyMetadata.getTablePropertyMetadata();
  }
}
