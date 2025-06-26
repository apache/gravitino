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
package org.apache.gravitino.trino.connector.catalog;

import static java.util.Collections.emptyList;

import io.trino.spi.session.PropertyMetadata;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.trino.connector.metadata.GravitinoCatalog;

/**
 * This interface is used to handle different parts of connectors from different catalog connectors.
 */
public interface CatalogConnectorAdapter {

  /** @return TableProperties list that used to validate table properties. */
  default List<PropertyMetadata<?>> getTableProperties() {
    return emptyList();
  }

  /**
   * Builds the internal connector configuration required.
   *
   * @param catalog the Gravitino catalog to build configuration for
   * @return a map of configuration properties for the internal Trino connector
   * @throws Exception if the configuration cannot be built
   */
  Map<String, String> buildInternalConnectorConfig(GravitinoCatalog catalog) throws Exception;

  /** @return Return internal connector name with Trino. */
  String internalConnectorName();

  /** @return SchemaProperties list that used to validate schema properties. */
  default List<PropertyMetadata<?>> getSchemaProperties() {
    return emptyList();
  }

  /** @return Return MetadataAdapter for special catalog connector. */
  CatalogConnectorMetadataAdapter getMetadataAdapter();

  /** @return ColumnProperties list that used to validate column properties. */
  default List<PropertyMetadata<?>> getColumnProperties() {
    return emptyList();
  }
}
