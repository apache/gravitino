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
package org.apache.gravitino.trino.connector.catalog.jdbc.postgresql;

import io.trino.spi.session.PropertyMetadata;
import java.util.List;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;

/** Transforming gravitino PostgreSQL metadata to Trino. */
public class PostgreSQLMetadataAdapter extends CatalogConnectorMetadataAdapter {

  /**
   * Constructs a new PostgreSQLMetadataAdapter.
   *
   * @param schemaProperties The list of schema property metadata
   * @param tableProperties The list of table property metadata
   * @param columnProperties The list of column property metadata
   */
  public PostgreSQLMetadataAdapter(
      List<PropertyMetadata<?>> schemaProperties,
      List<PropertyMetadata<?>> tableProperties,
      List<PropertyMetadata<?>> columnProperties) {

    super(schemaProperties, tableProperties, columnProperties, new PostgreSQLDataTypeTransformer());
  }
}
