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
package org.apache.gravitino.trino.connector.catalog.jdbc;

import io.trino.spi.session.PropertyMetadata;
import java.util.List;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import org.apache.gravitino.trino.connector.util.GeneralDataTypeTransformer;

/** Transforming gravitino jdbc connector metadata to Trino. */
public class JdbcConnectorMetadataAdapter extends CatalogConnectorMetadataAdapter {

  private static final String MERGE_ROW_ID = "$merge_row_id";

  /**
   * Constructs a new JdbcConnectorMetadataAdapter.
   *
   * @param schemaProperties The list of schema properties supported by this catalog connector
   * @param tableProperties The list of table properties supported by this catalog connector
   * @param columnProperties The list of column properties supported by this catalog connector
   * @param dataTypeTransformer The data type transformer used to convert between Gravitino and
   *     Trino types
   */
  protected JdbcConnectorMetadataAdapter(
      List<PropertyMetadata<?>> schemaProperties,
      List<PropertyMetadata<?>> tableProperties,
      List<PropertyMetadata<?>> columnProperties,
      GeneralDataTypeTransformer dataTypeTransformer) {
    super(schemaProperties, tableProperties, columnProperties, dataTypeTransformer);
  }

  /**
   * Get the column handle name that will generate row IDs for the merge operation.
   *
   * @return the column handle name for row IDs
   */
  @Override
  public String getMergeRowIdColumnHandleName() {
    return MERGE_ROW_ID;
  }
}
