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
import java.util.List;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import org.apache.gravitino.trino.connector.util.GeneralDataTypeTransformer;

/** Support Trino memory connector for testing. */
public class MemoryMetadataAdapter extends CatalogConnectorMetadataAdapter {

  /**
   * Constructs a new MemoryMetadataAdapter.
   *
   * @param schemaProperties the list of schema property metadata
   * @param tableProperties the list of table property metadata
   * @param columnProperties the list of column property metadata
   */
  public MemoryMetadataAdapter(
      List<PropertyMetadata<?>> schemaProperties,
      List<PropertyMetadata<?>> tableProperties,
      List<PropertyMetadata<?>> columnProperties) {

    super(schemaProperties, tableProperties, columnProperties, new GeneralDataTypeTransformer());
  }
}
