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

import com.google.common.collect.ImmutableList;
import io.trino.spi.session.PropertyMetadata;
import java.util.List;

/** Interface for adding property metadata to the catalogs */
public interface HasPropertyMeta {

  /**
   * Get the property metadata for a specific schema.
   *
   * @return a list of property metadata for schemas
   */
  default List<PropertyMetadata<?>> getSchemaPropertyMetadata() {
    return ImmutableList.of();
  }

  /**
   * Gets the property metadata for tables.
   *
   * @return a list of property metadata for tables
   */
  default List<PropertyMetadata<?>> getTablePropertyMetadata() {
    return ImmutableList.of();
  }

  /**
   * Gets the property metadata for columns.
   *
   * @return a list of property metadata for columns
   */
  default List<PropertyMetadata<?>> getColumnPropertyMetadata() {
    return ImmutableList.of();
  }

  /**
   * Gets the property metadata for catalogs.
   *
   * @return a list of property metadata for catalogs
   */
  default List<PropertyMetadata<?>> getCatalogPropertyMeta() {
    return ImmutableList.of();
  }
}
