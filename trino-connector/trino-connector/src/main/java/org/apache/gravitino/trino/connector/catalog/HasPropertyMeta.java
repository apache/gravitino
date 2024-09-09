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

  /** Property metadata for schema */
  default List<PropertyMetadata<?>> getSchemaPropertyMetadata() {
    return ImmutableList.of();
  }

  /** Property metadata for table */
  default List<PropertyMetadata<?>> getTablePropertyMetadata() {
    return ImmutableList.of();
  }

  /** Property metadata for column */
  default List<PropertyMetadata<?>> getColumnPropertyMetadata() {
    return ImmutableList.of();
  }

  default List<PropertyMetadata<?>> getCatalogPropertyMeta() {
    return ImmutableList.of();
  }
}
