/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.iceberg.common.cache;

import java.io.Closeable;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * A cache interface for managing {@link TableMetadata} objects, providing methods for
 * initialization, retrieval, update, invalidation, and resource cleanup.
 */
public interface TableMetadataCache extends Closeable {
  /**
   * A no-op (dummy) implementation of {@link TableMetadataCache} that performs no actual caching.
   * Useful for scenarios where caching functionality is not required.
   */
  TableMetadataCache DUMMY =
      new TableMetadataCache() {
        @Override
        public void initialize(
            int capacity,
            int expireMinutes,
            Map<String, String> catalogProperties,
            SupportsMetadataLocation supportsMetadataLocation) {}

        @Override
        public void invalidate(TableIdentifier tableIdentifier) {}

        @Override
        public Optional<TableMetadata> getTableMetadata(TableIdentifier tableIdentifier) {
          return Optional.empty();
        }

        @Override
        public void updateTableMetadata(
            TableIdentifier tableIdentifier, TableMetadata tableMetadata) {}

        @Override
        public void close() {}
      };

  /**
   * Initializes the metadata cache with specified configuration.
   *
   * @param capacity Maximum number of entries the cache can hold
   * @param catalogProperties Catalog-specific properties that may influence cache behavior
   * @param supportsMetadataLocation Component to resolve the latest metadata location for
   *     validation
   */
  void initialize(
      int capacity,
      int expireMinutes,
      Map<String, String> catalogProperties,
      SupportsMetadataLocation supportsMetadataLocation);

  /**
   * Removes the cached metadata entry for the specified table from the cache.
   *
   * @param tableIdentifier Identifier of the table whose cached metadata should be invalidated
   */
  void invalidate(TableIdentifier tableIdentifier);

  /**
   * Retrieves the latest {@link TableMetadata} for the specified table.
   *
   * @param tableIdentifier Identifier of the table to retrieve metadata for
   * @return Cached {@link TableMetadata} wrapped in an {@link Optional}, or an empty {@link
   *     Optional} if no valid entry exists in the cache or the cache is not latest.
   */
  Optional<TableMetadata> getTableMetadata(TableIdentifier tableIdentifier);

  /**
   * Updates the cache with new {@link TableMetadata} for the specified table.
   *
   * @param tableIdentifier Identifier of the table to update metadata for
   * @param tableMetadata New {@link TableMetadata} to store in the cache
   */
  void updateTableMetadata(TableIdentifier tableIdentifier, TableMetadata tableMetadata);
}
