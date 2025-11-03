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

import java.util.Optional;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class implementing {@link TableMetadataCache} that provides core metadata caching
 * functionality with validation of metadata location against the latest version.
 */
public abstract class BaseTableMetadataCache implements TableMetadataCache {

  public static final Logger LOG = LoggerFactory.getLogger(BaseTableMetadataCache.class);
  /** Component to retrieve the latest metadata location for a table, used for validation. */
  private SupportsMetadataLocation supportsMetadataLocation;

  /**
   * Abstract method to retrieve cached {@link TableMetadata} for a table. Subclasses must implement
   * this to provide actual cache retrieval logic.
   *
   * @param tableIdentifier Identifier of the table to retrieve cached metadata for
   * @return Cached {@link TableMetadata} wrapped in an {@link Optional}, or an empty {@link
   *     Optional} if not found in cache
   */
  protected abstract Optional<TableMetadata> doGetTableMetadata(TableIdentifier tableIdentifier);

  protected void initialize(SupportsMetadataLocation supportsMetadataLocation) {
    this.supportsMetadataLocation = supportsMetadataLocation;
  }

  /**
   * Retrieves and validates cached table metadata by comparing the cached metadata's location with
   * the latest known location. Invalidates the cache if locations mismatch.
   *
   * @param tableIdentifier Identifier of the table to retrieve metadata for
   * @return Valid {@link TableMetadata} if cache is valid and locations match; {@code null} if
   *     cache miss, location is invalid, or locations mismatch (after invalidation)
   */
  @Override
  public Optional<TableMetadata> getTableMetadata(TableIdentifier tableIdentifier) {
    Optional<TableMetadata> tableMetadataOptional = doGetTableMetadata(tableIdentifier);
    if (!tableMetadataOptional.isPresent()) {
      return Optional.empty();
    }
    TableMetadata tableMetadata = tableMetadataOptional.get();
    String latestLocation = supportsMetadataLocation.metadataLocation(tableIdentifier);
    if (latestLocation == null) {
      return Optional.empty();
    }
    if (latestLocation.equals(tableMetadata.metadataFileLocation())) {
      return Optional.of(tableMetadata);
    }

    LOG.debug(
        "The cached table metadata is not latest, table identifier: {}, "
            + "table metadata location in cache: {}, latest metadata location: {}",
        tableIdentifier,
        tableMetadata.metadataFileLocation(),
        latestLocation);
    invalidate(tableIdentifier);
    return Optional.empty();
  }
}
