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

import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class implementing {@link MetadataCache} that provides core metadata caching
 * functionality with validation of metadata location against the latest version.
 */
public abstract class BaseMetadataCache implements MetadataCache {

  public static final Logger LOG = LoggerFactory.getLogger(BaseMetadataCache.class);
  /** Component to retrieve the latest metadata location for a table, used for validation. */
  private SupportsMetadataLocation supportsMetadataLocation;

  /**
   * Abstract method to retrieve cached {@link TableMetadata} for a table. Subclasses must implement
   * this to provide actual cache retrieval logic.
   *
   * @param tableIdentifier Identifier of the table to retrieve cached metadata for
   * @return Cached {@link TableMetadata}, or {@code null} if not found in cache
   */
  protected abstract TableMetadata doGetTableMetadata(TableIdentifier tableIdentifier);

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
  public TableMetadata getTableMetadata(TableIdentifier tableIdentifier) {
    TableMetadata tableMetadata = doGetTableMetadata(tableIdentifier);
    if (tableMetadata == null) {
      LOG.info("Table cache miss, table identifier: {}", tableIdentifier);
      return null;
    }
    String latestLocation = supportsMetadataLocation.metadataLocation(tableIdentifier);
    if (latestLocation == null) {
      LOG.info("Table metadata location is null, table identifier: {}", tableIdentifier);
      return null;
    }
    if (latestLocation.equals(tableMetadata.metadataFileLocation())) {
      LOG.info(
          "Table metadata location match, table identifier: {}, table metadata location: {}",
          tableIdentifier,
          tableMetadata.metadataFileLocation());
      return tableMetadata;
    }

    LOG.info(
        "Table metadata location not match, table identifier: {}, table metadata location: {}",
        tableIdentifier,
        tableMetadata.metadataFileLocation());
    invalidate(tableIdentifier);
    return null;
  }
}
