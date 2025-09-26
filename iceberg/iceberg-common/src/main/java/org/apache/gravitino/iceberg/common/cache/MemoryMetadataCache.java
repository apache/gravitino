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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemoryMetadataCache implements MetadataCache {
  public static final Logger LOG = LoggerFactory.getLogger(MemoryMetadataCache.class);
  // private final Map<TableIdentifier, TableMetadata> tableMetadataCache = new
  // ConcurrentHashMap<>();
  private final Cache<TableIdentifier, TableMetadata> tableMetadataCache;
  private SupportsMetadataLocation supportsMetadataLocation;

  public MemoryMetadataCache() {
    this.tableMetadataCache = Caffeine.newBuilder().maximumSize(100).build();
  }

  @Override
  public void initialize(SupportsMetadataLocation supportsMetadataLocation) {
    this.supportsMetadataLocation = supportsMetadataLocation;
  }

  @Override
  public void invalidate(TableIdentifier tableIdentifier) {
    LOG.info("Invalidate table cache, table identifier: {}", tableIdentifier);
    tableMetadataCache.invalidate(tableIdentifier);
  }

  @Override
  public TableMetadata getTableMetadata(TableIdentifier tableIdentifier) {
    TableMetadata tableMetadata = tableMetadataCache.getIfPresent(tableIdentifier);
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

  @Override
  public void updateTableMetadata(TableIdentifier tableIdentifier, TableMetadata tableMetadata) {
    LOG.info(
        "Update table cache, table identifier: {}, table metadata location: {}",
        tableIdentifier,
        tableMetadata.metadataFileLocation());
    tableMetadataCache.put(tableIdentifier, tableMetadata);
  }
}
