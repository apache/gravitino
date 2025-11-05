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
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalTableMetadataCache extends BaseTableMetadataCache {
  public static final Logger LOG = LoggerFactory.getLogger(LocalTableMetadataCache.class);
  private Cache<TableIdentifier, TableMetadata> tableMetadataCache;

  @Override
  public void initialize(
      int capacity,
      int expireMinutes,
      Map<String, String> catalogProperties,
      SupportsMetadataLocation supportsMetadataLocation) {
    super.initialize(supportsMetadataLocation);
    this.tableMetadataCache =
        Caffeine.newBuilder()
            .maximumSize(capacity)
            .expireAfterAccess(expireMinutes, TimeUnit.MINUTES)
            // control the cache size not exceed the cache capacity
            .executor(Runnable::run)
            .build();
  }

  @Override
  public void invalidate(TableIdentifier tableIdentifier) {
    LOG.debug("Invalidate table cache, table identifier: {}", tableIdentifier);
    tableMetadataCache.invalidate(tableIdentifier);
  }

  @Override
  public void updateTableMetadata(TableIdentifier tableIdentifier, TableMetadata tableMetadata) {
    LOG.debug(
        "Update table cache, table identifier: {}, table metadata location: {}",
        tableIdentifier,
        tableMetadata.metadataFileLocation());
    tableMetadataCache.put(tableIdentifier, tableMetadata);
  }

  @Override
  public void close() throws IOException {
    LOG.info("Close Iceberg table metadata cache");
    if (tableMetadataCache != null) {
      tableMetadataCache.invalidateAll();
      tableMetadataCache.cleanUp();
    }
  }

  @Override
  protected Optional<TableMetadata> doGetTableMetadata(TableIdentifier tableIdentifier) {
    return Optional.ofNullable(tableMetadataCache.getIfPresent(tableIdentifier));
  }

  @VisibleForTesting
  int size() {
    return tableMetadataCache.asMap().size();
  }
}
