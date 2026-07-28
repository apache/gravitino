/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.iceberg.service.deletion;

import javax.annotation.Nullable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.cache.SupportsEntityCacheInvalidation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Invalidates table entities cached outside the lifecycle's relational transaction. */
final class IcebergTableCacheInvalidator {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergTableCacheInvalidator.class);

  @Nullable private final SupportsEntityCacheInvalidation invalidationOverride;

  IcebergTableCacheInvalidator() {
    this(null);
  }

  IcebergTableCacheInvalidator(@Nullable SupportsEntityCacheInvalidation invalidationOverride) {
    this.invalidationOverride = invalidationOverride;
  }

  /** Invalidates one table after a deletion-lifecycle transaction has committed. */
  void invalidate(NameIdentifier identifier) {
    try {
      SupportsEntityCacheInvalidation invalidation = invalidationOverride;
      if (invalidation == null) {
        EntityStore store = GravitinoEnv.getInstance().entityStore();
        if (!(store instanceof SupportsEntityCacheInvalidation)) {
          return;
        }
        invalidation = (SupportsEntityCacheInvalidation) store;
      }

      invalidation.invalidateEntityCache(identifier, Entity.EntityType.TABLE);
    } catch (RuntimeException e) {
      // The relational mutation is already durable. Cache eviction is best effort and must not
      // turn a committed DELETE into a failed HTTP response.
      LOG.warn(
          "Failed to invalidate the local table cache after a lifecycle mutation: {}",
          identifier,
          e);
    }
  }
}
