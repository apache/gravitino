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
package org.apache.gravitino.storage.relational;

import java.util.List;
import java.util.Locale;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.cache.SupportsEntityCacheInvalidation;
import org.apache.gravitino.storage.relational.po.cache.EntityChangeRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Invalidates local table entities when this server observes a peer's table change log. */
final class TableEntityCacheChangeListener implements EntityChangeLogListener {

  private static final Logger LOG = LoggerFactory.getLogger(TableEntityCacheChangeListener.class);

  private final SupportsEntityCacheInvalidation cacheInvalidation;

  TableEntityCacheChangeListener(SupportsEntityCacheInvalidation cacheInvalidation) {
    this.cacheInvalidation = cacheInvalidation;
  }

  @Override
  public void onEntityChange(List<EntityChangeRecord> changes) {
    for (EntityChangeRecord change : changes) {
      if (change.getEntityType() == null
          || !Entity.EntityType.TABLE.name().equals(change.getEntityType().toUpperCase(Locale.ROOT))
          || StringUtils.isBlank(change.getFullName())) {
        continue;
      }

      try {
        cacheInvalidation.invalidateEntityCache(
            NameIdentifier.parse(change.getFullName()), Entity.EntityType.TABLE);
      } catch (RuntimeException e) {
        LOG.warn("Failed to invalidate table cache from change log: {}", change.getFullName(), e);
      }
    }
  }
}
