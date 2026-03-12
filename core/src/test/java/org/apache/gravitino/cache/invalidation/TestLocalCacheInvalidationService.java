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

package org.apache.gravitino.cache.invalidation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.junit.jupiter.api.Test;

public class TestLocalCacheInvalidationService {

  @Test
  void testPublishAndHandleEvent() {
    LocalCacheInvalidationService service = new LocalCacheInvalidationService(true);
    AtomicInteger handled = new AtomicInteger(0);
    service.registerHandler(CacheDomain.CATALOG, "handler", event -> handled.incrementAndGet());

    service.publish(
        CacheInvalidationEvent.of(
            CacheDomain.CATALOG,
            CacheInvalidationOperation.INVALIDATE_KEY,
            CatalogCacheInvalidationKey.of(NameIdentifier.of("metalake", "catalog")),
            service.localNodeId()));

    assertEquals(1, handled.get());
    assertEquals(
        1L,
        service.getPublishedCount(CacheDomain.CATALOG, CacheInvalidationOperation.INVALIDATE_KEY));
    assertEquals(
        1L,
        service.getHandledCount(CacheDomain.CATALOG, CacheInvalidationOperation.INVALIDATE_KEY));
  }

  @Test
  void testDisableEventDispatch() {
    LocalCacheInvalidationService service = new LocalCacheInvalidationService(false);
    AtomicInteger handled = new AtomicInteger(0);
    service.registerHandler(CacheDomain.ENTITY, "handler", event -> handled.incrementAndGet());

    service.publish(
        CacheInvalidationEvent.of(
            CacheDomain.ENTITY,
            CacheInvalidationOperation.INVALIDATE_KEY,
            EntityCacheInvalidationKey.of(
                NameIdentifier.of("metalake", "catalog"), Entity.EntityType.CATALOG, null),
            service.localNodeId()));

    assertEquals(0, handled.get());
    assertEquals(
        0L,
        service.getPublishedCount(CacheDomain.ENTITY, CacheInvalidationOperation.INVALIDATE_KEY));
  }

  @Test
  void testUnregisterHandler() {
    LocalCacheInvalidationService service = new LocalCacheInvalidationService(true);
    AtomicInteger handled = new AtomicInteger(0);
    service.registerHandler(CacheDomain.AUTH_ROLE, "handler", event -> handled.incrementAndGet());
    service.unregisterHandler(CacheDomain.AUTH_ROLE, "handler");

    service.publish(
        CacheInvalidationEvent.of(
            CacheDomain.AUTH_ROLE,
            CacheInvalidationOperation.INVALIDATE_KEY,
            RoleCacheInvalidationKey.of(1L),
            service.localNodeId()));

    assertEquals(0, handled.get());
    assertTrue(service.localNodeId().contains("-"));
  }
}
