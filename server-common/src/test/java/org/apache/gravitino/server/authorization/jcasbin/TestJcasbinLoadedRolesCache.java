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
package org.apache.gravitino.server.authorization.jcasbin;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for the {@code roleId -> updated_at} cache that owns role permission policies. */
public class TestJcasbinLoadedRolesCache {

  private static final long ROLE_ID = 42L;

  @Test
  public void testTtlIsWriteBasedSoReadsCannotKeepAnEntryAlive() throws Exception {
    // versionCheckAndLoadRoles probes this cache on every request that carries the role. Under an
    // access-based TTL those probes renew the entry, so on a node under steady traffic the entry
    // never expires. That is what turns a lost policy load into a permanent authorization failure:
    // each denied request renews the very entry that tells the version check to skip the reload.
    long ttlMs = 150L;
    List<Long> cleaned = new ArrayList<>();
    JcasbinLoadedRolesCache cache = new JcasbinLoadedRolesCache(ttlMs, 100L, cleaned::add);
    cache.put(ROLE_ID, 1L);

    // Read the entry far more often than the TTL, the way a hot role is probed in production.
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(ttlMs * 4);
    while (System.nanoTime() < deadline) {
      cache.getIfPresent(ROLE_ID);
      Thread.sleep(10L);
    }

    Assertions.assertFalse(
        cache.getIfPresent(ROLE_ID).isPresent(),
        "repeated reads must not keep the entry alive past its TTL");
    // size() runs Caffeine's maintenance, which delivers any pending removal notification.
    cache.size();
    Assertions.assertTrue(
        cleaned.contains(ROLE_ID), "expiring the entry must clear the role's policies");
    cache.close();
  }

  @Test
  public void testReplacingAnEntryDoesNotClearPolicies() {
    // A refresh writes the new version over the old one. Treating that as a removal would delete
    // the policies the refresh just loaded.
    List<Long> cleaned = new ArrayList<>();
    JcasbinLoadedRolesCache cache = new JcasbinLoadedRolesCache(60_000L, 100L, cleaned::add);

    cache.put(ROLE_ID, 1L);
    cache.put(ROLE_ID, 2L);
    cache.size();

    Assertions.assertTrue(cleaned.isEmpty(), "replacing a value must not clear the role policies");
    Assertions.assertEquals(2L, cache.getIfPresent(ROLE_ID).orElse(null));

    cache.invalidate(ROLE_ID);
    Assertions.assertTrue(
        cleaned.contains(ROLE_ID), "explicit invalidation must clear the role policies");
    cache.close();
  }
}
