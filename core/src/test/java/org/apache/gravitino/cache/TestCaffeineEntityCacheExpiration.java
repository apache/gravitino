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
package org.apache.gravitino.cache;

import com.github.benmanes.caffeine.cache.Policy;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests the expiration policy of {@link CaffeineEntityCache}.
 *
 * <p>The cache must expire entries a fixed time after they were written, never after they were last
 * read. In a multi-node deployment the TTL is the safety net for a cross-node invalidation that
 * never arrives; an access-based TTL would keep a hot stale entry alive forever.
 */
public class TestCaffeineEntityCacheExpiration {

  @Test
  void testExpiresAfterWriteNotAfterAccess() {
    Config config = new Config() {};
    config.set(Configs.CACHE_EXPIRATION_TIME, 600_000L);

    CaffeineEntityCache cache = new CaffeineEntityCache(config);
    Policy<EntityCacheKey, ?> policy = cache.getCacheData().policy();

    Assertions.assertTrue(policy.expireAfterWrite().isPresent());
    Assertions.assertEquals(
        Duration.ofMillis(600_000L), policy.expireAfterWrite().get().getExpiresAfter());
    Assertions.assertFalse(
        policy.expireAfterAccess().isPresent(),
        "reads must not extend the lifetime of an entry: a stale entry that keeps being read "
            + "would otherwise never expire");
    Assertions.assertEquals(
        600_000L, policy.expireAfterWrite().get().getExpiresAfter(TimeUnit.MILLISECONDS));
  }

  @Test
  void testZeroExpirationDisablesTimeBasedEviction() {
    Config config = new Config() {};
    config.set(Configs.CACHE_EXPIRATION_TIME, 0L);

    CaffeineEntityCache cache = new CaffeineEntityCache(config);
    Policy<EntityCacheKey, ?> policy = cache.getCacheData().policy();

    Assertions.assertFalse(policy.expireAfterWrite().isPresent());
    Assertions.assertFalse(policy.expireAfterAccess().isPresent());
  }
}
