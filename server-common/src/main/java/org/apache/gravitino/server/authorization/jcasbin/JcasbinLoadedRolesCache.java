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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.cache.GravitinoCache;
import org.casbin.jcasbin.main.Enforcer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link GravitinoCache} of {@code roleId -> updated_at} that synchronously deletes the role's
 * JCasbin policies from both enforcers when a key is evicted (by TTL, size, or explicit
 * invalidate).
 *
 * <p>Uses a raw Caffeine cache internally so it can attach a removal listener with {@code
 * executor(Runnable::run)} — eviction and policy cleanup must happen on the same thread, so the
 * {@link JcasbinAuthorizer} never sees a role bound in the enforcer without a backing policy.
 */
class JcasbinLoadedRolesCache implements GravitinoCache<Long, Long> {

  private static final Logger LOG = LoggerFactory.getLogger(JcasbinLoadedRolesCache.class);

  private final Cache<Long, Long> cache;

  JcasbinLoadedRolesCache(long ttlMs, long maxSize, Enforcer allowEnforcer, Enforcer denyEnforcer) {
    this.cache =
        Caffeine.newBuilder()
            .expireAfterAccess(ttlMs, TimeUnit.MILLISECONDS)
            .maximumSize(maxSize)
            .executor(Runnable::run)
            .removalListener(
                (Long roleId, Long value, RemovalCause cause) -> {
                  LOG.info(
                      "Removed JCasbin loaded role cache entry, roleId={}, cause={}",
                      roleId,
                      cause);
                  if (roleId != null && cause != RemovalCause.REPLACED) {
                    allowEnforcer.deleteRole(String.valueOf(roleId));
                    denyEnforcer.deleteRole(String.valueOf(roleId));
                  }
                })
            .build();
  }

  @Override
  public Optional<Long> getIfPresent(Long key) {
    return Optional.ofNullable(cache.getIfPresent(key));
  }

  @Override
  public void put(Long key, Long value) {
    cache.put(key, value);
  }

  @Override
  public void invalidate(Long key) {
    cache.invalidate(key);
  }

  @Override
  public void invalidateAll() {
    cache.invalidateAll();
  }

  @Override
  public void invalidateByPrefix(String prefix) {
    // Role ids are Long keys, so prefix invalidation is not meaningful for this cache.
  }

  @Override
  public long size() {
    cache.cleanUp();
    return cache.estimatedSize();
  }

  @Override
  public void close() {
    cache.invalidateAll();
    cache.cleanUp();
  }
}
