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
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.cache.GravitinoCache;

/**
 * A {@link GravitinoCache} of {@code roleId -> }{@link CachedRolePolicies}, the per-role privilege
 * index consulted on the authorization hot path.
 *
 * <p><b>Policy ownership:</b> before privilege policies moved into {@link CachedRolePolicies}, this
 * cache stored only {@code role_meta.updated_at}, while the corresponding {@code p(roleId, ...)}
 * policies lived separately in JCasbin enforcers. Eviction therefore needed a synchronous removal
 * listener to delete those orphaned {@code p} rows without deleting the independently managed
 * {@code g(user/group, roleId)} bindings.
 *
 * <p>Now each cache value owns both the version sentinel and the role's complete privilege index,
 * and the JCasbin enforcer owns only the {@code g} bindings; it contains no {@code p} policies.
 * Eviction discards the policy index together with its cache entry, leaving no second policy copy
 * to clean up. On the next request, {@link JcasbinAuthorizer} observes the cache miss, reloads the
 * role from the database, and rebuilds the index. A removal listener must not delete the role from
 * the enforcer because doing so would also remove valid {@code g} bindings whose lifecycle is
 * managed by the user/group role caches.
 *
 * <p>Unlike {@link org.apache.gravitino.cache.CaffeineGravitinoCache} this cache is <b>access</b>
 * based ({@code expireAfterAccess}): the index of a role that keeps being authorized against stays
 * hot instead of being reloaded from the DB every TTL. Correctness never relies on the TTL — {@link
 * JcasbinAuthorizer} version-validates each entry against {@code role_meta.updated_at} on every
 * read, so eviction by TTL, size, or explicit invalidation only frees memory and forces a rebuild.
 */
class JcasbinLoadedRolesCache implements GravitinoCache<Long, CachedRolePolicies> {

  private final Cache<Long, CachedRolePolicies> cache;

  JcasbinLoadedRolesCache(long ttlMs, long maxSize) {
    this.cache =
        Caffeine.newBuilder()
            .expireAfterAccess(ttlMs, TimeUnit.MILLISECONDS)
            .maximumSize(maxSize)
            .build();
  }

  @Override
  public Optional<CachedRolePolicies> getIfPresent(Long key) {
    return Optional.ofNullable(cache.getIfPresent(key));
  }

  @Override
  public void put(Long key, CachedRolePolicies value) {
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
