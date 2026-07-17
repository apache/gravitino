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

package org.apache.gravitino.credential;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.gravitino.credential.config.CredentialConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CredentialCache<T> implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(CredentialCache.class);

  // Calculates the credential expire time in the cache.
  static class CredentialExpireTimeCalculator<T> implements Expiry<T, Optional<Credential>> {

    private double credentialCacheExpireRatio;

    public CredentialExpireTimeCalculator(double credentialCacheExpireRatio) {
      this.credentialCacheExpireRatio = credentialCacheExpireRatio;
    }

    // Set expire time after add a credential in the cache.
    @Override
    public long expireAfterCreate(T key, Optional<Credential> credential, long currentTime) {
      // Do not cache the absence of a credential, expire it immediately.
      if (!credential.isPresent()) {
        return 0;
      }

      long credentialExpireTime = credential.get().expireTimeInMs();
      long timeToExpire = credentialExpireTime - System.currentTimeMillis();
      if (timeToExpire <= 0) {
        return 0;
      }

      timeToExpire = (long) (timeToExpire * credentialCacheExpireRatio);
      return TimeUnit.MILLISECONDS.toNanos(timeToExpire);
    }

    // Not change expire time after update credential, this should not happen.
    @Override
    public long expireAfterUpdate(
        T key, Optional<Credential> value, long currentTime, long currentDuration) {
      return currentDuration;
    }

    // Not change expire time after read credential.
    @Override
    public long expireAfterRead(
        T key, Optional<Credential> value, long currentTime, long currentDuration) {
      return currentDuration;
    }
  }

  private Cache<T, Optional<Credential>> credentialCache;

  public void initialize(Map<String, String> catalogProperties) {
    CredentialConfig credentialConfig = new CredentialConfig(catalogProperties);
    long cacheSize = credentialConfig.get(CredentialConfig.CREDENTIAL_CACHE_MAX_SIZE);
    double cacheExpireRatio = credentialConfig.get(CredentialConfig.CREDENTIAL_CACHE_EXPIRE_RATIO);

    this.credentialCache =
        Caffeine.newBuilder()
            .expireAfter(new CredentialExpireTimeCalculator(cacheExpireRatio))
            .maximumSize(cacheSize)
            .removalListener(
                (cacheKey, credential, cause) ->
                    LOG.info(
                        "Removed credential cache entry, cacheKey={}, cause={}", cacheKey, cause))
            .build();
  }

  public Optional<Credential> getCredential(
      T cacheKey, Function<T, Optional<Credential>> credentialSupplier) {
    // Store a non-null Optional in the cache so the Caffeine mapping function never returns null,
    // keeping Cache#get atomic while supporting the "no credential available" case.
    return credentialCache.get(cacheKey, key -> credentialSupplier.apply(cacheKey));
  }

  @Override
  public void close() throws IOException {
    if (credentialCache != null) {
      credentialCache.invalidateAll();
      credentialCache = null;
    }
  }
}
