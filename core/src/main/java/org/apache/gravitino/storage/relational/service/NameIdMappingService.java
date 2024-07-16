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
package org.apache.gravitino.storage.relational.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.gravitino.NameIdentifier;

public class NameIdMappingService implements Closeable {

  private static volatile NameIdMappingService instance;

  private Cache<NameIdentifier, Long> ident2IdCache;

  private NameIdMappingService() {
    this.ident2IdCache =
        Caffeine.newBuilder()
            .expireAfterAccess(24 * 3600 * 1000L /* 1 day */, TimeUnit.MILLISECONDS)
            .maximumSize(1000000)
            .initialCapacity(1000)
            .scheduler(
                Scheduler.forScheduledExecutorService(
                    new ScheduledThreadPoolExecutor(
                        1,
                        new ThreadFactoryBuilder()
                            .setDaemon(true)
                            .setNameFormat("ident-to-id-cleaner-%d")
                            .build())))
            .build();
  }

  public static NameIdMappingService getInstance() {
    if (instance == null) {
      synchronized (NameIdMappingService.class) {
        if (instance == null) {
          instance = new NameIdMappingService();
        }
      }
    }

    return instance;
  }

  public void put(NameIdentifier key, Long value) {
    ident2IdCache.put(key, value);
  }

  public Long get(NameIdentifier key, Function<NameIdentifier, Long> mappingFunction) {
    return ident2IdCache.get(key, mappingFunction);
  }

  public Long get(NameIdentifier key) {
    return ident2IdCache.getIfPresent(key);
  }

  public NameIdentifier getById(Long value, Function<Long, NameIdentifier> mappingFunction) {
    synchronized (this) {
      BiMap<NameIdentifier, Long> map = HashBiMap.create(ident2IdCache.asMap());
      if (map.containsValue(value)) {
        return map.inverse().get(value);
      } else {
        NameIdentifier nameIdentifier = mappingFunction.apply(value);
        if (nameIdentifier != null) {
          ident2IdCache.put(nameIdentifier, value);
        }
        return nameIdentifier;
      }
    }
  }

  public NameIdentifier getById(Long value) {
    synchronized (this) {
      BiMap<NameIdentifier, Long> map = HashBiMap.create(ident2IdCache.asMap());
      if (map.containsValue(value)) {
        return map.inverse().get(value);
      }
      return null;
    }
  }

  public void invalidate(NameIdentifier key) {
    ident2IdCache.invalidate(key);
  }

  public void invalidateWithPrefix(NameIdentifier nameIdentifier) {
    ident2IdCache.asMap().keySet().stream()
        .filter(k -> k.toString().startsWith(nameIdentifier.toString()))
        .forEach(ident2IdCache::invalidate);
  }

  @Override
  public void close() throws IOException {
    if (ident2IdCache != null) {
      ident2IdCache.invalidateAll();
      ident2IdCache.cleanUp();
    }
  }
}
