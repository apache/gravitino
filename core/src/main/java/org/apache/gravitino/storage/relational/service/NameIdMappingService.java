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
import com.google.common.base.Objects;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.NameIdentifier;

public class NameIdMappingService implements Closeable {

  private static volatile NameIdMappingService instance;

  /**
   * Cache to store the mapping between the entity identifier and the entity id.
   *
   * <p>Note: both the key and value are unique, so we can use BiMap here and get the key by value.
   */
  private Cache<EntityIdentifier, Long> ident2IdCache;

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

  public static class EntityIdentifier {
    final NameIdentifier ident;
    final EntityType type;

    private EntityIdentifier(NameIdentifier ident, EntityType type) {
      this.ident = ident;
      this.type = type;
    }

    public static EntityIdentifier of(NameIdentifier ident, EntityType type) {
      return new EntityIdentifier(ident, type);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof EntityIdentifier)) {
        return false;
      }
      EntityIdentifier that = (EntityIdentifier) o;
      return Objects.equal(ident, that.ident) && type == that.type;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(ident, type);
    }
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

  public void put(EntityIdentifier key, Long value) {
    ident2IdCache.put(key, value);
  }

  public Long get(EntityIdentifier key, Function<EntityIdentifier, Long> mappingFunction) {
    return ident2IdCache.get(key, mappingFunction);
  }

  public Long get(EntityIdentifier key) {
    return ident2IdCache.getIfPresent(key);
  }

  /**
   * Get the entity identifier by the entity id.
   *
   * @param value the entity id
   * @param mappingFunction the function to get the entity identifier by the entity id
   * @return the entity identifier
   */
  public EntityIdentifier getById(Long value, Function<Long, EntityIdentifier> mappingFunction) {
    synchronized (this) {
      BiMap<EntityIdentifier, Long> map = HashBiMap.create(ident2IdCache.asMap());
      if (map.containsValue(value)) {
        return map.inverse().get(value);
      } else {
        EntityIdentifier nameIdentifier = mappingFunction.apply(value);
        if (nameIdentifier != null) {
          ident2IdCache.put(nameIdentifier, value);
        }
        return nameIdentifier;
      }
    }
  }

  /**
   * Get the entity identifier by the entity id.
   *
   * @param value the entity id
   * @return the entity identifier
   */
  public EntityIdentifier getById(Long value) {
    synchronized (this) {
      BiMap<EntityIdentifier, Long> map = HashBiMap.create(ident2IdCache.asMap());
      if (map.containsValue(value)) {
        return map.inverse().get(value);
      }
      return null;
    }
  }

  public void invalidate(EntityIdentifier key) {
    ident2IdCache.invalidate(key);
  }

  public void invalidateWithPrefix(EntityIdentifier nameIdentifier) {
    ident2IdCache.asMap().keySet().stream()
        .filter(k -> k.ident.toString().startsWith(nameIdentifier.ident.toString()))
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
