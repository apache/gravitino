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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;

/**
 * Lock-free, copy-on-write snapshot cache. All mutations build a new immutable Snapshot and publish
 * via CAS. Readers never block.
 */
public class SnapshotMetaCache extends BaseMetaCache {
  private final AtomicReference<Snapshot> snapshotRef;
  /** The singleton instance of SnapshotMetaCache */
  private static volatile SnapshotMetaCache INSTANCE;

  /** Resets the instance for testing purposes. */
  @VisibleForTesting
  static void resetForTest() {
    INSTANCE = null;
  }

  /**
   * Returns the instance of SnapshotMetaCache based on the cache configuration and entity store.
   *
   * @param cacheConfig The cache configuration
   * @param entityStore The entity store
   * @return The instance of {@link SnapshotMetaCache}
   */
  public static SnapshotMetaCache getInstance(CacheConfig cacheConfig, EntityStore entityStore) {
    if (INSTANCE == null) {
      synchronized (SnapshotMetaCache.class) {
        if (INSTANCE == null) {
          INSTANCE = new SnapshotMetaCache(cacheConfig, entityStore);
        }
      }
    }
    return INSTANCE;
  }

  /**
   * Returns the instance of SnapshotMetaCache based on the cache configuration.
   *
   * @param cacheConfig The cache configuration
   * @return The instance of {@link SnapshotMetaCache}
   */
  public static SnapshotMetaCache getInstance(CacheConfig cacheConfig) {
    return getInstance(cacheConfig, null);
  }

  private SnapshotMetaCache(CacheConfig cacheConfig, EntityStore store) {
    super(cacheConfig, store);
    this.snapshotRef = new AtomicReference<>(new Snapshot());
  }

  /** {@inheritDoc} */
  @Override
  protected void removeExpiredEntityFromDataCache(Entity entity) {}

  /** {@inheritDoc} */
  @Override
  public Entity getOrLoadMetadataById(Long id, Entity.EntityType type) {
    Pair<Long, Entity.EntityType> key = Pair.of(id, type);
    Snapshot snap = snapshotRef.get();
    Entity e = snap.byId.get(key);

    if (e != null) {
      return e;
    }

    Entity loaded = loadEntityFromDBById(id, type);
    put(loaded);
    return loaded;
  }

  /** {@inheritDoc} */
  @Override
  public Entity getOrLoadMetadataByName(NameIdentifier ident, Entity.EntityType type) {
    Snapshot snap = snapshotRef.get();
    Entity e = snap.byName.get(ident);

    if (e != null) {
      return e;
    }

    try {
      Entity loaded = loadEntityFromDBByName(ident, type);
      put(loaded);
      return loaded;
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void removeById(Long id, Entity.EntityType type) {
    Snapshot snap = snapshotRef.get();
    Entity e = snap.byId.get(Pair.of(id, type));
    if (e == null) {
      throw new IllegalArgumentException("Not in cache: " + id + "/" + type);
    }

    remove(e);
  }

  /** {@inheritDoc} */
  @Override
  public void removeByName(NameIdentifier ident) {
    Snapshot snap = snapshotRef.get();
    Entity e = snap.byName.get(ident);
    if (e == null) {
      throw new IllegalArgumentException("Not in cache: " + ident);
    }
    remove(e);
  }

  /** {@inheritDoc} */
  @Override
  public boolean containsById(Long id, Entity.EntityType type) {
    return snapshotRef.get().byId.containsKey(Pair.of(id, type));
  }

  /** {@inheritDoc} */
  @Override
  public boolean containsByName(NameIdentifier ident) {
    return snapshotRef.get().byName.containsKey(ident);
  }

  /** {@inheritDoc} */
  @Override
  public void clear() {
    snapshotRef.set(new Snapshot());
  }

  /** {@inheritDoc} */
  @Override
  public long sizeOfCacheData() {
    return snapshotRef.get().byId.size();
  }

  /** {@inheritDoc} */
  @Override
  public long sizeOfCacheIndex() {
    return snapshotRef.get().index.size();
  }

  /** {@inheritDoc} */
  @Override
  public void put(Entity entity) {
    long id = CacheUtils.getIdFromEntity(entity);
    NameIdentifier ident = CacheUtils.getIdentFromEntity(entity);
    Entity.EntityType tp = entity.type();

    while (true) {
      Snapshot old = snapshotRef.get();
      // 1. copy existing maps
      Map<Pair<Long, Entity.EntityType>, Entity> newById = new HashMap<>(old.byId);
      Map<NameIdentifier, Entity> newByName = new HashMap<>(old.byName);
      NavigableMap<String, NameIdentifier> newIndex = new TreeMap<>(old.index);

      // 2. apply mutation
      newById.put(Pair.of(id, tp), entity);
      newByName.put(ident, entity);
      newIndex.put(ident.toString(), ident);

      Snapshot next = new Snapshot(newById, newByName, newIndex);
      if (snapshotRef.compareAndSet(old, next)) return;
    }
  }

  /**
   * Removes the given entity from the cache.
   *
   * @param entity The entity to remove.
   */
  private void remove(Entity entity) {
    String prefix = CacheUtils.getIdentFromEntity(entity).toString();

    while (true) {
      Snapshot old = snapshotRef.get();
      // 1.  collect keys to remove
      List<NameIdentifier> toRemove = new ArrayList<>();
      for (Map.Entry<String, NameIdentifier> e : old.index.tailMap(prefix).entrySet()) {
        if (!e.getKey().startsWith(prefix)) {
          break;
        }

        toRemove.add(e.getValue());
      }

      if (toRemove.isEmpty()) {
        return;
      }

      Map<Pair<Long, Entity.EntityType>, Entity> newById = new HashMap<>(old.byId);
      Map<NameIdentifier, Entity> newByName = new HashMap<>(old.byName);
      NavigableMap<String, NameIdentifier> newIndex = new TreeMap<>(old.index);

      // 2. apply removals
      for (NameIdentifier ident : toRemove) {
        Entity removed = newByName.remove(ident);
        if (removed != null) {
          newById.remove(Pair.of(CacheUtils.getIdFromEntity(removed), removed.type()));
        }
        newIndex.remove(ident.toString());
      }

      Snapshot next = new Snapshot(newById, newByName, newIndex);
      if (snapshotRef.compareAndSet(old, next)) {
        return;
      }
    }
  }

  /** Immutable snapshot of entire cache+index state */
  private static class Snapshot {
    final ImmutableMap<Pair<Long, Entity.EntityType>, Entity> byId;
    final ImmutableMap<NameIdentifier, Entity> byName;
    final ImmutableSortedMap<String, NameIdentifier> index;

    Snapshot() {
      this.byId = ImmutableMap.of();
      this.byName = ImmutableMap.of();
      this.index = ImmutableSortedMap.of();
    }

    /**
     * Constructs a new snapshot with the given maps.
     *
     * @param byId The map of entities by ID
     * @param byName The map of entities by name
     * @param index The index of names by prefix
     */
    Snapshot(
        Map<Pair<Long, Entity.EntityType>, Entity> byId,
        Map<NameIdentifier, Entity> byName,
        NavigableMap<String, NameIdentifier> index) {
      this.byId = ImmutableMap.copyOf(byId);
      this.byName = ImmutableMap.copyOf(byName);
      this.index = ImmutableSortedMap.copyOf(index);
    }
  }
}
