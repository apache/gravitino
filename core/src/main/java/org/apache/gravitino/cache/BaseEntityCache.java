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

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.gravitino.Config;
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.utils.HierarchicalSchemaUtil;

/**
 * An abstract class that provides a basic implementation for the {@link EntityCache} interface.
 * This class is abstract and cannot be instantiated directly, it is designed to be a base class for
 * other entity cache implementations.
 *
 * <p>This class enforces the cacheable entity type allowlist documented on {@link
 * SupportsEntityStoreCache#put(Entity)}: {@link #put(Entity)} is final and drops entities that have
 * not been explicitly approved before delegating to {@link #doPut(Entity)}, so no subclass can
 * accidentally cache them.
 */
public abstract class BaseEntityCache implements EntityCache {

  /**
   * Entity types that have been explicitly approved for caching, see {@link
   * SupportsEntityStoreCache#put(Entity)} for the contract. New entity types are excluded by
   * default until their invalidation behavior has been validated.
   *
   * <p>Only self-contained entities are cacheable: a stale copy of one is at worst cosmetically old
   * (an old comment, property, or job status), never a wrong pointer, and each can be invalidated
   * with a single one-to-one key drop. Every other type is read straight from the store.
   * User/group/role embed relation-derived data that a per-node cache cannot invalidate;
   * model/model version, Semantic Model, and function carry load-bearing content that would be
   * silently wrong if served stale.
   */
  private static final Set<Entity.EntityType> CACHEABLE_TYPES =
      Sets.immutableEnumSet(
          Entity.EntityType.METALAKE,
          Entity.EntityType.CATALOG,
          Entity.EntityType.SCHEMA,
          Entity.EntityType.TABLE,
          Entity.EntityType.TOPIC,
          Entity.EntityType.VIEW,
          Entity.EntityType.FILESET,
          Entity.EntityType.TAG,
          Entity.EntityType.POLICY,
          Entity.EntityType.JOB);

  protected final Config cacheConfig;

  /**
   * Constructs a new {@link BaseEntityCache} instance.
   *
   * @param config The cache configuration
   */
  public BaseEntityCache(Config config) {
    Preconditions.checkArgument(config != null, "Config must not be null");

    this.cacheConfig = config;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Defaults to {@link Coherence#LOCAL_PER_NODE}: an in-memory, per-node cache whose changes
   * must be propagated to the other nodes. A shared implementation must override this to return
   * {@link Coherence#SHARED}.
   */
  @Override
  public Coherence coherence() {
    return Coherence.LOCAL_PER_NODE;
  }

  /**
   * Returns whether entities of the given type may be cached.
   *
   * @param type The entity type to check.
   * @return {@code true} if entities of this type may be cached, {@code false} otherwise.
   */
  public static boolean isCacheable(Entity.EntityType type) {
    Preconditions.checkArgument(type != null, "Entity type cannot be null");

    return CACHEABLE_TYPES.contains(type);
  }

  /**
   * Separates {@link NameIdentifier} levels in a cache index key. Matching on this boundary (not
   * the bare identifier) keeps sibling prefix scans exact, e.g. {@code catalog1} vs {@code
   * catalog10}.
   */
  public static final String NAME_LEVEL_BOUNDARY = ".";

  /** Escapes {@link #NAME_LEVEL_BOUNDARY} and itself inside a level of an index key. */
  private static final char INDEX_ESCAPE = '\u0001';

  /**
   * Joins the levels of an identifier into prefix-index text used by hierarchy scans.
   *
   * @param ident The identifier to encode.
   * @return The escaped index text for {@code ident}.
   */
  public static String indexText(NameIdentifier ident) {
    StringBuilder text = new StringBuilder();
    for (String level : ident.namespace().levels()) {
      appendEscaped(text, level).append(NAME_LEVEL_BOUNDARY);
    }
    return appendEscaped(text, ident.name()).toString();
  }

  /**
   * Returns the descendant index prefixes used when cascading from {@code ident}.
   *
   * <p>Same prefixes {@link CaffeineEntityCache} uses for hierarchy invalidation. Does not include
   * {@code ident} itself.
   *
   * @param ident The ancestor identifier.
   * @param type The ancestor entity type.
   * @return One or two prefixes ending at a name-level or schema-separator boundary.
   */
  public static List<String> descendantPrefixes(NameIdentifier ident, Entity.EntityType type) {
    String identifier = indexText(ident);
    List<String> prefixes = new ArrayList<>(2);
    prefixes.add(identifier + NAME_LEVEL_BOUNDARY);
    if (type == Entity.EntityType.SCHEMA) {
      prefixes.add(identifier + HierarchicalSchemaUtil.schemaSeparator());
    }
    return prefixes;
  }

  private static StringBuilder appendEscaped(StringBuilder text, String level) {
    for (int i = 0; i < level.length(); i++) {
      char c = level.charAt(i);
      if (c == INDEX_ESCAPE || c == NAME_LEVEL_BOUNDARY.charAt(0)) {
        text.append(INDEX_ESCAPE);
      }
      text.append(c);
    }
    return text;
  }

  /** {@inheritDoc} */
  @Override
  public final <E extends Entity & HasIdentifier> void put(E entity) {
    Preconditions.checkArgument(entity != null, "Entity cannot be null");

    // Called before the cacheability check: a non-cacheable entity can still invalidate a cacheable
    // one, so this runs for every entity. The caller may already hold this entity's lock, so the
    // hook must not take another entity's lock (see
    // SupportsEntityStoreCache#invalidateOnKeyChange).
    invalidateOnKeyChange(entity);

    if (!isCacheable(entity.type())) {
      return;
    }

    doPut(entity);
  }

  /**
   * Caches the given entity. Called by {@link #put(Entity)} once the entity is known to be
   * non-null, cacheable, and any related cache entries have been invalidated.
   *
   * @param entity The entity to cache.
   * @param <E> The class of the entity.
   */
  protected abstract <E extends Entity & HasIdentifier> void doPut(E entity);

  /**
   * Returns the {@link NameIdentifier} of the entity based on its type.
   *
   * @param entity The {@link Entity} instance.
   * @return The {@link NameIdentifier} of the entity
   */
  public static NameIdentifier getIdentFromEntity(Entity entity) {
    validateEntityHasIdentifier(entity);
    HasIdentifier hasIdentifier = (HasIdentifier) entity;

    return hasIdentifier.nameIdentifier();
  }

  /**
   * Checks if the entity is of type {@link HasIdentifier}.
   *
   * @param entity The {@link Entity} instance to check.
   */
  protected static void validateEntityHasIdentifier(Entity entity) {
    Preconditions.checkArgument(entity != null, "Entity cannot be null");
    Preconditions.checkArgument(
        entity instanceof HasIdentifier, "Unsupported EntityType: " + entity.type());
  }

  /**
   * Converts a list of entities to a new list with the target entity type.
   *
   * @param entities Thr original list of entities.
   * @return A list of converted entities.
   * @param <E> The type of the entities in the new list.
   */
  @SuppressWarnings("unchecked")
  public static <E extends Entity & HasIdentifier> List<E> convertEntities(List<Entity> entities) {
    entities.forEach(BaseEntityCache::validateEntityHasIdentifier);

    return (List<E>) (List<? extends Entity>) entities;
  }

  /**
   * Converts an entity to a new one.
   *
   * @param entity The original entity.
   * @return A new entity.
   * @param <E> The type of the new entity.
   */
  @SuppressWarnings("unchecked")
  public static <E extends Entity & HasIdentifier> E convertEntity(Entity entity) {
    validateEntityHasIdentifier(entity);

    return (E) entity;
  }

  /**
   * Removes a key from the implementation's bookkeeping only if it is absent from the underlying
   * cache. This includes eviction, expiration, and explicit invalidation. This may be called while
   * the underlying cache holds an internal lock. Implementations must not acquire locks held across
   * underlying cache operations, and must tolerate the key having been added back before the call.
   *
   * <p>The default implementation delegates to the legacy hook so existing subclass overrides
   * remain effective.
   *
   * @param key The cache key to remove from bookkeeping if absent.
   */
  protected void removeIndexEntryIfAbsent(EntityCacheKey key) {
    invalidateExpiredItem(key);
  }

  /**
   * Legacy hook for removing an absent key from the implementation's bookkeeping.
   *
   * @param key The evicted, expired, or explicitly invalidated entity key.
   * @deprecated Use {@link #removeIndexEntryIfAbsent(EntityCacheKey)}. Existing overrides continue
   *     to receive calls through its default implementation and must obey its locking and
   *     reinsertion contract.
   */
  @Deprecated
  protected abstract void invalidateExpiredItem(EntityCacheKey key);
}
