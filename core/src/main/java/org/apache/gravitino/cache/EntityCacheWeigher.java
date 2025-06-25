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

import com.github.benmanes.caffeine.cache.Weigher;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import lombok.NonNull;
import org.apache.gravitino.Entity;
import org.checkerframework.checker.index.qual.NonNegative;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link Weigher} implementation that calculates the weight of an entity based on its type. The
 * weight is calculated as follows:
 *
 * <ul>
 *   <li>Metalake: 100
 *   <li>Catalog: 75
 *   <li>Schema: 50
 *   <li>Other: 15
 * </ul>
 */
public class EntityCacheWeigher implements Weigher<EntityCacheKey, List<Entity>> {
  public static final int METALAKE_WEIGHT = 100;
  public static final int CATALOG_WEIGHT = 75;
  public static final int SCHEMA_WEIGHT = 50;
  public static final int OTHER_WEIGHT = 15;
  private static final Logger LOG = LoggerFactory.getLogger(EntityCacheWeigher.class.getName());
  private static final EntityCacheWeigher INSTANCE = new EntityCacheWeigher();
  private static final Map<Entity.EntityType, Integer> ENTITY_WEIGHTS =
      ImmutableMap.of(
          Entity.EntityType.METALAKE, METALAKE_WEIGHT,
          Entity.EntityType.CATALOG, CATALOG_WEIGHT,
          Entity.EntityType.SCHEMA, SCHEMA_WEIGHT);
  private static long MAX_WEIGHT =
      2 * (METALAKE_WEIGHT * 10 + CATALOG_WEIGHT * (10 * 200) + SCHEMA_WEIGHT * (10 * 200 * 1000));

  @VisibleForTesting
  protected EntityCacheWeigher() {}

  /**
   * Returns the maximum weight that can be stored in the cache.
   *
   * <p>The total weight is estimated based on the expected number of entities:
   *
   * <ul>
   *   <li>~10 Metalakes per Gravitino instance
   *   <li>~200 Catalogs per Metalake
   *   <li>~1000 Schemas per Catalog
   * </ul>
   *
   * <p>The total estimated entity count is:
   *
   * <pre>
   *   10 * METALAKE_WEIGHT
   * + (10 * 200) * CATALOG_WEIGHT
   * + (10 * 200 * 1000) * SCHEMA_WEIGHT
   * </pre>
   *
   * <p>To provide headroom and avoid early eviction, the result is multiplied by 2:
   *
   * <pre>
   *   total = 2 * (10 * METALAKE_WEIGHT + 2000 * CATALOG_WEIGHT + 2_000_000 * SCHEMA_WEIGHT)
   * </pre>
   *
   * @return The maximum weight that can be stored in the cache.
   */
  public static long getMaxWeight() {
    return MAX_WEIGHT;
  }

  /**
   * Returns the singleton instance of the {@link EntityCacheWeigher}.
   *
   * @return the singleton instance of the {@link EntityCacheWeigher}.
   */
  public static EntityCacheWeigher getInstance() {
    return INSTANCE;
  }

  /** {@inheritDoc} */
  @Override
  public @NonNegative int weigh(
      @NonNull EntityCacheKey storeEntityCacheKey, @NonNull List<Entity> entities) {
    int weight = 0;
    for (Entity entity : entities) {
      weight += calculateWeight(entity.type());
    }

    if (weight > getMaxWeight()) {
      LOG.warn("Entity group exceeds max weight: {}", weight);
    }

    return weight;
  }

  private int calculateWeight(Entity.EntityType entityType) {
    Preconditions.checkArgument(entityType != null, "entityType cannot be null");
    return ENTITY_WEIGHTS.getOrDefault(entityType, OTHER_WEIGHT);
  }
}
