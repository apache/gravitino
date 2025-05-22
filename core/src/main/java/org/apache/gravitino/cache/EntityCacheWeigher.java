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
import java.util.List;
import lombok.NonNull;
import org.apache.gravitino.Entity;
import org.checkerframework.checker.index.qual.NonNegative;

/**
 * A {@link Weigher} implementation that calculates the weight of an entity based on its type. The
 * weight is calculated as follows:
 *
 * <ul>
 *   <li>Metalake: 100 bytes
 *   <li>Catalog: 75 bytes
 *   <li>Schema: 50 bytes
 *   <li>Other: 15 bytes
 * </ul>
 */
public class EntityCacheWeigher implements Weigher<EntityCacheKey, List<Entity>> {
  private static final EntityCacheWeigher INSTANCE = new EntityCacheWeigher();

  private EntityCacheWeigher() {}

  public static final int METALAKE_WEIGHT = 100;
  public static final int CATALOG_WEIGHT = 75;
  public static final int SCHEMA_WEIGHT = 50;
  public static final int OTHER_WEIGHT = 15;

  /**
   * Returns the maximum weight that can be stored in the cache. it is calculated as follows:
   * metalake approx 100 in every gravitino, catalog approx 200 in each metalake, schema approx 1000
   * in each catalog.
   *
   * @return The max weigh that can be stored in the cache.
   */
  public static long getMaxWeight() {
    return 2
        * (METALAKE_WEIGHT * 10 + CATALOG_WEIGHT * (10 * 200) + SCHEMA_WEIGHT * (10 * 200 * 1000));
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
    return weight;
  }

  private int calculateWeight(Entity.EntityType tp) {
    int weight;
    switch (tp) {
      case METALAKE:
        weight = METALAKE_WEIGHT;
        break;

      case CATALOG:
        weight = CATALOG_WEIGHT;
        break;

      case SCHEMA:
        weight = SCHEMA_WEIGHT;
        break;

      default:
        weight = OTHER_WEIGHT;
        break;
    }

    return weight;
  }
}
