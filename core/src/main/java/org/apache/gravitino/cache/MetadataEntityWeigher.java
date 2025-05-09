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
import org.apache.gravitino.Entity;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * A {@link Weigher} implementation that weighs {@link Entity} objects by the approximate size of
 * the entity object.
 */
public class MetadataEntityWeigher implements Weigher<Long, Entity> {

  /** The amount of weight that is expected to roughly equate to 1MB of memory usage */
  public static final long WEIGHT_PER_MB = 1024 * 1024;
  /** Represents the approximate size of an entity beyond the properties */
  private static final int APPROXIMATE_ENTITY_OVERHEAD = 500;

  private static final MetadataEntityWeigher INSTANCE = new MetadataEntityWeigher();

  private MetadataEntityWeigher() {}

  public static MetadataEntityWeigher getInstance() {
    return INSTANCE;
  }

  /**
   * Computes the weight of a given entity. The unit here is not exactly bytes, but it's close.
   *
   * @param id The entity's id; not used
   * @param entity The entity to be cached
   * @return The weight of the entity
   */
  @Override
  public @NonNegative int weigh(@NonNull Long id, @NonNull Entity entity) {
    return APPROXIMATE_ENTITY_OVERHEAD + calculateWeight(entity.type());
  }

  /**
   * Calculates the weight of an entity based on its type.
   *
   * @param tp The entity type
   * @return The weight of the entity type
   */
  private int calculateWeight(Entity.EntityType tp) {
    // TODO: find a more accurate weight of each entity type
    int weight;
    switch (tp) {
      case METALAKE:
        weight = 50;
        break;

      case CATALOG:
        weight = 75;
        break;

      case SCHEMA:
        weight = 100;
        break;

      default:
        weight = 125;
        break;
    }

    return weight;
  }
}
