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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.utils.TestUtil;

public class BenchmarkHelper {
  public static final int RELATION_CNT = 5;
  private static final Random random = new Random();

  private BenchmarkHelper() {
    // Utility class, no constructor needed
  }

  /**
   * Generates a map of entities with their relations.
   *
   * @param entityCnt the count of entities to generate.
   * @return a map of entities with their relations.
   * @param <E> the type of the entity.
   */
  public static <E extends Entity & HasIdentifier> Map<RoleEntity, List<E>> getRelationEntities(
      int entityCnt) {
    Map<RoleEntity, List<E>> entitiesWithRelations = Maps.newHashMap();
    for (int i = 0; i < entityCnt; i++) {
      RoleEntity testRoleEntity = TestUtil.getTestRoleEntity();
      int userCnt = random.nextInt(RELATION_CNT);
      List<E> userList = BaseEntityCache.convertEntities(getUserList(testRoleEntity, userCnt));
      entitiesWithRelations.put(testRoleEntity, userList);
    }

    return entitiesWithRelations;
  }

  /**
   * Generates a list of entities with the specified count.
   *
   * @param entityCnt the count of entities to generate.
   * @return a list of model entities.
   */
  public static List<ModelEntity> getEntities(int entityCnt) {
    List<ModelEntity> entities = new ArrayList<>(entityCnt);
    for (int i = 0; i < entityCnt; i++) {
      entities.add(TestUtil.getTestModelEntity());
    }

    return entities;
  }

  /**
   * Returns a randomly selected key from the given map.
   *
   * @param map the input map from which to select a random key
   * @param <K> the type of keys in the map
   * @param <V> the type of values in the map
   * @return a randomly selected key from the map, or {@code null} if the map is null or empty
   */
  public static <K, V> K getRandomKey(Map<K, V> map) {
    if (map == null || map.isEmpty()) {
      return null;
    }

    List<K> keys = new ArrayList<>(map.keySet());

    return keys.get(random.nextInt(keys.size()));
  }

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
  public static void validateEntityHasIdentifier(Entity entity) {
    Preconditions.checkArgument(entity != null, "Entity cannot be null");
    Preconditions.checkArgument(
        entity instanceof HasIdentifier, "Unsupported EntityType: " + entity.type());
  }

  private static List<Entity> getUserList(RoleEntity roleEntity, int userCnt) {
    List<Entity> userList = new ArrayList<>(userCnt);
    List<Long> roleIds = ImmutableList.of(roleEntity.id());

    for (int i = 0; i < userCnt; i++) {
      userList.add(TestUtil.getTestUserEntity(roleIds));
    }

    return userList;
  }
}
