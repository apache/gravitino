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
import com.google.common.collect.Lists;
import com.googlecode.concurrenttrees.radix.ConcurrentRadixTree;
import com.googlecode.concurrenttrees.radix.RadixTree;
import com.googlecode.concurrenttrees.radix.node.concrete.DefaultCharArrayNodeFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.UserEntity;

/**
 * Reverse index cache for managing entity relationships. This cache uses a radix tree to
 * efficiently store and retrieve relationships between entities based on their keys.
 */
public class ReverseIndexCache {
  private RadixTree<List<EntityCacheKey>> reverseIndex;
  /** Registers a reverse index processor for a specific entity class. */
  private final Map<Class<? extends Entity>, ReverseIndexRule> reverseIndexRules = new HashMap<>();

  public ReverseIndexCache() {
    this.reverseIndex = new ConcurrentRadixTree<>(new DefaultCharArrayNodeFactory());

    registerReverseRule(UserEntity.class, ReverseIndexRules.USER_REVERSE_RULE);
    registerReverseRule(GroupEntity.class, ReverseIndexRules.GROUP_REVERSE_RULE);
    registerReverseRule(RoleEntity.class, ReverseIndexRules.ROLE_REVERSE_RULE);
  }

  public boolean remove(EntityCacheKey key) {
    return reverseIndex.remove(key.toString());
  }

  public Iterable<List<EntityCacheKey>> getValuesForKeysStartingWith(String keyPrefix) {
    return reverseIndex.getValuesForKeysStartingWith(keyPrefix);
  }

  public Iterable<CharSequence> getKeysStartingWith(String keyPrefix) {
    return reverseIndex.getKeysStartingWith(keyPrefix);
  }

  public boolean remove(String key) {
    return reverseIndex.remove(key);
  }

  public int size() {
    return reverseIndex.size();
  }

  public void put(
      NameIdentifier nameIdentifier, Entity.EntityType type, EntityCacheRelationKey key) {
    EntityCacheKey entityCacheKey = EntityCacheKey.of(nameIdentifier, type);
    List<EntityCacheKey> existingKeys = reverseIndex.getValueForExactKey(entityCacheKey.toString());
    if (existingKeys == null) {
      reverseIndex.put(entityCacheKey.toString(), List.of(key));
    } else {
      if (existingKeys.contains(key)) {
        return;
      }

      List<EntityCacheKey> newValues = Lists.newArrayList(existingKeys);
      newValues.add(key);
      reverseIndex.put(entityCacheKey.toString(), newValues);
    }
  }

  public List<EntityCacheKey> get(NameIdentifier nameIdentifier, Entity.EntityType type) {
    EntityCacheKey entityCacheKey = EntityCacheKey.of(nameIdentifier, type);
    return reverseIndex.getValueForExactKey(entityCacheKey.toString());
  }

  public void put(Entity entity, EntityCacheRelationKey key) {
    Preconditions.checkArgument(entity != null, "EntityCacheRelationKey cannot be null");

    if (entity instanceof HasIdentifier) {
      NameIdentifier nameIdent = ((HasIdentifier) entity).nameIdentifier();
      put(nameIdent, entity.type(), key);
    }
  }

  public void registerReverseRule(Class<? extends Entity> entityClass, ReverseIndexRule rule) {
    reverseIndexRules.put(entityClass, rule);
  }

  /** Processes an entity and updates the reverse index accordingly. */
  public void indexEntity(Entity entity, EntityCacheRelationKey key) {
    ReverseIndexRule rule = reverseIndexRules.get(entity.getClass());
    if (rule != null) {
      rule.indexEntity(entity, key, this);
    }
  }

  /** Functional interface for processing reverse index rules. */
  @FunctionalInterface
  interface ReverseIndexRule {
    void indexEntity(Entity entity, EntityCacheRelationKey key, ReverseIndexCache cache);
  }

  @Override
  public String toString() {
    Iterable<CharSequence> keys = reverseIndex.getKeysStartingWith("");
    StringBuilder sb = new StringBuilder();
    for (CharSequence key : keys) {
      sb.append(key).append(" -> ").append(reverseIndex.getValueForExactKey(key.toString()));
      sb.append("\n");
    }

    return sb.toString();
  }
}
