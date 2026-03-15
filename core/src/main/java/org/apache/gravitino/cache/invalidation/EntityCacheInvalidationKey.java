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

package org.apache.gravitino.cache.invalidation;

import com.google.common.base.Preconditions;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.SupportsRelationOperations;

/** Invalidation key for entity and relation caches. */
public final class EntityCacheInvalidationKey {
  private final NameIdentifier identifier;
  private final Entity.EntityType entityType;
  @Nullable private final SupportsRelationOperations.Type relationType;

  private EntityCacheInvalidationKey(
      NameIdentifier identifier,
      Entity.EntityType entityType,
      @Nullable SupportsRelationOperations.Type relationType) {
    this.identifier = Preconditions.checkNotNull(identifier, "identifier cannot be null");
    this.entityType = Preconditions.checkNotNull(entityType, "entityType cannot be null");
    this.relationType = relationType;
  }

  public static EntityCacheInvalidationKey of(
      NameIdentifier identifier,
      Entity.EntityType entityType,
      @Nullable SupportsRelationOperations.Type relationType) {
    return new EntityCacheInvalidationKey(identifier, entityType, relationType);
  }

  public NameIdentifier identifier() {
    return identifier;
  }

  public Entity.EntityType entityType() {
    return entityType;
  }

  @Nullable
  public SupportsRelationOperations.Type relationType() {
    return relationType;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof EntityCacheInvalidationKey)) {
      return false;
    }
    EntityCacheInvalidationKey that = (EntityCacheInvalidationKey) o;
    return Objects.equals(identifier, that.identifier)
        && entityType == that.entityType
        && relationType == that.relationType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(identifier, entityType, relationType);
  }
}
