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

import java.util.Objects;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.SupportsRelationOperations;

/** Key for caching relation entities. */
public class RelationEntityCacheKey {
  private final NameIdentifier identifier;
  private final Entity.EntityType type;
  private final SupportsRelationOperations.Type relationType;

  /**
   * Creates a new instance of {@link RelationEntityCacheKey} with the given parameters.
   *
   * @param ident The identifier of the entity.
   * @param type The type of the entity.
   * @param relationType The type of the relation.
   * @return A new instance of {@link RelationEntityCacheKey}.
   */
  public static RelationEntityCacheKey of(
      NameIdentifier ident, Entity.EntityType type, SupportsRelationOperations.Type relationType) {
    return new RelationEntityCacheKey(ident, type, relationType);
  }

  /**
   * Creates a new instance of {@link RelationEntityCacheKey} with the given parameters.
   *
   * @param storeEntityCacheKey The {@link StoreEntityCacheKey} instance.
   * @param relationType The type of the relation.
   * @return A new instance of {@link RelationEntityCacheKey}.
   */
  public static RelationEntityCacheKey of(
      StoreEntityCacheKey storeEntityCacheKey, SupportsRelationOperations.Type relationType) {
    return new RelationEntityCacheKey(
        storeEntityCacheKey.identifier(), storeEntityCacheKey.type(), relationType);
  }

  /**
   * Creates a new instance of {@link RelationEntityCacheKey} with the given parameters.
   *
   * @param identifier The identifier of the entity.
   * @param type The type of the entity.
   * @param relationType The type of the relation.
   */
  public RelationEntityCacheKey(
      NameIdentifier identifier,
      Entity.EntityType type,
      SupportsRelationOperations.Type relationType) {
    this.identifier = identifier;
    this.type = type;
    this.relationType = relationType;
  }

  /**
   * Returns the identifier of the entity.
   *
   * @return The identifier of the entity.
   */
  public NameIdentifier identifier() {
    return identifier;
  }

  /**
   * Returns the type of the entity.
   *
   * @return The type of the entity.
   */
  public Entity.EntityType entityType() {
    return type;
  }

  /**
   * Returns the type of the relation.
   *
   * @return The type of the relation.
   */
  public SupportsRelationOperations.Type relationType() {
    return relationType;
  }

  /**
   * Returns the {@link StoreEntityCacheKey} instance for the entity.
   *
   * @return The {@link StoreEntityCacheKey} instance for the entity.
   */
  public StoreEntityCacheKey storeEntityCacheKey() {
    return StoreEntityCacheKey.of(identifier, type);
  }

  /**
   * Compares two instances of {@link RelationEntityCacheKey} for equality. The comparison is done
   * by comparing the identifier, type, and relationType of the instances.
   *
   * @param obj The object to compare to.
   * @return {@code true} if the objects are equal, {@code false} otherwise.
   */
  @Override
  public boolean equals(Object obj) {
    if (obj == this) return true;
    if (!(obj instanceof RelationEntityCacheKey)) return false;
    RelationEntityCacheKey other = (RelationEntityCacheKey) obj;

    return Objects.equals(identifier, other.identifier)
        && Objects.equals(type, other.type)
        && Objects.equals(relationType, other.relationType);
  }

  /**
   * Returns a hash code for this instance. The hash code is calculated by hashing the identifier,
   * type, and relationType of the instance.
   *
   * @return A hash code for this instance.
   */
  @Override
  public int hashCode() {
    return Objects.hash(identifier, type, relationType);
  }

  /**
   * Returns a string representation of this instance. The string is formatted as
   * "identifier:type:relationType".
   *
   * @return A string representation of this instance.
   */
  @Override
  public String toString() {
    return identifier.name() + ":" + type.name() + ":" + relationType.name();
  }
}
