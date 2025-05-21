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
import java.util.Objects;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;

/** key for storing entities in store cache. */
public class StoreEntityCacheKey {
  private final NameIdentifier identifier;
  private final Entity.EntityType type;

  /**
   * Creates a new instance of {@link StoreEntityCacheKey}.
   *
   * @param ident The NameIdentifier of the entity
   * @param type The type of the entity
   * @return The new instance
   */
  public static StoreEntityCacheKey of(NameIdentifier ident, Entity.EntityType type) {
    return new StoreEntityCacheKey(ident, type);
  }

  /**
   * Creates a new instance of {@link StoreEntityCacheKey}.
   *
   * @param identifier The NameIdentifier of the entity
   * @param type The entity type
   */
  public StoreEntityCacheKey(NameIdentifier identifier, Entity.EntityType type) {
    Preconditions.checkArgument(identifier != null, "Id cannot be null");
    Preconditions.checkArgument(type != null, "EntityType cannot be null");

    this.identifier = identifier;
    this.type = type;
  }

  /**
   * Returns the entity's NameIdentifier.
   *
   * @return The entity's NameIdentifier
   */
  public NameIdentifier identifier() {
    return identifier;
  }

  /**
   * Returns the entity type.
   *
   * @return The entity type
   */
  public Entity.EntityType type() {
    return type;
  }

  /**
   * Compares two instances of {@link StoreEntityCacheKey}. The comparison is based on the entity
   * nameIdentifier and type.
   *
   * @param obj The object to compare to
   * @return {@code true} if the two instances are equal, {@code false} otherwise
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!(obj instanceof StoreEntityCacheKey)) return false;
    StoreEntityCacheKey other = (StoreEntityCacheKey) obj;
    return Objects.equals(identifier, other.identifier) && Objects.equals(type, other.type);
  }

  /**
   * Returns the hash code of this instance. The hash code is based on the entity nameIdentifier and
   * type.
   *
   * @return The hash code of this instance
   */
  @Override
  public int hashCode() {
    return Objects.hash(identifier, type);
  }

  /**
   * Returns a string representation of this instance.
   *
   * @return A string representation of this instance
   */
  @Override
  public String toString() {
    return identifier.toString() + ":" + type.getShortName();
  }
}
