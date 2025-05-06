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

/**
 * Represents a key for the cache.
 *
 * <p>The key consists of the entity id and type.
 */
public class MetaCacheKey {
  private final Long id;
  private final Entity.EntityType type;

  /**
   * Creates a new instance of {@link MetaCacheKey}.
   *
   * @param id The entity id
   * @param type The entity type
   * @return The new instance
   */
  public static MetaCacheKey of(long id, Entity.EntityType type) {
    return new MetaCacheKey(id, type);
  }

  /**
   * Creates a new instance of {@link MetaCacheKey}.
   *
   * @param id The entity id
   * @param type The entity type
   */
  public MetaCacheKey(Long id, Entity.EntityType type) {
    Preconditions.checkArgument(id != null, "Id cannot be null");
    Preconditions.checkArgument(type != null, "EntityType cannot be null");

    this.id = id;
    this.type = type;
  }

  /**
   * Returns the entity id.
   *
   * @return The entity id
   */
  public Long id() {
    return id;
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
   * Compares two instances of {@link MetaCacheKey}. The comparison is based on the entity id and
   * type.
   *
   * @param obj The object to compare to
   * @return {@code true} if the two instances are equal, {@code false} otherwise
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!(obj instanceof MetaCacheKey)) return false;
    MetaCacheKey other = (MetaCacheKey) obj;
    return id.equals(other.id()) && type.equals(other.type());
  }

  /**
   * Returns the hash code of this instance. The hash code is based on the entity id and type.
   *
   * @return The hash code of this instance
   */
  @Override
  public int hashCode() {
    return 31 * Objects.hashCode(id) + Objects.hashCode(type);
  }
}
