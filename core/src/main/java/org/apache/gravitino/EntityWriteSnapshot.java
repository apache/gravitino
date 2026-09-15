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
package org.apache.gravitino;

import com.google.common.base.Preconditions;

/**
 * An entity and the storage version observed together for a conditional reconciliation.
 *
 * <p>The storage version is an OCC token, not a schema version or an external catalog revision.
 * Callers must obtain this snapshot before reading the external state used for reconciliation.
 *
 * @param <E> the entity type
 */
public final class EntityWriteSnapshot<E extends Entity & HasIdentifier> {
  private final E entity;
  private final NameIdentifier identifier;
  private final Entity.EntityType type;
  private final long id;
  private final long version;

  /**
   * Creates a snapshot from an entity and its atomically observed storage version.
   *
   * @param entity the observed entity
   * @param version the observed storage version
   */
  public EntityWriteSnapshot(E entity, long version) {
    this.entity = Preconditions.checkNotNull(entity, "entity must not be null");
    Preconditions.checkArgument(version > 0, "version must be positive");
    this.identifier = entity.nameIdentifier();
    this.type = entity.type();
    this.id = entity.id();
    this.version = version;
  }

  /**
   * @return the observed entity
   */
  public E entity() {
    return entity;
  }

  /**
   * @return the observed name
   */
  public NameIdentifier identifier() {
    return identifier;
  }

  /**
   * @return the observed entity type
   */
  public Entity.EntityType type() {
    return type;
  }

  /**
   * @return the observed stable ID
   */
  public long id() {
    return id;
  }

  /**
   * @return the observed storage OCC version
   */
  public long version() {
    return version;
  }

  /**
   * Checks that a replacement belongs to this snapshot. Reconciliation cannot rename an entity.
   *
   * @param replacement the proposed replacement
   */
  public void validateReplacement(E replacement) {
    Preconditions.checkArgument(
        replacement.type() == type
            && replacement.id() == id
            && replacement.nameIdentifier().equals(identifier),
        "Reconciliation must preserve the observed entity name, type, and ID");
  }
}
