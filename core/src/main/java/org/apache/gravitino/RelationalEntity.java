/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino;

/**
 * Represents a directed relation between two entities. The source is identified by a {@link
 * NameIdentifier} and an {@link Entity.EntityType}; the target is the actual resolved entity,
 * avoiding additional database round-trips to look it up.
 *
 * @param <T> the type of the target entity
 */
public class RelationalEntity<T extends Entity & HasIdentifier> {
  private final SupportsRelationOperations.Type type;
  private final NameIdentifier source;
  private final Entity.EntityType sourceType;
  private final T targetEntity;

  /**
   * Constructs a RelationalEntity.
   *
   * @param type the relation type
   * @param source the source identifier
   * @param sourceType the entity type of the source
   * @param targetEntity the resolved target entity
   */
  public RelationalEntity(
      SupportsRelationOperations.Type type,
      NameIdentifier source,
      Entity.EntityType sourceType,
      T targetEntity) {
    this.type = type;
    this.source = source;
    this.sourceType = sourceType;
    this.targetEntity = targetEntity;
  }

  /**
   * Gets the relation type.
   *
   * @return the type of the relation
   */
  public SupportsRelationOperations.Type type() {
    return type;
  }

  /**
   * Gets the source identifier.
   *
   * @return the source identifier
   */
  public NameIdentifier source() {
    return source;
  }

  /**
   * Gets the entity type of the source.
   *
   * @return the source entity type
   */
  public Entity.EntityType sourceType() {
    return sourceType;
  }

  /**
   * Gets the resolved target entity.
   *
   * @return the target entity
   */
  public T targetEntity() {
    return targetEntity;
  }
}
