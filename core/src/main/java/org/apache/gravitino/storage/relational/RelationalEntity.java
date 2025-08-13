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
package org.apache.gravitino.storage.relational;

import java.util.List;
import org.apache.gravitino.Entity;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Relation;

/**
 * Represents a relational entity that contains an entity, its vertex type, related identifiers, and
 * the type of related entity.
 *
 * @param <E> the type of the entity, which must extend {@link Entity} and implement {@link
 *     HasIdentifier}
 */
public class RelationalEntity<E extends Entity & HasIdentifier> {
  private final E entity;
  private final Relation.VertexType vertexType;
  private final List<NameIdentifier> relatedIdents;
  private final Entity.EntityType relatedEntityType;

  private RelationalEntity(
      E entity,
      Relation.VertexType vertexType,
      List<NameIdentifier> relatedIdents,
      Entity.EntityType relatedEntityType) {
    this.entity = entity;
    this.vertexType = vertexType;
    this.relatedIdents = relatedIdents;
    this.relatedEntityType = relatedEntityType;
  }

  /**
   * Creates a new instance of {@link RelationalEntity}.
   *
   * @param entity the entity to be wrapped
   * @param vertexType the type of the vertex in the relation
   * @param relatedIdents the list of related identifiers
   * @param relatedEntityType the type of the related entity
   * @return a new instance of {@link RelationalEntity}
   * @param <E> the type of the entity, which must extend {@link Entity} and implement {@link
   *     HasIdentifier}
   */
  public static <E extends Entity & HasIdentifier> RelationalEntity<E> of(
      E entity,
      Relation.VertexType vertexType,
      List<NameIdentifier> relatedIdents,
      Entity.EntityType relatedEntityType) {
    return new RelationalEntity<>(entity, vertexType, relatedIdents, relatedEntityType);
  }

  /**
   * Returns the entity wrapped by this relational entity.
   *
   * @return the entity
   */
  public E entity() {
    return entity;
  }

  /**
   * Returns the vertex type of the entity in the relation.
   *
   * @return the vertex type of the entity
   */
  public Relation.VertexType vertexType() {
    return vertexType;
  }

  /**
   * Returns the list of related identifiers.
   *
   * @return the list of related identifiers
   */
  public List<NameIdentifier> relatedIdents() {
    return relatedIdents;
  }

  /**
   * Returns the type of the related entity.
   *
   * @return the type of the related entity
   */
  public Entity.EntityType relatedEntityType() {
    return relatedEntityType;
  }
}
