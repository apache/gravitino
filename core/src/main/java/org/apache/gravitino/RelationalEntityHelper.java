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

import java.util.List;

/**
 * Utility class for creating instances of {@link Entity.RelationalEntity} that represent relational
 * entities in a graph structure.
 */
public class RelationalEntityHelper {
  private RelationalEntityHelper() {}

  /**
   * Creates a new instance of {@link Entity.RelationalEntity}.
   *
   * @param entity the entity to be wrapped
   * @param vertexType the type of the vertex in the relation
   * @param relatedIdents the list of related identifiers
   * @param relatedEntityType the type of the related entity
   * @return a new instance of {@link Entity.RelationalEntity}
   * @param <E> the type of the entity, which must extend {@link Entity} and implement {@link
   *     HasIdentifier}
   */
  public static <E extends Entity & HasIdentifier> Entity.RelationalEntity<E> of(
      E entity,
      Relation.VertexType vertexType,
      List<NameIdentifier> relatedIdents,
      Entity.EntityType relatedEntityType) {
    if (vertexType == Relation.VertexType.SOURCE) {
      return new SourceRelationEntity<>(entity, relatedIdents, relatedEntityType);
    } else {
      return new DestRelationEntity<>(entity, relatedIdents, relatedEntityType);
    }
  }

  /**
   * Represents a relational entity that contains a source vertex entity, its vertex type, related
   * destination vertex identifiers, and the type of the destination entity.
   *
   * @param <E> the type of the entity, which must extend {@link Entity} and implement {@link
   *     HasIdentifier}
   */
  private static class SourceRelationEntity<E extends Entity & HasIdentifier>
      implements Entity.RelationalEntity<E> {
    final E sourceVertexEntity;
    final List<NameIdentifier> destVertexIdents;
    final Entity.EntityType destVertexEntityType;

    SourceRelationEntity(
        E sourceVertexEntity,
        List<NameIdentifier> destVertexIdents,
        Entity.EntityType destVertexEntityType) {
      this.sourceVertexEntity = sourceVertexEntity;
      this.destVertexIdents = destVertexIdents;
      this.destVertexEntityType = destVertexEntityType;
    }

    @Override
    public E entity() {
      return sourceVertexEntity;
    }

    @Override
    public Relation.VertexType vertexType() {
      return Relation.VertexType.SOURCE;
    }

    @Override
    public List<NameIdentifier> relatedNameIdentifiers() {
      return destVertexIdents;
    }

    @Override
    public Entity.EntityType relatedEntityType() {
      return destVertexEntityType;
    }
  }

  /**
   * Represents a relational entity that contains a destination vertex entity, its vertex type,
   * related source vertex identifiers, and the type of the source entity.
   *
   * @param <E> the type of the entity, which must extend {@link Entity} and implement {@link
   *     HasIdentifier}
   */
  private static class DestRelationEntity<E extends Entity & HasIdentifier>
      implements Entity.RelationalEntity<E> {
    final E destVertexEntity;
    final List<NameIdentifier> sourceVertexIdents;
    final Entity.EntityType sourceVertexEntityType;

    DestRelationEntity(
        E destVertexEntity,
        List<NameIdentifier> sourceVertexIdents,
        Entity.EntityType sourceEntityType) {
      this.destVertexEntity = destVertexEntity;
      this.sourceVertexIdents = sourceVertexIdents;
      this.sourceVertexEntityType = sourceEntityType;
    }

    @Override
    public E entity() {
      return destVertexEntity;
    }

    @Override
    public Relation.VertexType vertexType() {
      return Relation.VertexType.DESTINATION;
    }

    @Override
    public List<NameIdentifier> relatedNameIdentifiers() {
      return sourceVertexIdents;
    }

    @Override
    public Entity.EntityType relatedEntityType() {
      return sourceVertexEntityType;
    }
  }
}
