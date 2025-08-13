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

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import lombok.Getter;

/** This interface defines an entity within the Apache Gravitino framework. */
public interface Entity extends Serializable {

  // The below constants are used for virtual metalakes, catalogs and schemas
  // The system doesn't need to create them. The system uses these constants
  // to organize the system information better.

  /** The system reserved metalake name. */
  String SYSTEM_METALAKE_RESERVED_NAME = "system";

  /** The system reserved catalog name. */
  String SYSTEM_CATALOG_RESERVED_NAME = "system";

  /** The authorization catalog name in the system metalake. */
  String AUTHORIZATION_CATALOG_NAME = "authorization";

  /** The user schema name in the system catalog. */
  String USER_SCHEMA_NAME = "user";

  /** The group schema name in the system catalog. */
  String GROUP_SCHEMA_NAME = "group";

  /** The role schema name in the system catalog. */
  String ROLE_SCHEMA_NAME = "role";

  /** The admin schema name in the authorization catalog of the system metalake. */
  String ADMIN_SCHEMA_NAME = "admin";

  /** The tag schema name in the system catalog. */
  String TAG_SCHEMA_NAME = "tag";

  /** The policy schema name in the system catalog. */
  String POLICY_SCHEMA_NAME = "policy";

  String JOB_TEMPLATE_SCHEMA_NAME = "job_template";

  String JOB_SCHEMA_NAME = "job";

  /** Enumeration defining the types of entities in the Gravitino framework. */
  @Getter
  enum EntityType {
    METALAKE,
    CATALOG,
    SCHEMA,
    TABLE,
    COLUMN,
    FILESET,
    TOPIC,
    USER,
    GROUP,
    ROLE,
    TAG,
    MODEL,
    MODEL_VERSION,
    POLICY,
    STATISTIC,
    JOB_TEMPLATE,
    JOB,
    AUDIT;
  }

  /**
   * Validates the entity by ensuring the validity of its field arguments.
   *
   * @throws IllegalArgumentException If the validation fails.
   */
  default void validate() throws IllegalArgumentException {
    fields().forEach(Field::validate);
  }

  /**
   * Retrieves the fields and their associated values of the entity.
   *
   * @return A map of Field to Object representing the entity's schema with values.
   */
  Map<Field, Object> fields();

  /**
   * Retrieves the type of the entity.
   *
   * @return The type of the entity as defined by {@link EntityType}.
   */
  EntityType type();

  /**
   * Represents a relational entity that contains an entity, its vertex type, related identifiers,
   * and the type of related entity.
   *
   * @param <E> the type of the entity, which must extend {@link Entity} and implement {@link
   *     HasIdentifier}
   */
  interface RelationalEntity<E extends Entity & HasIdentifier> {

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
    static <E extends Entity & HasIdentifier> RelationalEntity<E> of(
        E entity,
        Relation.VertexType vertexType,
        List<NameIdentifier> relatedIdents,
        EntityType relatedEntityType) {
      if (vertexType == Relation.VertexType.SOURCE) {
        return new SourceRelationEntity<>(entity, relatedIdents, relatedEntityType);
      } else {
        return new DestRelationEntity<>(entity, relatedIdents, relatedEntityType);
      }
    }

    /**
     * Returns the entity wrapped by this relational entity.
     *
     * @return the entity
     */
    E entity();

    /**
     * Returns the vertex type of the entity in the relation.
     *
     * @return the vertex type of the entity
     */
    Relation.VertexType vertexType();

    /**
     * Returns the list of related name identifiers.
     *
     * @return the list of related name identifiers
     */
    List<NameIdentifier> relatedNameIdentifiers();

    /**
     * Returns the type of the related entity.
     *
     * @return the type of the related entity
     */
    EntityType relatedEntityType();
  }

  /**
   * Represents a relational entity that contains a source vertex entity, its vertex type, related
   * destination vertex identifiers, and the type of the destination entity.
   *
   * @param <E> the type of the entity, which must extend {@link Entity} and implement {@link
   *     HasIdentifier}
   */
  class SourceRelationEntity<E extends Entity & HasIdentifier> implements RelationalEntity<E> {
    final E sourceVertexEntity;
    final List<NameIdentifier> destVertexIdents;
    final EntityType destVertexEntityType;

    SourceRelationEntity(
        E sourceVertexEntity,
        List<NameIdentifier> destVertexIdents,
        EntityType destVertexEntityType) {
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
    public EntityType relatedEntityType() {
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
  class DestRelationEntity<E extends Entity & HasIdentifier> implements RelationalEntity<E> {
    final E destVertexEntity;
    final List<NameIdentifier> sourceVertexIdents;
    final EntityType sourceVertexEntityType;

    DestRelationEntity(
        E destVertexEntity, List<NameIdentifier> sourceVertexIdents, EntityType sourceEntityType) {
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
    public EntityType relatedEntityType() {
      return sourceVertexEntityType;
    }
  }
}
