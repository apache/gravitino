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

import java.io.IOException;
import java.util.List;
import org.apache.gravitino.exceptions.NoSuchEntityException;

/**
 * This is an extended interface. This is mainly used for strengthen the ability of querying
 * relational data.
 */
public interface SupportsRelationOperations {

  /** Relation is an abstraction which connects two entities. */
  enum Type {
    /** The owner relationship */
    OWNER_REL,
    /** Metadata objet and role relationship */
    METADATA_OBJECT_ROLE_REL,
    /** Role and user relationship */
    ROLE_USER_REL,
    /** Role and group relationship */
    ROLE_GROUP_REL,
    /** Policy and metadata object relationship */
    POLICY_METADATA_OBJECT_REL
  }

  /**
   * List the entities according to a given entity in a specific relation.
   *
   * @param <E> The type of entities returned.
   * @param relType The type of relation.
   * @param nameIdentifier The given entity identifier.
   * @param identType The given entity type.
   * @return The list of entities
   * @throws IOException When occurs storage issues, it will throw IOException.
   */
  default <E extends Entity & HasIdentifier> List<E> listEntitiesByRelation(
      Type relType, NameIdentifier nameIdentifier, Entity.EntityType identType) throws IOException {
    return listEntitiesByRelation(relType, nameIdentifier, identType, true /* allFields*/);
  }

  /**
   * List the entities according to a given entity in a specific relation.
   *
   * @param <E> the type of entities returned.
   * @param relType The type of relation.
   * @param nameIdentifier The given entity identifier
   * @param identType The given entity type.
   * @param allFields Some fields may have a relatively high acquisition cost, EntityStore provide
   *     an optional setting to avoid fetching these high-cost fields to improve the performance. If
   *     true, the method will fetch all the fields, Otherwise, the method will fetch all the fields
   *     except for high-cost fields.
   * @return The list of entities
   * @throws IOException When occurs storage issues, it will throw IOException.
   */
  <E extends Entity & HasIdentifier> List<E> listEntitiesByRelation(
      Type relType, NameIdentifier nameIdentifier, Entity.EntityType identType, boolean allFields)
      throws IOException;

  /**
   * Get a specific entity that is related to a given source entity.
   *
   * <p>For example, this can be used to get a specific policy that is directly associated with a
   * metadata object.
   *
   * @param <E> The type of the entity to be returned.
   * @param relType The type of relation.
   * @param srcIdentifier The identifier of the source entity in the relation (e.g., a metadata
   *     object).
   * @param srcType The type of the source entity.
   * @param destEntityIdent The identifier of the target entity to retrieve (e.g., a policy).
   * @return The specific entity that is related to the source entity.
   * @throws IOException If a storage-related error occurs.
   * @throws NoSuchEntityException If the source entity or the target related entity does not exist,
   *     or if the relation does not exist.
   */
  <E extends Entity & HasIdentifier> E getEntityByRelation(
      Type relType,
      NameIdentifier srcIdentifier,
      Entity.EntityType srcType,
      NameIdentifier destEntityIdent)
      throws IOException, NoSuchEntityException;

  /**
   * insert a relation between two entities
   *
   * @param relType The type of relation.
   * @param srcIdentifier The source entity identifier.
   * @param srcType The source entity type.
   * @param dstIdentifier The destination entity identifier.
   * @param dstType The destination entity type.
   * @param override If override is true, we should remove all relations of source entity first.
   * @throws IOException When occurs storage issues, it will throw IOException.
   */
  void insertRelation(
      Type relType,
      NameIdentifier srcIdentifier,
      Entity.EntityType srcType,
      NameIdentifier dstIdentifier,
      Entity.EntityType dstType,
      boolean override)
      throws IOException;

  /**
   * Updates the relations for a given entity by adding a set of new relations and removing another
   * set of relations.
   *
   * @param <E> The type of the entity returned in the list, which represents the final state of
   *     related entities.
   * @param relType The type of relation to update.
   * @param srcEntityIdent The identifier of the source entity whose relations are being updated.
   * @param srcEntityType The type of the source entity, which is the entity whose relations are
   *     being updated.
   * @param destEntitiesToAdd An array of identifiers for entities to be associated with.
   * @param destEntitiesToRemove An array of identifiers for entities to be disassociated from.
   * @return A list of entities that are related to the given entity after the update.
   * @throws IOException If a storage-related error occurs.
   * @throws NoSuchEntityException If any of the specified entities does not exist.
   * @throws EntityAlreadyExistsException If a relation to be added already exists.
   */
  default <E extends Entity & HasIdentifier> List<E> updateEntityRelations(
      Type relType,
      NameIdentifier srcEntityIdent,
      Entity.EntityType srcEntityType,
      NameIdentifier[] destEntitiesToAdd,
      NameIdentifier[] destEntitiesToRemove)
      throws IOException, NoSuchEntityException, EntityAlreadyExistsException {
    throw new UnsupportedOperationException(
        "updateEntityRelations is not supported by this implementation");
  }
}
