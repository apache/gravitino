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
    ROLE_GROUP_REL
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
}
