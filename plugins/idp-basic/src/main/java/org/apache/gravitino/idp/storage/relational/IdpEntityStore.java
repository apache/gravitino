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
package org.apache.gravitino.idp.storage.relational;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import org.apache.gravitino.Config;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.idp.meta.IdpEntity;
import org.apache.gravitino.idp.meta.IdpEntityType;

/** Entity store for built-in IdP metadata. */
public interface IdpEntityStore extends Closeable {

  /**
   * Initializes the built-in IdP entity store.
   *
   * @param config The configuration for the entity store.
   * @throws RuntimeException If the initialization fails.
   */
  void initialize(Config config) throws RuntimeException;

  /**
   * Checks whether the entity exists.
   *
   * @param ident The name identifier of the entity.
   * @param entityType The built-in IdP entity type.
   * @return True if the entity exists, false otherwise.
   * @throws IOException If the check operation fails.
   */
  boolean exists(NameIdentifier ident, IdpEntityType entityType) throws IOException;

  /**
   * Stores the entity.
   *
   * @param entity The entity to store.
   * @param overwritten Whether to overwrite an existing entity.
   * @param <E> The entity type.
   * @throws IOException If the store operation fails.
   * @throws EntityAlreadyExistsException If the entity already exists and overwrite is false.
   */
  <E extends IdpEntity & HasIdentifier> void put(E entity, boolean overwritten)
      throws IOException, EntityAlreadyExistsException;

  /**
   * Gets the entity.
   *
   * @param ident The name identifier of the entity.
   * @param entityType The built-in IdP entity type.
   * @param clazz The entity class.
   * @param <E> The entity type.
   * @return The entity.
   * @throws NoSuchEntityException If the entity does not exist.
   * @throws IOException If the retrieve operation fails.
   */
  <E extends IdpEntity & HasIdentifier> E get(
      NameIdentifier ident, IdpEntityType entityType, Class<E> clazz)
      throws NoSuchEntityException, IOException;

  /**
   * Batch gets entities.
   *
   * @param idents The name identifiers of the entities.
   * @param entityType The built-in IdP entity type.
   * @param clazz The entity class.
   * @param <E> The entity type.
   * @return The entities.
   */
  <E extends IdpEntity & HasIdentifier> List<E> batchGet(
      List<NameIdentifier> idents, IdpEntityType entityType, Class<E> clazz);

  /**
   * Deletes the entity.
   *
   * @param ident The name identifier of the entity.
   * @param entityType The built-in IdP entity type.
   * @return True if the entity existed and was deleted, false otherwise.
   * @throws IOException If the delete operation fails.
   */
  boolean delete(NameIdentifier ident, IdpEntityType entityType) throws IOException;
}
