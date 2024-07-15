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
package com.datastrato.gravitino.storage.relational;

import com.apache.gravitino.NameIdentifier;
import com.apache.gravitino.Namespace;
import com.apache.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.EntityAlreadyExistsException;
import com.datastrato.gravitino.HasIdentifier;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.function.Function;

/** Interface defining the operations for a Relation Backend. */
public interface RelationalBackend extends Closeable {

  /**
   * Initializes the Relational Backend environment with the provided configuration.
   *
   * @param config The configuration for the backend.
   */
  void initialize(Config config);

  /**
   * Lists the entities associated with the given parent namespace and entityType
   *
   * @param namespace The parent namespace of these entities.
   * @param entityType The type of these entities.
   * @return The list of entities associated with the given parent namespace and entityType, or null
   *     if the entities does not exist.
   * @throws NoSuchEntityException If the corresponding parent entity of these list entities cannot
   *     be found.
   * @throws IOException If the store operation fails
   */
  <E extends Entity & HasIdentifier> List<E> list(Namespace namespace, Entity.EntityType entityType)
      throws NoSuchEntityException, IOException;

  /**
   * Checks the entity associated with the given identifier and entityType whether exists.
   *
   * @param ident The identifier of the entity.
   * @param entityType The type of the entity.
   * @return True, if the entity can be found, else return false.
   * @throws IOException If the store operation fails
   */
  boolean exists(NameIdentifier ident, Entity.EntityType entityType) throws IOException;

  /**
   * Stores the entity, possibly overwriting an existing entity if specified.
   *
   * @param e The entity which need be stored.
   * @param overwritten If true, overwrites the existing value.
   * @throws EntityAlreadyExistsException If the entity already exists and overwrite is false.
   * @throws IOException If the store operation fails
   */
  <E extends Entity & HasIdentifier> void insert(E e, boolean overwritten)
      throws EntityAlreadyExistsException, IOException;

  /**
   * Updates the entity.
   *
   * @param ident The identifier of the entity which need be stored.
   * @param entityType The type of the entity.
   * @return The entity after updating.
   * @throws NoSuchEntityException If the entity is not exist.
   * @throws IOException If the entity is failed to update.
   */
  <E extends Entity & HasIdentifier> E update(
      NameIdentifier ident, Entity.EntityType entityType, Function<E, E> updater)
      throws IOException, NoSuchEntityException;

  /**
   * Retrieves the entity associated with the identifier and the entity type.
   *
   * @param ident The identifier of the entity.
   * @param entityType The type of the entity.
   * @return The entity associated with the identifier and the entity type, or null if the key does
   *     not exist.
   * @throws IOException If an I/O exception occurs during retrieval.
   */
  <E extends Entity & HasIdentifier> E get(NameIdentifier ident, Entity.EntityType entityType)
      throws IOException;

  /**
   * Soft deletes the entity associated with the identifier and the entity type.
   *
   * @param ident The identifier of the entity.
   * @param entityType The type of the entity.
   * @param cascade True, If you need to cascade delete entities, else false.
   * @return True, if the entity was successfully deleted, else false.
   * @throws IOException If the store operation fails
   */
  boolean delete(NameIdentifier ident, Entity.EntityType entityType, boolean cascade)
      throws IOException;

  /**
   * Permanently deletes the legacy data that has been marked as deleted before the given legacy
   * timeline.
   *
   * @param entityType The type of the entity.
   * @param legacyTimeline The time before which the data has been marked as deleted.
   * @return The count of the deleted data.
   * @throws IOException If the store operation fails
   */
  int hardDeleteLegacyData(Entity.EntityType entityType, long legacyTimeline) throws IOException;

  /**
   * Soft deletes the old version data that is older than or equal to the given version retention
   * count.
   *
   * @param entityType The type of the entity.
   * @param versionRetentionCount The count of versions to retain.
   * @return The count of the deleted data.
   * @throws IOException If the store operation fails
   */
  int deleteOldVersionData(Entity.EntityType entityType, long versionRetentionCount)
      throws IOException;
}
