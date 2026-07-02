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
import org.apache.gravitino.exceptions.NoSuchEntityException;

/**
 * Optional extension for entity stores that support lookup and mutation by external id within a
 * namespace.
 */
public interface SupportsExternalIdOperations {

  /**
   * Get the entity from the underlying storage by external id within the namespace.
   *
   * @param namespace the namespace of the entity
   * @param entityType the general type of the entity
   * @param type the detailed type of the entity
   * @param externalId the external id of the entity
   * @param <E> the class of entity
   * @return the entity retrieved from the underlying storage
   * @throws NoSuchEntityException if the entity does not exist
   * @throws IOException if the retrieve operation fails
   */
  <E extends Entity & HasIdentifier> E getByExternalId(
      Namespace namespace, Entity.EntityType entityType, Class<E> type, String externalId)
      throws NoSuchEntityException, IOException;

  /**
   * Update the enabled state of an entity by external id within the namespace.
   *
   * @param namespace the namespace of the entity
   * @param entityType the general type of the entity
   * @param externalId the external id of the entity
   * @param enabled the expected enabled state
   * @param <E> the class of entity
   * @return the updated entity
   * @throws NoSuchEntityException if the entity does not exist
   * @throws IOException if the update operation fails
   */
  <E extends Entity & HasIdentifier> E updateEnabledByExternalId(
      Namespace namespace, Entity.EntityType entityType, String externalId, boolean enabled)
      throws NoSuchEntityException, IOException;

  /**
   * Delete an entity by external id within the namespace.
   *
   * @param namespace the namespace of the entity
   * @param entityType the general type of the entity
   * @param externalId the external id of the entity
   * @return true if the entity was deleted
   * @throws NoSuchEntityException if the entity does not exist
   * @throws IOException if the delete operation fails
   */
  boolean deleteByExternalId(Namespace namespace, Entity.EntityType entityType, String externalId)
      throws NoSuchEntityException, IOException;
}
