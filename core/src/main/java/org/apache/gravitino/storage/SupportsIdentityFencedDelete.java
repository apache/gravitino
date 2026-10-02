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
package org.apache.gravitino.storage;

import java.io.IOException;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.OptimisticLockException;

/**
 * Optional store or backend capability for deleting an observed registration by identity.
 *
 * <p>Implementations must support both operations for schemas, tables, topics, and views. Callers
 * obtain this capability and read the identity before an external operation. There is no fallback
 * to deletion by name. Row versions remain internal to the atomic store write.
 */
public interface SupportsIdentityFencedDelete {

  /**
   * Reads the current registration identity directly from storage, bypassing entity caches.
   *
   * @param ident the entity identifier
   * @param entityType the entity type
   * @return the registration id
   * @throws NoSuchEntityException if there is no registration
   * @throws IOException if the read fails
   */
  long getEntityId(NameIdentifier ident, Entity.EntityType entityType) throws IOException;

  /**
   * Deletes a registration only if its id still matches the observation.
   *
   * <p>The identity check and deletion must be atomic. Updates to the same identity are allowed;
   * implementations use the current row version to reject concurrent writes during deletion. A
   * cascade fences the root registration, not the identities of children created after observation.
   *
   * @param ident the entity identifier
   * @param entityType the entity type
   * @param cascade whether to delete children
   * @param expectedId the id observed before the external operation
   * @return true if the registration was deleted
   * @throws NoSuchEntityException if there is no registration
   * @throws OptimisticLockException if the registration has another id or the atomic write
   *     conflicts
   * @throws IOException if the delete fails
   */
  boolean deleteIfIdMatches(
      NameIdentifier ident, Entity.EntityType entityType, boolean cascade, long expectedId)
      throws IOException;

  /**
   * Requires the complete identity-fenced delete capability before an external operation.
   *
   * @param store the entity store or backend
   * @return the capability
   * @throws UnsupportedOperationException if the store or backend does not implement it
   */
  static SupportsIdentityFencedDelete require(Object store) {
    if (!(store instanceof SupportsIdentityFencedDelete)) {
      throw new UnsupportedOperationException("The store does not support identity-fenced deletes");
    }
    return (SupportsIdentityFencedDelete) store;
  }
}
