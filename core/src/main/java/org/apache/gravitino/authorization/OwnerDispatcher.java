/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.authorization;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Optional;
import org.apache.gravitino.MetadataObject;

/** Dispatcher interface for managing owners of metadata objects. */
public interface OwnerDispatcher {

  /**
   * Sets the owner of a metadata object.
   *
   * @param metalake the name of the metalake
   * @param metadataObject the metadata object for which the owner is being set
   * @param ownerName the name of the owner
   * @param ownerType the type of the owner (e.g., USER, GROUP)
   */
  void setOwner(
      String metalake, MetadataObject metadataObject, String ownerName, Owner.Type ownerType);

  /**
   * Whether identity-fenced initial table ownership is supported.
   *
   * <p>Post-create callers must check this before creating the table, so an old implementation
   * fails before any external or metadata mutation. The default is false.
   *
   * @return whether {@link #setInitialTableOwner} is supported
   */
  default boolean supportsInitialTableOwner() {
    return false;
  }

  /**
   * Sets the first owner after table creation, fenced by the created table's ID.
   *
   * <p>This internal post-create operation runs after creation commits, outside a transaction. It
   * returns false without modifying ownership if the table was dropped/replaced or already has an
   * owner. Principal, storage and notification failures propagate. Unsupported implementations fail
   * before writing; there is deliberately no fallback to name-only {@link #setOwner}.
   *
   * @param metalake the metalake name
   * @param table the table metadata object
   * @param tableId the stable ID returned by creation
   * @param ownerName the principal name
   * @param ownerType the principal type
   * @return true if ownership was initialized, false if initialization was no longer needed
   * @throws UnsupportedOperationException if this capability is unsupported
   */
  default boolean setInitialTableOwner(
      String metalake, MetadataObject table, long tableId, String ownerName, Owner.Type ownerType) {
    throw new UnsupportedOperationException(
        "Identity-fenced initial table ownership is unsupported");
  }

  /**
   * Sets the same owner on multiple metadata objects in iteration order. Semantics match repeated
   * {@link #setOwner} calls; relational {@link org.apache.gravitino.EntityStore} implementations
   * may persist using {@link org.apache.gravitino.SupportsRelationOperations#batchInsertRelations}.
   * The default implementation delegates per object so implementations that wrap {@code setOwner}
   * (e.g. for events) apply consistently.
   *
   * <p>Note: implementations that persist via {@code batchInsertRelations} (such as {@link
   * OwnerManager}) require all {@code metadataObjects} to share the same {@link
   * MetadataObject.Type} within a single call, because the batch operation takes a single object
   * type. Such implementations throw {@link IllegalArgumentException} when mixed types are
   * supplied, so callers should group objects by type before calling. The default per-object
   * implementation has no such restriction.
   *
   * @param metalake the name of the metalake
   * @param metadataObjects the objects to update; must not be null (may be empty)
   * @param ownerName the name of the owner
   * @param ownerType the type of the owner (e.g., USER, GROUP)
   */
  default void setOwners(
      String metalake,
      List<MetadataObject> metadataObjects,
      String ownerName,
      Owner.Type ownerType) {
    Preconditions.checkArgument(metadataObjects != null, "metadataObjects must not be null");
    for (MetadataObject metadataObject : metadataObjects) {
      setOwner(metalake, metadataObject, ownerName, ownerType);
    }
  }

  /**
   * Retrieves the owner of a metadata object.
   *
   * @param metalake the name of the metalake
   * @param metadataObject the metadata object for which the owner is being retrieved
   * @return an Optional containing the owner if it exists, otherwise an empty Optional
   */
  Optional<Owner> getOwner(String metalake, MetadataObject metadataObject);
}
