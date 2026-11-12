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
import java.util.function.LongConsumer;
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
   * Prepares an identity-conditional initial owner assignment before an object is created.
   *
   * <p>This call must check implementation support without writing an owner or holding database
   * resources across creation. After a successful create, invoke the returned writer once with the
   * Gravitino entity ID captured in the creation result. The writer must atomically fence that ID
   * and preserve any existing owner. Deleted objects and existing owners are successful no-ops;
   * principal and storage failures propagate. It must never re-resolve a replacement by name or
   * fall back to {@link #setOwner}. Explicit owner transfers continue to use setOwner.
   *
   * @param metalake the metalake name
   * @param metadataObject the object about to be created
   * @param ownerName the creator's user name
   * @return the writer that consumes the created Gravitino entity ID
   * @throws UnsupportedOperationException before creation if the implementation or object type
   *     cannot provide conditional initialization
   */
  default LongConsumer prepareInitialOwner(
      String metalake, MetadataObject metadataObject, String ownerName) {
    throw new UnsupportedOperationException(
        "Identity-conditional owner initialization is unsupported");
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
