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
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.meta.GlossaryTermNames;

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
   * Sets the same owner on multiple metadata objects in iteration order. Semantics match repeated
   * {@link #setOwner} calls; relational {@link org.apache.gravitino.EntityStore} implementations
   * may persist using {@link org.apache.gravitino.SupportsRelationOperations#batchInsertRelations}.
   * The default implementation delegates per object so implementations that wrap {@code setOwner}
   * (e.g. for events) apply consistently. Equivalent glossary term names refer to one transfer,
   * using the first spelling supplied. For top-level calls, earlier term transfers remain committed
   * if a later transfer fails. In a caller-owned transaction, individual calls can return and
   * dispatch events before the caller commits or rolls back.
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
    Set<String> glossaryNames = new HashSet<>();
    for (MetadataObject metadataObject : metadataObjects) {
      if (metadataObject.type() == MetadataObject.Type.GLOSSARY_TERM
          && !glossaryNames.add(GlossaryTermNames.key(metadataObject.name()))) {
        continue;
      }
      setOwner(metalake, metadataObject, ownerName, ownerType);
    }
  }

  /**
   * Retrieves the owner of a metadata object. For glossary terms, the relational implementation
   * checks visibility and reads ownership in one snapshot, started outside a caller transaction.
   *
   * @param metalake the name of the metalake
   * @param metadataObject the metadata object for which the owner is being retrieved
   * @return an Optional containing the owner if it exists, otherwise an empty Optional
   * @throws ForbiddenException if a glossary term is hidden from the caller or does not exist;
   *     reads do not distinguish the two
   */
  Optional<Owner> getOwner(String metalake, MetadataObject metadataObject);
}
