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
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.CapabilityHelpers;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.NameIdentifierUtil;

/**
 * Normalizes user-supplied metadata object names before ownership operations. Internal create hooks
 * bypass this dispatcher because their names have already been normalized.
 */
public class OwnerNormalizeDispatcher implements OwnerDispatcher {
  private final OwnerDispatcher dispatcher;
  private final CatalogManager catalogManager;
  private final EntityStore entityStore;

  /**
   * Creates a dispatcher for ownership requests.
   *
   * @param dispatcher the dispatcher that reads and writes ownership
   * @param catalogManager the manager supplying catalog name rules
   * @param entityStore the store used to preserve exact column names
   */
  public OwnerNormalizeDispatcher(
      OwnerDispatcher dispatcher, CatalogManager catalogManager, EntityStore entityStore) {
    this.dispatcher = dispatcher;
    this.catalogManager = catalogManager;
    this.entityStore = entityStore;
  }

  /** {@inheritDoc} */
  @Override
  public void setOwner(
      String metalake, MetadataObject metadataObject, String ownerName, Owner.Type ownerType) {
    dispatcher.setOwner(metalake, normalize(metalake, metadataObject), ownerName, ownerType);
  }

  /** {@inheritDoc} */
  @Override
  public void setOwners(
      String metalake,
      List<MetadataObject> metadataObjects,
      String ownerName,
      Owner.Type ownerType) {
    Preconditions.checkArgument(metadataObjects != null, "metadataObjects must not be null");
    if (metadataObjects.isEmpty()) {
      return;
    }
    // Resolve the whole batch before writing. Equivalent spellings must not insert the same
    // owner relation twice in the underlying batch operation.
    LinkedHashSet<MetadataObject> normalized = new LinkedHashSet<>();
    for (MetadataObject object : metadataObjects) {
      normalized.add(normalize(metalake, object));
    }
    dispatcher.setOwners(metalake, new ArrayList<>(normalized), ownerName, ownerType);
  }

  /** {@inheritDoc} */
  @Override
  public Optional<Owner> getOwner(String metalake, MetadataObject metadataObject) {
    return dispatcher.getOwner(metalake, normalize(metalake, metadataObject));
  }

  private MetadataObject normalize(String metalake, MetadataObject object) {
    Preconditions.checkArgument(object != null, "metadataObject cannot be null");
    switch (object.type()) {
      case SCHEMA:
      case TABLE:
      case COLUMN:
      case VIEW:
      case FUNCTION:
      case FILESET:
      case TOPIC:
      case MODEL:
      case SEMANTIC_MODEL:
        NameIdentifier identifier = MetadataObjectUtil.toEntityIdent(metalake, object);
        NameIdentifier normalized =
            CapabilityHelpers.normalizeMetadataIdentifier(
                identifier,
                Capability.Scope.valueOf(object.type().name()),
                catalogManager,
                () -> entityStore);
        return normalized.equals(identifier)
            ? object
            : NameIdentifierUtil.toMetadataObject(
                normalized, MetadataObjectUtil.toEntityType(object));
      default:
        return object;
    }
  }
}
