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

package org.apache.gravitino.server.authorization;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.CapabilityHelpers;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.utils.EntityClassMapper;
import org.apache.gravitino.utils.MetadataObjectUtil;

/** It is used to convert MetadataObject to MetadataId */
public class MetadataIdConverter {

  // Maps metadata type to capability scope
  private static final Map<MetadataObject.Type, Capability.Scope> METADATA_SCOPE_MAPPING =
      ImmutableMap.of(
          MetadataObject.Type.SCHEMA, Capability.Scope.SCHEMA,
          MetadataObject.Type.TABLE, Capability.Scope.TABLE,
          MetadataObject.Type.MODEL, Capability.Scope.MODEL,
          MetadataObject.Type.FILESET, Capability.Scope.FILESET,
          MetadataObject.Type.TOPIC, Capability.Scope.TOPIC,
          MetadataObject.Type.COLUMN, Capability.Scope.COLUMN);

  private MetadataIdConverter() {}

  /**
   * Converts the given metadata object to metadata id.
   *
   * @param metadataObject The metadata object to convert.
   * @param metalake The metalake name.
   * @return The metadata id.
   */
  public static Long getID(MetadataObject metadataObject, String metalake) {
    Preconditions.checkArgument(metadataObject != null, "Metadata object cannot be null");
    EntityStore entityStore = GravitinoEnv.getInstance().entityStore();
    CatalogManager catalogManager = GravitinoEnv.getInstance().catalogManager();

    MetadataObject.Type metadataType = metadataObject.type();
    NameIdentifier ident = MetadataObjectUtil.toEntityIdent(metalake, metadataObject);

    NameIdentifier normalizedIdent =
        normalizeCaseSensitive(ident, METADATA_SCOPE_MAPPING.get(metadataType), catalogManager);

    Entity.EntityType entityType = MetadataObjectUtil.toEntityType(metadataType);

    Entity entity;
    try {
      entity =
          entityStore.get(
              normalizedIdent, entityType, EntityClassMapper.getEntityClass(entityType));
    } catch (IOException e) {
      throw new RuntimeException(
          "failed to load entity from entity store: " + metadataObject.fullName(), e);
    }

    return extractIdFromEntity(entity);
  }

  @VisibleForTesting
  static NameIdentifier normalizeCaseSensitive(
      NameIdentifier ident, Capability.Scope scope, CatalogManager catalogManager) {
    if (scope == null) {
      return ident;
    }

    Capability capability = CapabilityHelpers.getCapability(ident, catalogManager);
    return CapabilityHelpers.applyCaseSensitive(ident, scope, capability);
  }

  private static Long extractIdFromEntity(Entity entity) {
    Preconditions.checkArgument(
        entity instanceof HasIdentifier, "Entity must implement HasIdentifier interface");

    return ((HasIdentifier) entity).id();
  }
}
