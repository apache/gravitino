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
import com.google.common.collect.ObjectArrays;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.CapabilityHelpers;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.utils.EntityClassMapper;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.NameIdentifierUtil;

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
          MetadataObject.Type.COLUMN, Capability.Scope.COLUMN,
          MetadataObject.Type.SEMANTIC_MODEL, Capability.Scope.SEMANTIC_MODEL);

  private MetadataIdConverter() {}

  /**
   * Converts the given metadata object to metadata id.
   *
   * @param metadataObject The metadata object to convert.
   * @param metalake The metalake name.
   * @return The metadata id, empty if metadata does not exist.
   */
  public static Optional<Long> getID(MetadataObject metadataObject, String metalake) {
    Preconditions.checkArgument(metadataObject != null, "Metadata object cannot be null");
    EntityStore entityStore = GravitinoEnv.getInstance().entityStore();
    CatalogManager catalogManager = GravitinoEnv.getInstance().catalogManager();

    MetadataObject.Type metadataType = metadataObject.type();
    NameIdentifier ident = MetadataObjectUtil.toEntityIdent(metalake, metadataObject);

    NameIdentifier normalizedIdent;
    try {
      normalizedIdent =
          normalizeCaseSensitive(ident, METADATA_SCOPE_MAPPING.get(metadataType), catalogManager);
    } catch (NotFoundException e) {
      return Optional.empty();
    }

    Entity.EntityType entityType = MetadataObjectUtil.toEntityType(metadataType);

    Entity entity;
    try {
      entity =
          entityStore.get(
              normalizedIdent, entityType, EntityClassMapper.getEntityClass(entityType));
    } catch (NoSuchEntityException nse) {
      return Optional.empty();
    } catch (IOException e) {
      throw new RuntimeException(
          "failed to load entity from entity store: " + metadataObject.fullName(), e);
    }

    return Optional.of(extractIdFromEntity(entity));
  }

  /**
   * Normalizes a metadata object's name using the same catalog rules as ID resolution.
   *
   * <p>Types without a catalog capability scope retain their names. Semantic model leaves remain
   * case sensitive, while column names and their table/schema parents follow their own scopes.
   *
   * @param metadataObject the object whose name will be normalized
   * @param metalake the metalake name
   * @return the normalized object, suitable for name-to-ID cache keys
   * @throws NotFoundException if the containing catalog does not exist
   */
  public static MetadataObject normalizeMetadataObject(
      MetadataObject metadataObject, String metalake) {
    Preconditions.checkArgument(metadataObject != null, "Metadata object cannot be null");
    Capability.Scope scope = METADATA_SCOPE_MAPPING.get(metadataObject.type());
    if (scope == null) {
      return metadataObject;
    }
    NameIdentifier ident = MetadataObjectUtil.toEntityIdent(metalake, metadataObject);
    NameIdentifier normalized =
        normalizeIdentifier(ident, scope, GravitinoEnv.getInstance().catalogManager());
    if (normalized.equals(ident)) {
      return metadataObject;
    }
    return NameIdentifierUtil.toMetadataObject(
        normalized, MetadataObjectUtil.toEntityType(metadataObject));
  }

  /**
   * Returns the immutable set of metadata types with catalog-scoped normalization rules.
   *
   * @return the types covered by the production capability mapping
   */
  @VisibleForTesting
  public static Set<MetadataObject.Type> catalogScopedTypes() {
    return METADATA_SCOPE_MAPPING.keySet();
  }

  @VisibleForTesting
  static NameIdentifier normalizeCaseSensitive(
      NameIdentifier ident, Capability.Scope scope, CatalogManager catalogManager) {
    return normalizeIdentifier(ident, scope, catalogManager);
  }

  private static NameIdentifier normalizeIdentifier(
      NameIdentifier ident, Capability.Scope scope, CatalogManager catalogManager) {
    if (scope == null) {
      return ident;
    }

    Capability capability = CapabilityHelpers.getCapability(ident, catalogManager);
    if (scope == Capability.Scope.COLUMN) {
      // The NameIdentifier overload applies SCHEMA to the namespace and TABLE to the leaf.
      NameIdentifier table =
          CapabilityHelpers.applyCaseSensitive(
              NameIdentifier.of(ident.namespace().levels()), Capability.Scope.TABLE, capability);
      return NameIdentifier.of(
          Namespace.of(ObjectArrays.concat(table.namespace().levels(), table.name())),
          CapabilityHelpers.applyCaseSensitiveOnName(scope, ident.name(), capability));
    }
    if (scope == Capability.Scope.SEMANTIC_MODEL) {
      return NameIdentifier.of(
          CapabilityHelpers.applyCaseSensitive(ident.namespace(), scope, capability), ident.name());
    }
    return CapabilityHelpers.applyCaseSensitive(ident, scope, capability);
  }

  private static Long extractIdFromEntity(Entity entity) {
    Preconditions.checkArgument(
        entity instanceof HasIdentifier, "Entity must implement HasIdentifier interface");

    return ((HasIdentifier) entity).id();
  }
}
