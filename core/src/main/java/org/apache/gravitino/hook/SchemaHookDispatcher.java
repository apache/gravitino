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
package org.apache.gravitino.hook;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.catalog.CapabilityHelpers;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.catalog.HierarchicalSchemaUtil;
import org.apache.gravitino.catalog.SchemaDispatcher;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;

/**
 * {@code SchemaHookDispatcher} is a decorator for {@link SchemaDispatcher} that not only delegates
 * schema operations to the underlying schema dispatcher but also executes some hook operations
 * before or after the underlying operations.
 */
public class SchemaHookDispatcher implements SchemaDispatcher {
  private final SchemaDispatcher dispatcher;

  public SchemaHookDispatcher(SchemaDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  @Override
  public NameIdentifier[] listSchemas(Namespace namespace) throws NoSuchCatalogException {
    return dispatcher.listSchemas(namespace);
  }

  @Override
  public Schema createSchema(NameIdentifier ident, String comment, Map<String, String> properties)
      throws NoSuchCatalogException, SchemaAlreadyExistsException {
    // Collect missing parent identifiers BEFORE the underlying call; the catalog itself is
    // responsible for auto-creating them. We only need to assign ownership afterwards.
    List<NameIdentifier> missingParents = findMissingParents(ident);
    Schema schema = dispatcher.createSchema(ident, comment, properties);

    OwnerDispatcher ownerManager = GravitinoEnv.getInstance().ownerDispatcher();
    if (ownerManager != null) {
      CatalogManager catalogManager = GravitinoEnv.getInstance().catalogManager();
      String metalake = ident.namespace().level(0);
      String user = PrincipalUtils.getCurrentUserName();
      // Auto-created parent schemas (hierarchical namespace) and the new schema each need an
      // owner; outer-to-inner order matches parent-before-child creation.
      List<MetadataObject> toOwn = new ArrayList<>(missingParents.size() + 1);
      for (NameIdentifier parentIdent : missingParents) {
        NameIdentifier normalizedParent =
            CapabilityHelpers.applyCapabilities(
                parentIdent, Capability.Scope.SCHEMA, catalogManager);
        toOwn.add(NameIdentifierUtil.toMetadataObject(normalizedParent, Entity.EntityType.SCHEMA));
      }
      toOwn.add(
          NameIdentifierUtil.toMetadataObject(
              CapabilityHelpers.applyCapabilities(ident, Capability.Scope.SCHEMA, catalogManager),
              Entity.EntityType.SCHEMA));
      ownerManager.setOwners(metalake, toOwn, user, Owner.Type.USER);
    }
    return schema;
  }

  /**
   * Returns ancestor schema identifiers for {@code ident} that do not currently exist. The catalog
   * will auto-create them during {@link #createSchema}; we collect them here only so we can assign
   * ownership after the fact.
   *
   * <p>Walks from the nearest parent outward. If a parent exists, more distant ancestors must exist
   * too, so lookup stops. Uses {@link org.apache.gravitino.EntityStore#batchGet} once for store
   * state; any ancestor not in that result is checked with {@link #schemaExists(NameIdentifier)}.
   */
  private List<NameIdentifier> findMissingParents(NameIdentifier ident) {
    String separator = HierarchicalSchemaUtil.schemaSeparator();
    List<String> ancestorNames = HierarchicalSchemaUtil.getAncestorNames(ident.name(), separator);
    if (ancestorNames.isEmpty()) {
      return new ArrayList<>();
    }

    List<NameIdentifier> ancestorIdents = new ArrayList<>(ancestorNames.size());
    for (String ancestorName : ancestorNames) {
      ancestorIdents.add(NameIdentifier.of(ident.namespace(), ancestorName));
    }

    CatalogManager catalogManager = GravitinoEnv.getInstance().catalogManager();
    Set<String> foundNormalizedNames = new HashSet<>();
    for (SchemaEntity schemaEntity :
        GravitinoEnv.getInstance()
            .entityStore()
            .batchGet(ancestorIdents, Entity.EntityType.SCHEMA, SchemaEntity.class)) {
      NameIdentifier storeIdent = NameIdentifier.of(ident.namespace(), schemaEntity.name());
      Capability cap = CapabilityHelpers.getCapability(storeIdent, catalogManager);
      foundNormalizedNames.add(
          CapabilityHelpers.applyCaseSensitive(storeIdent, Capability.Scope.SCHEMA, cap).name());
    }

    List<NameIdentifier> missing = new ArrayList<>();
    for (int i = ancestorIdents.size() - 1; i >= 0; i--) {
      NameIdentifier ancestorIdent = ancestorIdents.get(i);
      Capability cap = CapabilityHelpers.getCapability(ancestorIdent, catalogManager);
      String normalizedName =
          CapabilityHelpers.applyCaseSensitive(ancestorIdent, Capability.Scope.SCHEMA, cap).name();
      boolean exists =
          foundNormalizedNames.contains(normalizedName) || dispatcher.schemaExists(ancestorIdent);
      if (exists) {
        break;
      }
      missing.add(ancestorIdent);
    }
    Collections.reverse(missing);
    return missing;
  }

  @Override
  public Schema loadSchema(NameIdentifier ident) throws NoSuchSchemaException {
    return dispatcher.loadSchema(ident);
  }

  @Override
  public Schema alterSchema(NameIdentifier ident, SchemaChange... changes)
      throws NoSuchSchemaException {
    // Schema doesn't support to rename operation now. So we don't need to change
    // authorization plugin privileges, too.
    return dispatcher.alterSchema(ident, changes);
  }

  @Override
  public boolean dropSchema(NameIdentifier ident, boolean cascade) throws NonEmptySchemaException {
    List<String> locations =
        AuthorizationUtils.getMetadataObjectLocation(ident, Entity.EntityType.SCHEMA);
    boolean dropped = dispatcher.dropSchema(ident, cascade);
    AuthorizationUtils.authorizationPluginRemovePrivileges(
        ident, Entity.EntityType.SCHEMA, locations);
    return dropped;
  }

  @Override
  public boolean schemaExists(NameIdentifier ident) {
    return dispatcher.schemaExists(ident);
  }
}
