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
import java.util.List;
import java.util.Map;
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
import org.apache.gravitino.catalog.SchemaDispatcher;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.utils.HierarchicalSchemaUtil;
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
    // The inner NormalizeDispatcher case-folds the schema name based on catalog capabilities, so
    // the entity is stored under the normalized identifier. Normalize here too so ownership is
    // attached to the identifiers the manager sees and ancestor probing matches stored names.
    NameIdentifier normalizedIdent =
        CapabilityHelpers.applyCapabilities(
            ident, Capability.Scope.SCHEMA, GravitinoEnv.getInstance().catalogManager());

    // For a nested schema name (e.g. "A:B:C") the store auto-creates a row for each missing
    // ancestor ("A", "A:B"). Probe BEFORE the create which ancestors are new, so ownership is
    // assigned only to schemas this request actually creates and a pre-existing ancestor's owner
    // is never overwritten.
    List<NameIdentifier> newAncestors = findMissingAncestors(normalizedIdent);

    Schema schema = dispatcher.createSchema(ident, comment, properties);

    // Set the creator as the owner of the new schema and of any ancestors it created. This mirrors
    // IcebergNamespaceHookDispatcher.createNamespace so ownership-based authorization -- which
    // treats ownership of an ancestor schema as ownership of the whole subtree -- behaves the same
    // on the Gravitino and Iceberg REST surfaces. Unlike the Iceberg path we do not wrap this in a
    // tree lock: the inner SchemaOperationDispatcher.createSchema already holds a catalog-level
    // WRITE lock, so a branch-scoped lock here would invert lock ordering and risk deadlock. The
    // only residual race -- two concurrent creates of sibling nested schemas both claiming a
    // shared, newly-created ancestor -- resolves to last-writer-wins between two legitimate
    // creators, which is acceptable.
    OwnerDispatcher ownerManager = GravitinoEnv.getInstance().ownerDispatcher();
    if (ownerManager != null) {
      List<MetadataObject> ownedObjects = new ArrayList<>(newAncestors.size() + 1);
      for (NameIdentifier ancestor : newAncestors) {
        ownedObjects.add(NameIdentifierUtil.toMetadataObject(ancestor, Entity.EntityType.SCHEMA));
      }
      ownedObjects.add(
          NameIdentifierUtil.toMetadataObject(normalizedIdent, Entity.EntityType.SCHEMA));
      // All objects are SCHEMA-typed, so the batch path (which requires a single type) is valid.
      ownerManager.setOwners(
          normalizedIdent.namespace().level(0),
          ownedObjects,
          PrincipalUtils.getCurrentUserName(),
          Owner.Type.USER);
    }
    return schema;
  }

  /**
   * Returns the identifiers of the (already-normalized) ancestor schemas of {@code normalizedIdent}
   * that do not yet exist, ordered outermost-to-innermost. Returns an empty list for a flat (non
   * hierarchical) schema name.
   */
  private List<NameIdentifier> findMissingAncestors(NameIdentifier normalizedIdent) {
    String separator = HierarchicalSchemaUtil.schemaSeparator();
    String schemaName = normalizedIdent.name();
    List<NameIdentifier> missing = new ArrayList<>();
    if (!schemaName.contains(separator)) {
      return missing;
    }
    String metalake = normalizedIdent.namespace().level(0);
    String catalog = normalizedIdent.namespace().level(1);
    for (String ancestorName : HierarchicalSchemaUtil.getAncestorNames(schemaName, separator)) {
      NameIdentifier ancestorIdent = NameIdentifierUtil.ofSchema(metalake, catalog, ancestorName);
      if (!dispatcher.schemaExists(ancestorIdent)) {
        missing.add(ancestorIdent);
      }
    }
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
