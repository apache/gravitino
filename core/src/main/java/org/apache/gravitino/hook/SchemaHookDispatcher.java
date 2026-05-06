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
import java.util.Optional;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.catalog.HierarchicalSchemaUtil;
import org.apache.gravitino.catalog.SchemaDispatcher;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
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

    String metalake = ident.namespace().level(0);
    OwnerDispatcher ownerManager = GravitinoEnv.getInstance().ownerDispatcher();
    if (ownerManager != null) {
      String creator = PrincipalUtils.getCurrentUserName();
      for (NameIdentifier parent : missingParents) {
        if (dispatcher.schemaExists(parent)) {
          // Parent schemas can be concurrently created by another request between the pre-check and
          // this point. Only assign owner when there is no owner to avoid overwriting ownership.
          if (canSetOwner(ownerManager, metalake, parent)) {
            ownerManager.setOwner(
                metalake,
                NameIdentifierUtil.toMetadataObject(parent, Entity.EntityType.SCHEMA),
                creator,
                Owner.Type.USER);
          }
        }
      }
      ownerManager.setOwner(
          metalake,
          NameIdentifierUtil.toMetadataObject(ident, Entity.EntityType.SCHEMA),
          creator,
          Owner.Type.USER);
    }
    return schema;
  }

  /**
   * Returns ancestor schema identifiers for {@code ident} that do not currently exist. The catalog
   * will auto-create them during {@link #createSchema}; we collect them here only so we can assign
   * ownership after the fact.
   */
  private List<NameIdentifier> findMissingParents(NameIdentifier ident) {
    String separator = HierarchicalSchemaUtil.schemaSeparator();
    List<String> ancestorNames = HierarchicalSchemaUtil.getAncestorNames(ident.name(), separator);
    List<NameIdentifier> missing = new ArrayList<>();
    for (String ancestorName : ancestorNames) {
      NameIdentifier ancestorIdent = NameIdentifier.of(ident.namespace(), ancestorName);
      if (!dispatcher.schemaExists(ancestorIdent)) {
        missing.add(ancestorIdent);
      }
    }
    return missing;
  }

  private boolean canSetOwner(OwnerDispatcher ownerManager, String metalake, NameIdentifier ident) {
    Optional<Owner> owner =
        ownerManager.getOwner(
            metalake, NameIdentifierUtil.toMetadataObject(ident, Entity.EntityType.SCHEMA));
    return !owner.isPresent();
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
