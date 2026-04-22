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
import java.util.List;
import java.util.Map;
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
    List<NameIdentifier> createdParents = createMissingParents(ident);
    Schema schema = dispatcher.createSchema(ident, comment, properties);

    String metalake = ident.namespace().level(0);
    OwnerDispatcher ownerManager = GravitinoEnv.getInstance().ownerDispatcher();
    if (ownerManager != null) {
      String creator = PrincipalUtils.getCurrentUserName();
      for (NameIdentifier parent : createdParents) {
        ownerManager.setOwner(
            metalake,
            NameIdentifierUtil.toMetadataObject(parent, Entity.EntityType.SCHEMA),
            creator,
            Owner.Type.USER);
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
   * Creates any missing ancestor schemas for the given schema identifier and returns the list of
   * newly created parent identifiers (outermost first). Missing ancestors are created with empty
   * comment and properties.
   */
  private List<NameIdentifier> createMissingParents(NameIdentifier ident) {
    String separator = HierarchicalSchemaUtil.namespaceSeparator();
    List<String> ancestorNames = HierarchicalSchemaUtil.getAncestorNames(ident.name(), separator);
    if (ancestorNames.isEmpty()) {
      return Collections.emptyList();
    }
    List<NameIdentifier> created = new ArrayList<>();
    for (String ancestorName : ancestorNames) {
      NameIdentifier ancestorIdent = NameIdentifier.of(ident.namespace(), ancestorName);
      if (!dispatcher.schemaExists(ancestorIdent)) {
        dispatcher.createSchema(ancestorIdent, null, Collections.emptyMap());
        created.add(ancestorIdent);
      }
    }
    return created;
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
