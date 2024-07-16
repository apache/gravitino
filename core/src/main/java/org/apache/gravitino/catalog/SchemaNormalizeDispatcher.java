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
package org.apache.gravitino.catalog;

import static org.apache.gravitino.catalog.CapabilityHelpers.applyCapabilities;
import static org.apache.gravitino.catalog.CapabilityHelpers.applyCaseSensitive;

import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;

public class SchemaNormalizeDispatcher implements SchemaDispatcher {

  private final SchemaOperationDispatcher dispatcher;

  public SchemaNormalizeDispatcher(SchemaOperationDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  @Override
  public NameIdentifier[] listSchemas(Namespace namespace) throws NoSuchCatalogException {
    NameIdentifier[] identifiers = dispatcher.listSchemas(namespace);
    // The constraints of the name spec may be more strict than underlying catalog,
    // and for compatibility reasons, we only apply case-sensitive capabilities here.
    return applyCaseSensitive(identifiers, Capability.Scope.SCHEMA, dispatcher);
  }

  @Override
  public boolean schemaExists(NameIdentifier ident) {
    // The constraints of the name spec may be more strict than underlying catalog,
    // and for compatibility reasons, we only apply case-sensitive capabilities here.
    return dispatcher.schemaExists(applyCaseSensitive(ident, Capability.Scope.SCHEMA, dispatcher));
  }

  @Override
  public Schema createSchema(NameIdentifier ident, String comment, Map<String, String> properties)
      throws NoSuchCatalogException, SchemaAlreadyExistsException {
    return dispatcher.createSchema(normalizeNameIdentifier(ident), comment, properties);
  }

  @Override
  public Schema loadSchema(NameIdentifier ident) throws NoSuchSchemaException {
    // The constraints of the name spec may be more strict than underlying catalog,
    // and for compatibility reasons, we only apply case-sensitive capabilities here.
    return dispatcher.loadSchema(applyCaseSensitive(ident, Capability.Scope.SCHEMA, dispatcher));
  }

  @Override
  public Schema alterSchema(NameIdentifier ident, SchemaChange... changes)
      throws NoSuchSchemaException {
    // The constraints of the name spec may be more strict than underlying catalog,
    // and for compatibility reasons, we only apply case-sensitive capabilities here.
    return dispatcher.alterSchema(
        applyCaseSensitive(ident, Capability.Scope.SCHEMA, dispatcher), changes);
  }

  @Override
  public boolean dropSchema(NameIdentifier ident, boolean cascade) throws NonEmptySchemaException {
    return dispatcher.dropSchema(normalizeNameIdentifier(ident), cascade);
  }

  private NameIdentifier normalizeNameIdentifier(NameIdentifier ident) {
    Capability capability = dispatcher.getCatalogCapability(ident);
    return applyCapabilities(ident, Capability.Scope.SCHEMA, capability);
  }
}
