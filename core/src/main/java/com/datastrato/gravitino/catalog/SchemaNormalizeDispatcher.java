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
package com.datastrato.gravitino.catalog;

import static com.datastrato.gravitino.catalog.CapabilityHelpers.applyCapabilities;
import static com.datastrato.gravitino.catalog.CapabilityHelpers.applyCaseSensitive;
import static com.datastrato.gravitino.catalog.CapabilityHelpers.getCapability;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.Schema;
import com.datastrato.gravitino.SchemaChange;
import com.datastrato.gravitino.connector.capability.Capability;
import com.datastrato.gravitino.exceptions.NoSuchCatalogException;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NonEmptySchemaException;
import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import java.util.Map;

public class SchemaNormalizeDispatcher implements SchemaDispatcher {
  private final CatalogManager catalogManager;

  private SchemaDispatcher dispatcher;

  public SchemaNormalizeDispatcher(CatalogManager catalogManager) {
    this.catalogManager = catalogManager;
  }

  @Override
  public SchemaDispatcher wrap(SchemaDispatcher schemaDispatcher) {
    this.dispatcher = schemaDispatcher;
    return dispatcher;
  }

  @Override
  public SchemaDispatcher delegate() {
    return dispatcher;
  }

  @Override
  public NameIdentifier[] listSchemas(Namespace namespace) throws NoSuchCatalogException {
    NameIdentifier[] identifiers = delegate().listSchemas(namespace);
    // The constraints of the name spec may be more strict than underlying catalog,
    // and for compatibility reasons, we only apply case-sensitive capabilities here.
    return normalizeCaseSensitive(identifiers);
  }

  @Override
  public boolean schemaExists(NameIdentifier ident) {
    // The constraints of the name spec may be more strict than underlying catalog,
    // and for compatibility reasons, we only apply case-sensitive capabilities here.
    return delegate().schemaExists(normalizeCaseSensitive(ident));
  }

  @Override
  public Schema createSchema(NameIdentifier ident, String comment, Map<String, String> properties)
      throws NoSuchCatalogException, SchemaAlreadyExistsException {
    return delegate().createSchema(normalizeNameIdentifier(ident), comment, properties);
  }

  @Override
  public Schema loadSchema(NameIdentifier ident) throws NoSuchSchemaException {
    // The constraints of the name spec may be more strict than underlying catalog,
    // and for compatibility reasons, we only apply case-sensitive capabilities here.
    return delegate().loadSchema(normalizeCaseSensitive(ident));
  }

  @Override
  public Schema alterSchema(NameIdentifier ident, SchemaChange... changes)
      throws NoSuchSchemaException {
    // The constraints of the name spec may be more strict than underlying catalog,
    // and for compatibility reasons, we only apply case-sensitive capabilities here.
    return delegate().alterSchema(normalizeCaseSensitive(ident), changes);
  }

  @Override
  public boolean dropSchema(NameIdentifier ident, boolean cascade) throws NonEmptySchemaException {
    return delegate().dropSchema(normalizeNameIdentifier(ident), cascade);
  }

  private NameIdentifier normalizeNameIdentifier(NameIdentifier schemaIdent) {
    Capability capabilities =
        getCapability(NameIdentifier.of(schemaIdent.namespace().levels()), catalogManager);
    return applyCapabilities(schemaIdent, Capability.Scope.SCHEMA, capabilities);
  }

  private NameIdentifier normalizeCaseSensitive(NameIdentifier schemaIdent) {
    Capability capabilities =
        getCapability(NameIdentifier.of(schemaIdent.namespace().levels()), catalogManager);
    return applyCaseSensitive(schemaIdent, Capability.Scope.SCHEMA, capabilities);
  }

  private NameIdentifier[] normalizeCaseSensitive(NameIdentifier[] schemaIdents) {
    Capability capabilities =
        getCapability(NameIdentifier.of(schemaIdents[0].namespace().levels()), catalogManager);
    return applyCaseSensitive(schemaIdents, Capability.Scope.SCHEMA, capabilities);
  }
}
