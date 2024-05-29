/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog;

import static com.datastrato.gravitino.catalog.CapabilityHelpers.applyCapabilities;
import static com.datastrato.gravitino.catalog.CapabilityHelpers.applyCaseSensitive;

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
    // The constraints of the name spec may be more strict than underlying catalog,
    // and for compatibility reasons, we only apply case-sensitive capabilities here.
    return dispatcher.dropSchema(
        applyCaseSensitive(ident, Capability.Scope.SCHEMA, dispatcher), cascade);
  }

  @Override
  public boolean importSchema(NameIdentifier identifier) {
    return dispatcher.importSchema(
        applyCaseSensitive(identifier, Capability.Scope.SCHEMA, dispatcher));
  }

  private NameIdentifier normalizeNameIdentifier(NameIdentifier ident) {
    Capability capability = dispatcher.getCatalogCapability(ident);
    return applyCapabilities(ident, Capability.Scope.SCHEMA, capability);
  }
}
