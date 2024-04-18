/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog;

import static com.datastrato.gravitino.catalog.CapabilityHelpers.applyCapabilities;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.connector.capability.Capability;
import com.datastrato.gravitino.exceptions.NoSuchCatalogException;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.exceptions.NonEmptySchemaException;
import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import com.datastrato.gravitino.rel.Schema;
import com.datastrato.gravitino.rel.SchemaChange;
import java.util.Map;

public class SchemaStandardizedDispatcher implements SchemaDispatcher {

  private final SchemaOperationDispatcher dispatcher;

  public SchemaStandardizedDispatcher(SchemaOperationDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  @Override
  public NameIdentifier[] listSchemas(Namespace namespace) throws NoSuchCatalogException {
    Capability capability = dispatcher.getCatalogCapability(namespace);
    Namespace standardizedNamespace =
        applyCapabilities(namespace, Capability.Scope.SCHEMA, capability);
    NameIdentifier[] identifiers = dispatcher.listSchemas(standardizedNamespace);
    return applyCapabilities(identifiers, Capability.Scope.SCHEMA, capability);
  }

  @Override
  public boolean schemaExists(NameIdentifier ident) {
    return dispatcher.schemaExists(standardizeNameIdentifier(ident));
  }

  @Override
  public Schema createSchema(NameIdentifier ident, String comment, Map<String, String> properties)
      throws NoSuchCatalogException, SchemaAlreadyExistsException {
    return dispatcher.createSchema(standardizeNameIdentifier(ident), comment, properties);
  }

  @Override
  public Schema loadSchema(NameIdentifier ident) throws NoSuchSchemaException {
    return dispatcher.loadSchema(standardizeNameIdentifier(ident));
  }

  @Override
  public Schema alterSchema(NameIdentifier ident, SchemaChange... changes)
      throws NoSuchSchemaException {
    return dispatcher.alterSchema(standardizeNameIdentifier(ident), changes);
  }

  @Override
  public boolean dropSchema(NameIdentifier ident, boolean cascade) throws NonEmptySchemaException {
    return dispatcher.dropSchema(standardizeNameIdentifier(ident), cascade);
  }

  private NameIdentifier standardizeNameIdentifier(NameIdentifier ident) {
    Capability capability = dispatcher.getCatalogCapability(ident);
    return applyCapabilities(ident, Capability.Scope.SCHEMA, capability);
  }
}
