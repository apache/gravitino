/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog;

import static com.datastrato.gravitino.catalog.CapabilityHelpers.applyCapabilities;
import static com.datastrato.gravitino.catalog.CapabilityHelpers.applyCaseSensitive;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.connector.capability.Capability;
import com.datastrato.gravitino.exceptions.FilesetAlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchFilesetException;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.file.Fileset;
import com.datastrato.gravitino.file.FilesetChange;
import java.util.Map;

public class FilesetNormalizeDispatcher implements FilesetDispatcher {

  private final FilesetOperationDispatcher dispatcher;

  public FilesetNormalizeDispatcher(FilesetOperationDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  @Override
  public NameIdentifier[] listFilesets(Namespace namespace) throws NoSuchSchemaException {
    // The constraints of the name spec may be more strict than underlying catalog,
    // and for compatibility reasons, we only apply case-sensitive capabilities here.
    Namespace caseSensitiveNs = applyCaseSensitive(namespace, Capability.Scope.FILESET, dispatcher);
    NameIdentifier[] identifiers = dispatcher.listFilesets(caseSensitiveNs);
    return applyCaseSensitive(identifiers, Capability.Scope.FILESET, dispatcher);
  }

  @Override
  public Fileset loadFileset(NameIdentifier ident) throws NoSuchFilesetException {
    // The constraints of the name spec may be more strict than underlying catalog,
    // and for compatibility reasons, we only apply case-sensitive capabilities here.
    return dispatcher.loadFileset(applyCaseSensitive(ident, Capability.Scope.FILESET, dispatcher));
  }

  @Override
  public boolean filesetExists(NameIdentifier ident) {
    // The constraints of the name spec may be more strict than underlying catalog,
    // and for compatibility reasons, we only apply case-sensitive capabilities here.
    return dispatcher.filesetExists(
        applyCaseSensitive(ident, Capability.Scope.FILESET, dispatcher));
  }

  @Override
  public Fileset createFileset(
      NameIdentifier ident,
      String comment,
      Fileset.Type type,
      String storageLocation,
      Map<String, String> properties)
      throws NoSuchSchemaException, FilesetAlreadyExistsException {
    return dispatcher.createFileset(
        normalizeNameIdentifier(ident), comment, type, storageLocation, properties);
  }

  @Override
  public Fileset alterFileset(NameIdentifier ident, FilesetChange... changes)
      throws NoSuchFilesetException, IllegalArgumentException {
    Capability capability = dispatcher.getCatalogCapability(ident);
    return dispatcher.alterFileset(
        // The constraints of the name spec may be more strict than underlying catalog,
        // and for compatibility reasons, we only apply case-sensitive capabilities here.
        applyCaseSensitive(ident, Capability.Scope.FILESET, dispatcher),
        applyCapabilities(capability, changes));
  }

  @Override
  public boolean dropFileset(NameIdentifier ident) {
    // The constraints of the name spec may be more strict than underlying catalog,
    // and for compatibility reasons, we only apply case-sensitive capabilities here.
    return dispatcher.dropFileset(applyCaseSensitive(ident, Capability.Scope.FILESET, dispatcher));
  }

  private NameIdentifier normalizeNameIdentifier(NameIdentifier ident) {
    Capability capability = dispatcher.getCatalogCapability(ident);
    return applyCapabilities(ident, Capability.Scope.FILESET, capability);
  }
}
