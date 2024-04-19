/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog;

import static com.datastrato.gravitino.catalog.CapabilityHelpers.applyCapabilities;

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
    Capability capability = dispatcher.getCatalogCapability(namespace);
    Namespace standardizedNamespace =
        applyCapabilities(namespace, Capability.Scope.FILESET, capability);
    NameIdentifier[] identifiers = dispatcher.listFilesets(standardizedNamespace);
    return applyCapabilities(identifiers, Capability.Scope.FILESET, capability);
  }

  @Override
  public Fileset loadFileset(NameIdentifier ident) throws NoSuchFilesetException {
    return dispatcher.loadFileset(normalizeNameIdentifier(ident));
  }

  @Override
  public boolean filesetExists(NameIdentifier ident) {
    return dispatcher.filesetExists(normalizeNameIdentifier(ident));
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
        applyCapabilities(ident, Capability.Scope.FILESET, capability),
        applyCapabilities(capability, changes));
  }

  @Override
  public boolean dropFileset(NameIdentifier ident) {
    return dispatcher.dropFileset(normalizeNameIdentifier(ident));
  }

  private NameIdentifier normalizeNameIdentifier(NameIdentifier ident) {
    Capability capability = dispatcher.getCatalogCapability(ident);
    return applyCapabilities(ident, Capability.Scope.FILESET, capability);
  }
}
