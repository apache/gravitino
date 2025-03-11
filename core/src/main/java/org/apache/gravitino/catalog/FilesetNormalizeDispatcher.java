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
import static org.apache.gravitino.catalog.CapabilityHelpers.getCapability;

import java.util.Map;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.exceptions.FilesetAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchFilesetException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.file.FilesetChange;

public class FilesetNormalizeDispatcher implements FilesetDispatcher {
  private final CatalogManager catalogManager;
  private final FilesetDispatcher dispatcher;

  public FilesetNormalizeDispatcher(FilesetDispatcher dispatcher, CatalogManager catalogManager) {
    this.dispatcher = dispatcher;
    this.catalogManager = catalogManager;
  }

  @Override
  public NameIdentifier[] listFilesets(Namespace namespace) throws NoSuchSchemaException {
    // The constraints of the name spec may be more strict than underlying catalog,
    // and for compatibility reasons, we only apply case-sensitive capabilities here.
    Namespace caseSensitiveNs = normalizeCaseSensitive(namespace);
    NameIdentifier[] identifiers = dispatcher.listFilesets(caseSensitiveNs);
    return normalizeCaseSensitive(identifiers);
  }

  @Override
  public Fileset loadFileset(NameIdentifier ident) throws NoSuchFilesetException {
    // The constraints of the name spec may be more strict than underlying catalog,
    // and for compatibility reasons, we only apply case-sensitive capabilities here.
    return dispatcher.loadFileset(normalizeCaseSensitive(ident));
  }

  @Override
  public boolean filesetExists(NameIdentifier ident) {
    // The constraints of the name spec may be more strict than underlying catalog,
    // and for compatibility reasons, we only apply case-sensitive capabilities here.
    return dispatcher.filesetExists(normalizeCaseSensitive(ident));
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
    Capability capability = getCapability(ident, catalogManager);
    return dispatcher.alterFileset(
        // The constraints of the name spec may be more strict than underlying catalog,
        // and for compatibility reasons, we only apply case-sensitive capabilities here.
        normalizeCaseSensitive(ident), applyCapabilities(capability, changes));
  }

  @Override
  public boolean dropFileset(NameIdentifier ident) {
    // The constraints of the name spec may be more strict than underlying catalog,
    // and for compatibility reasons, we only apply case-sensitive capabilities here.
    return dispatcher.dropFileset(normalizeCaseSensitive(ident));
  }

  @Override
  public String getFileLocation(NameIdentifier ident, String subPath) {
    // The constraints of the name spec may be more strict than underlying catalog,
    // and for compatibility reasons, we only apply case-sensitive capabilities here.
    return dispatcher.getFileLocation(normalizeCaseSensitive(ident), subPath);
  }

  private NameIdentifier normalizeNameIdentifier(NameIdentifier ident) {
    Capability capabilities = getCapability(ident, catalogManager);
    return applyCapabilities(ident, Capability.Scope.FILESET, capabilities);
  }

  private Namespace normalizeCaseSensitive(Namespace namespace) {
    Capability capabilities = getCapability(NameIdentifier.of(namespace.levels()), catalogManager);
    return applyCaseSensitive(namespace, Capability.Scope.FILESET, capabilities);
  }

  private NameIdentifier normalizeCaseSensitive(NameIdentifier filesetIdent) {
    Capability capabilities = getCapability(filesetIdent, catalogManager);
    return applyCaseSensitive(filesetIdent, Capability.Scope.FILESET, capabilities);
  }

  private NameIdentifier[] normalizeCaseSensitive(NameIdentifier[] filesetIdents) {
    if (ArrayUtils.isEmpty(filesetIdents)) {
      return filesetIdents;
    }

    Capability capabilities = getCapability(filesetIdents[0], catalogManager);
    return applyCaseSensitive(filesetIdents, Capability.Scope.FILESET, capabilities);
  }
}
