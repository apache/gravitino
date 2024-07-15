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
package com.apache.gravitino.catalog;

import static com.apache.gravitino.catalog.CapabilityHelpers.applyCapabilities;
import static com.apache.gravitino.catalog.CapabilityHelpers.applyCaseSensitive;

import com.apache.gravitino.NameIdentifier;
import com.apache.gravitino.Namespace;
import com.apache.gravitino.connector.capability.Capability;
import com.apache.gravitino.exceptions.FilesetAlreadyExistsException;
import com.apache.gravitino.exceptions.NoSuchFilesetException;
import com.apache.gravitino.exceptions.NoSuchSchemaException;
import com.apache.gravitino.file.Fileset;
import com.apache.gravitino.file.FilesetChange;
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
