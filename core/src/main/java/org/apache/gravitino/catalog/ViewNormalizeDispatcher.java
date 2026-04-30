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
import javax.annotation.Nullable;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchViewException;
import org.apache.gravitino.exceptions.ViewAlreadyExistsException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.View;
import org.apache.gravitino.rel.ViewChange;

public class ViewNormalizeDispatcher implements ViewDispatcher {

  private final CatalogManager catalogManager;
  private final ViewDispatcher dispatcher;

  public ViewNormalizeDispatcher(ViewDispatcher dispatcher, CatalogManager catalogManager) {
    this.dispatcher = dispatcher;
    this.catalogManager = catalogManager;
  }

  @Override
  public NameIdentifier[] listViews(Namespace namespace) throws NoSuchSchemaException {
    // The constraints of the name spec may be more strict than underlying catalog,
    // and for compatibility reasons, we only apply case-sensitive capabilities here.
    Namespace caseSensitiveNs = normalizeCaseSensitive(namespace);
    NameIdentifier[] identifiers = dispatcher.listViews(caseSensitiveNs);
    return normalizeCaseSensitive(identifiers);
  }

  @Override
  public View loadView(NameIdentifier ident) throws NoSuchViewException {
    return dispatcher.loadView(normalizeCaseSensitive(ident));
  }

  @Override
  public boolean viewExists(NameIdentifier ident) {
    return dispatcher.viewExists(normalizeCaseSensitive(ident));
  }

  @Override
  public View createView(
      NameIdentifier ident,
      String comment,
      Column[] columns,
      Representation[] representations,
      @Nullable String defaultCatalog,
      @Nullable String defaultSchema,
      Map<String, String> properties)
      throws NoSuchSchemaException, ViewAlreadyExistsException {
    return dispatcher.createView(
        normalizeNameIdentifier(ident),
        comment,
        columns,
        representations,
        defaultCatalog,
        defaultSchema,
        properties);
  }

  @Override
  public View alterView(NameIdentifier ident, ViewChange... changes)
      throws NoSuchViewException, IllegalArgumentException {
    Capability capability = getCapability(ident, catalogManager);
    return dispatcher.alterView(
        normalizeCaseSensitive(ident), applyCapabilities(capability, changes));
  }

  @Override
  public boolean dropView(NameIdentifier ident) {
    return dispatcher.dropView(normalizeCaseSensitive(ident));
  }

  private Namespace normalizeCaseSensitive(Namespace namespace) {
    Capability capabilities = getCapability(NameIdentifier.of(namespace.levels()), catalogManager);
    return applyCaseSensitive(namespace, Capability.Scope.VIEW, capabilities);
  }

  private NameIdentifier normalizeCaseSensitive(NameIdentifier ident) {
    Capability capabilities = getCapability(ident, catalogManager);
    return applyCaseSensitive(ident, Capability.Scope.VIEW, capabilities);
  }

  private NameIdentifier[] normalizeCaseSensitive(NameIdentifier[] idents) {
    if (ArrayUtils.isEmpty(idents)) {
      return idents;
    }
    Capability capabilities = getCapability(idents[0], catalogManager);
    return applyCaseSensitive(idents, Capability.Scope.VIEW, capabilities);
  }

  private NameIdentifier normalizeNameIdentifier(NameIdentifier ident) {
    Capability capability = getCapability(ident, catalogManager);
    return applyCapabilities(ident, Capability.Scope.VIEW, capability);
  }
}
