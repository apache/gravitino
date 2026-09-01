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
import static org.apache.gravitino.catalog.CapabilityHelpers.applyCapabilitiesOnName;
import static org.apache.gravitino.catalog.CapabilityHelpers.applyCaseSensitive;
import static org.apache.gravitino.catalog.CapabilityHelpers.getCapability;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.exceptions.IllegalSemanticModelException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchSemanticModelException;
import org.apache.gravitino.exceptions.SemanticModelAlreadyExistsException;
import org.apache.gravitino.semantic.SemanticModel;
import org.apache.gravitino.semantic.SemanticModelChange;
import org.apache.gravitino.semantic.SemanticModelDefinition;

/** Normalizes Semantic Model parents while retaining Gravitino-owned model naming rules. */
public class SemanticModelNormalizeDispatcher implements SemanticModelDispatcher {

  private final SemanticModelDispatcher dispatcher;
  private final CatalogManager catalogManager;

  /**
   * Creates a Semantic Model normalization dispatcher.
   *
   * @param dispatcher The underlying dispatcher.
   * @param catalogManager The catalog manager used to load parent naming capabilities.
   */
  public SemanticModelNormalizeDispatcher(
      SemanticModelDispatcher dispatcher, CatalogManager catalogManager) {
    this.dispatcher = dispatcher;
    this.catalogManager = catalogManager;
  }

  @Override
  public NameIdentifier[] listSemanticModels(Namespace namespace) throws NoSuchSchemaException {
    return dispatcher.listSemanticModels(normalizeParentForLookup(namespace));
  }

  @Override
  public SemanticModel loadSemanticModel(NameIdentifier ident) throws NoSuchSemanticModelException {
    return dispatcher.loadSemanticModel(normalizeForLookup(ident));
  }

  @Override
  public boolean semanticModelExists(NameIdentifier ident) {
    return dispatcher.semanticModelExists(normalizeForLookup(ident));
  }

  @Override
  public SemanticModel createSemanticModel(
      NameIdentifier ident,
      @Nullable String comment,
      SemanticModelDefinition definition,
      Map<String, String> properties)
      throws NoSuchSchemaException, SemanticModelAlreadyExistsException,
          IllegalSemanticModelException {
    return dispatcher.createSemanticModel(
        normalizeForCreate(ident), comment, definition, properties);
  }

  @Override
  public SemanticModel alterSemanticModel(NameIdentifier ident, SemanticModelChange... changes)
      throws NoSuchSemanticModelException, SemanticModelAlreadyExistsException,
          IllegalSemanticModelException {
    Preconditions.checkArgument(
        changes != null && changes.length > 0, "At least one change is required");
    SemanticModelChange[] normalizedChanges =
        Arrays.stream(changes).map(this::normalizeChange).toArray(SemanticModelChange[]::new);
    return dispatcher.alterSemanticModel(normalizeForLookup(ident), normalizedChanges);
  }

  @Override
  public boolean dropSemanticModel(NameIdentifier ident) {
    return dispatcher.dropSemanticModel(normalizeForLookup(ident));
  }

  private Namespace normalizeParentForLookup(Namespace namespace) {
    Capability capability = getCapability(NameIdentifier.of(namespace.levels()), catalogManager);
    return applyCaseSensitive(namespace, Capability.Scope.SEMANTIC_MODEL, capability);
  }

  private NameIdentifier normalizeForLookup(NameIdentifier ident) {
    return NameIdentifier.of(normalizeParentForLookup(ident.namespace()), ident.name());
  }

  private NameIdentifier normalizeForCreate(NameIdentifier ident) {
    Capability capability = getCapability(ident, catalogManager);
    Namespace namespace =
        applyCapabilities(ident.namespace(), Capability.Scope.SEMANTIC_MODEL, capability);
    return NameIdentifier.of(namespace, normalizeSemanticModelName(ident.name()));
  }

  private SemanticModelChange normalizeChange(SemanticModelChange change) {
    if (change instanceof SemanticModelChange.RenameSemanticModel) {
      return SemanticModelChange.rename(
          normalizeSemanticModelName(
              ((SemanticModelChange.RenameSemanticModel) change).getNewName()));
    }
    return change;
  }

  private String normalizeSemanticModelName(String name) {
    return applyCapabilitiesOnName(Capability.Scope.SEMANTIC_MODEL, name, Capability.DEFAULT);
  }
}
