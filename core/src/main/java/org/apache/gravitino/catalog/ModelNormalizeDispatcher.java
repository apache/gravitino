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
import org.apache.gravitino.exceptions.ModelAlreadyExistsException;
import org.apache.gravitino.exceptions.ModelVersionAliasesAlreadyExistException;
import org.apache.gravitino.exceptions.NoSuchModelException;
import org.apache.gravitino.exceptions.NoSuchModelVersionException;
import org.apache.gravitino.exceptions.NoSuchModelVersionURINameException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.model.Model;
import org.apache.gravitino.model.ModelChange;
import org.apache.gravitino.model.ModelVersion;
import org.apache.gravitino.model.ModelVersionChange;

public class ModelNormalizeDispatcher implements ModelDispatcher {
  private final CatalogManager catalogManager;
  private final ModelDispatcher dispatcher;

  public ModelNormalizeDispatcher(ModelDispatcher dispatcher, CatalogManager catalogManager) {
    this.dispatcher = dispatcher;
    this.catalogManager = catalogManager;
  }

  @Override
  public NameIdentifier[] listModels(Namespace namespace) throws NoSuchSchemaException {
    // The constraints of the name spec may be more strict than underlying catalog,
    // and for compatibility reasons, we only apply case-sensitive capabilities here.
    Namespace caseSensitiveNs = normalizeCaseSensitive(namespace);
    NameIdentifier[] identifiers = dispatcher.listModels(caseSensitiveNs);
    return normalizeCaseSensitive(identifiers);
  }

  @Override
  public Model getModel(NameIdentifier ident) throws NoSuchModelException {
    // The constraints of the name spec may be more strict than underlying catalog,
    // and for compatibility reasons, we only apply case-sensitive capabilities here.
    return dispatcher.getModel(normalizeCaseSensitive(ident));
  }

  @Override
  public boolean modelExists(NameIdentifier ident) {
    // The constraints of the name spec may be more strict than underlying catalog,
    // and for compatibility reasons, we only apply case-sensitive capabilities here.
    return dispatcher.modelExists(normalizeCaseSensitive(ident));
  }

  @Override
  public Model registerModel(NameIdentifier ident, String comment, Map<String, String> properties)
      throws NoSuchSchemaException, ModelAlreadyExistsException {
    return dispatcher.registerModel(normalizeNameIdentifier(ident), comment, properties);
  }

  @Override
  public boolean deleteModel(NameIdentifier ident) {
    // The constraints of the name spec may be more strict than underlying catalog,
    // and for compatibility reasons, we only apply case-sensitive capabilities here.
    return dispatcher.deleteModel(normalizeCaseSensitive(ident));
  }

  @Override
  public int[] listModelVersions(NameIdentifier ident) throws NoSuchModelException {
    return dispatcher.listModelVersions(normalizeCaseSensitive(ident));
  }

  @Override
  public ModelVersion[] listModelVersionInfos(NameIdentifier ident) throws NoSuchModelException {
    return dispatcher.listModelVersionInfos(normalizeCaseSensitive(ident));
  }

  @Override
  public ModelVersion getModelVersion(NameIdentifier ident, int version)
      throws NoSuchModelVersionException {
    return dispatcher.getModelVersion(normalizeCaseSensitive(ident), version);
  }

  @Override
  public ModelVersion getModelVersion(NameIdentifier ident, String alias)
      throws NoSuchModelVersionException {
    return dispatcher.getModelVersion(normalizeCaseSensitive(ident), alias);
  }

  @Override
  public boolean modelVersionExists(NameIdentifier ident, int version) {
    return dispatcher.modelVersionExists(normalizeCaseSensitive(ident), version);
  }

  @Override
  public boolean modelVersionExists(NameIdentifier ident, String alias) {
    return dispatcher.modelVersionExists(normalizeCaseSensitive(ident), alias);
  }

  @Override
  public void linkModelVersion(
      NameIdentifier ident,
      Map<String, String> uris,
      String[] aliases,
      String comment,
      Map<String, String> properties)
      throws NoSuchModelException, ModelVersionAliasesAlreadyExistException {
    dispatcher.linkModelVersion(normalizeCaseSensitive(ident), uris, aliases, comment, properties);
  }

  @Override
  public String getModelVersionUri(NameIdentifier ident, int version, String uriName)
      throws NoSuchModelVersionException, NoSuchModelVersionURINameException {
    return dispatcher.getModelVersionUri(normalizeCaseSensitive(ident), version, uriName);
  }

  @Override
  public String getModelVersionUri(NameIdentifier ident, String alias, String uriName)
      throws NoSuchModelVersionException, NoSuchModelVersionURINameException {
    return dispatcher.getModelVersionUri(normalizeCaseSensitive(ident), alias, uriName);
  }

  @Override
  public boolean deleteModelVersion(NameIdentifier ident, int version) {
    return dispatcher.deleteModelVersion(normalizeCaseSensitive(ident), version);
  }

  @Override
  public boolean deleteModelVersion(NameIdentifier ident, String alias) {
    return dispatcher.deleteModelVersion(normalizeCaseSensitive(ident), alias);
  }

  /** {@inheritDoc} */
  @Override
  public Model alterModel(NameIdentifier ident, ModelChange... changes)
      throws NoSuchModelException, IllegalArgumentException {
    return dispatcher.alterModel(normalizeCaseSensitive(ident), changes);
  }

  /** {@inheritDoc} */
  @Override
  public ModelVersion alterModelVersion(
      NameIdentifier ident, int version, ModelVersionChange... changes)
      throws NoSuchModelException, NoSuchModelVersionException, IllegalArgumentException {
    return dispatcher.alterModelVersion(normalizeCaseSensitive(ident), version, changes);
  }

  /** {@inheritDoc} */
  @Override
  public ModelVersion alterModelVersion(
      NameIdentifier ident, String alias, ModelVersionChange... changes)
      throws NoSuchModelException, IllegalArgumentException {
    return dispatcher.alterModelVersion(normalizeCaseSensitive(ident), alias, changes);
  }

  private Namespace normalizeCaseSensitive(Namespace namespace) {
    Capability capabilities = getCapability(NameIdentifier.of(namespace.levels()), catalogManager);
    return applyCaseSensitive(namespace, Capability.Scope.MODEL, capabilities);
  }

  private NameIdentifier normalizeCaseSensitive(NameIdentifier ident) {
    Capability capabilities = getCapability(ident, catalogManager);
    return applyCaseSensitive(ident, Capability.Scope.MODEL, capabilities);
  }

  private NameIdentifier[] normalizeCaseSensitive(NameIdentifier[] idents) {
    if (ArrayUtils.isEmpty(idents)) {
      return idents;
    }

    Capability capabilities = getCapability(idents[0], catalogManager);
    return applyCaseSensitive(idents, Capability.Scope.MODEL, capabilities);
  }

  private NameIdentifier normalizeNameIdentifier(NameIdentifier ident) {
    Capability capability = getCapability(ident, catalogManager);
    return applyCapabilities(ident, Capability.Scope.MODEL, capability);
  }
}
