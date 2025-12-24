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

import org.apache.commons.lang3.ArrayUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.exceptions.FunctionAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchFunctionException;
import org.apache.gravitino.exceptions.NoSuchFunctionVersionException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.function.Function;
import org.apache.gravitino.function.FunctionChange;
import org.apache.gravitino.function.FunctionColumn;
import org.apache.gravitino.function.FunctionDefinition;
import org.apache.gravitino.function.FunctionType;
import org.apache.gravitino.rel.types.Type;

/**
 * {@code FunctionNormalizeDispatcher} normalizes function identifiers and namespaces by applying
 * case-sensitivity and other naming capabilities before delegating to the underlying dispatcher.
 */
public class FunctionNormalizeDispatcher implements FunctionDispatcher {
  private final CatalogManager catalogManager;
  private final FunctionDispatcher dispatcher;

  public FunctionNormalizeDispatcher(FunctionDispatcher dispatcher, CatalogManager catalogManager) {
    this.dispatcher = dispatcher;
    this.catalogManager = catalogManager;
  }

  @Override
  public NameIdentifier[] listFunctions(Namespace namespace) throws NoSuchSchemaException {
    Namespace caseSensitiveNs = normalizeCaseSensitive(namespace);
    NameIdentifier[] identifiers = dispatcher.listFunctions(caseSensitiveNs);
    return normalizeCaseSensitive(identifiers);
  }

  @Override
  public Function[] listFunctionInfos(Namespace namespace) throws NoSuchSchemaException {
    return dispatcher.listFunctionInfos(normalizeCaseSensitive(namespace));
  }

  @Override
  public Function getFunction(NameIdentifier ident) throws NoSuchFunctionException {
    return dispatcher.getFunction(normalizeCaseSensitive(ident));
  }

  @Override
  public Function getFunction(NameIdentifier ident, int version)
      throws NoSuchFunctionException, NoSuchFunctionVersionException {
    return dispatcher.getFunction(normalizeCaseSensitive(ident), version);
  }

  @Override
  public boolean functionExists(NameIdentifier ident) {
    return dispatcher.functionExists(normalizeCaseSensitive(ident));
  }

  @Override
  public Function registerFunction(
      NameIdentifier ident,
      String comment,
      FunctionType functionType,
      boolean deterministic,
      Type returnType,
      FunctionDefinition[] definitions)
      throws NoSuchSchemaException, FunctionAlreadyExistsException {
    return dispatcher.registerFunction(
        normalizeNameIdentifier(ident),
        comment,
        functionType,
        deterministic,
        returnType,
        definitions);
  }

  @Override
  public Function registerFunction(
      NameIdentifier ident,
      String comment,
      boolean deterministic,
      FunctionColumn[] returnColumns,
      FunctionDefinition[] definitions)
      throws NoSuchSchemaException, FunctionAlreadyExistsException {
    return dispatcher.registerFunction(
        normalizeNameIdentifier(ident), comment, deterministic, returnColumns, definitions);
  }

  @Override
  public Function alterFunction(NameIdentifier ident, FunctionChange... changes)
      throws NoSuchFunctionException, IllegalArgumentException {
    return dispatcher.alterFunction(normalizeCaseSensitive(ident), changes);
  }

  @Override
  public boolean dropFunction(NameIdentifier ident) {
    return dispatcher.dropFunction(normalizeCaseSensitive(ident));
  }

  private Namespace normalizeCaseSensitive(Namespace namespace) {
    Capability capabilities = getCapability(NameIdentifier.of(namespace.levels()), catalogManager);
    return applyCaseSensitive(namespace, Capability.Scope.FUNCTION, capabilities);
  }

  private NameIdentifier normalizeCaseSensitive(NameIdentifier functionIdent) {
    Capability capabilities = getCapability(functionIdent, catalogManager);
    return applyCaseSensitive(functionIdent, Capability.Scope.FUNCTION, capabilities);
  }

  private NameIdentifier[] normalizeCaseSensitive(NameIdentifier[] functionIdents) {
    if (ArrayUtils.isEmpty(functionIdents)) {
      return functionIdents;
    }

    Capability capabilities = getCapability(functionIdents[0], catalogManager);
    return applyCaseSensitive(functionIdents, Capability.Scope.FUNCTION, capabilities);
  }

  private NameIdentifier normalizeNameIdentifier(NameIdentifier functionIdent) {
    Capability capability = getCapability(functionIdent, catalogManager);
    return applyCapabilities(functionIdent, Capability.Scope.FUNCTION, capability);
  }
}
