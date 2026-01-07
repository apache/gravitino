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

import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.FunctionAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchFunctionException;
import org.apache.gravitino.exceptions.NoSuchFunctionVersionException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.function.Function;
import org.apache.gravitino.function.FunctionCatalog;
import org.apache.gravitino.function.FunctionChange;
import org.apache.gravitino.function.FunctionColumn;
import org.apache.gravitino.function.FunctionDefinition;
import org.apache.gravitino.function.FunctionType;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.storage.IdGenerator;

/**
 * {@code ManagedFunctionOperations} provides the storage-level operations for managing functions in
 * Gravitino's EntityStore.
 *
 * <p>This class handles the actual persistence of function metadata, including:
 *
 * <ul>
 *   <li>Storing function entities and their versions
 *   <li>Retrieving functions by identifier or version
 *   <li>Updating function metadata
 *   <li>Deleting functions and their versions
 * </ul>
 */
public class ManagedFunctionOperations implements FunctionCatalog {

  @SuppressWarnings("UnusedVariable")
  private static final int INIT_VERSION = 0;

  @SuppressWarnings("UnusedVariable")
  private final EntityStore store;

  @SuppressWarnings("UnusedVariable")
  private final IdGenerator idGenerator;

  /**
   * Creates a new ManagedFunctionOperations instance.
   *
   * @param store The EntityStore instance for function persistence.
   * @param idGenerator The IdGenerator instance for generating unique IDs.
   */
  public ManagedFunctionOperations(EntityStore store, IdGenerator idGenerator) {
    this.store = store;
    this.idGenerator = idGenerator;
  }

  @Override
  public NameIdentifier[] listFunctions(Namespace namespace) throws NoSuchSchemaException {
    // TODO: Implement when FunctionEntity is available
    throw new UnsupportedOperationException("listFunctions: FunctionEntity not yet implemented");
  }

  @Override
  public Function[] listFunctionInfos(Namespace namespace) throws NoSuchSchemaException {
    // TODO: Implement when FunctionEntity is available
    throw new UnsupportedOperationException(
        "listFunctionInfos: FunctionEntity not yet implemented");
  }

  @Override
  public Function getFunction(NameIdentifier ident) throws NoSuchFunctionException {
    // TODO: Implement when FunctionEntity is available
    throw new UnsupportedOperationException("getFunction: FunctionEntity not yet implemented");
  }

  @Override
  public Function getFunction(NameIdentifier ident, int version)
      throws NoSuchFunctionException, NoSuchFunctionVersionException {
    // TODO: Implement when FunctionEntity is available
    throw new UnsupportedOperationException(
        "getFunction with version: FunctionEntity not yet implemented");
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
    // TODO: Implement when FunctionEntity is available
    throw new UnsupportedOperationException("registerFunction: FunctionEntity not yet implemented");
  }

  @Override
  public Function registerFunction(
      NameIdentifier ident,
      String comment,
      boolean deterministic,
      FunctionColumn[] returnColumns,
      FunctionDefinition[] definitions)
      throws NoSuchSchemaException, FunctionAlreadyExistsException {
    // TODO: Implement when FunctionEntity is available
    throw new UnsupportedOperationException(
        "registerFunction for table-valued functions: FunctionEntity not yet implemented");
  }

  @Override
  public Function alterFunction(NameIdentifier ident, FunctionChange... changes)
      throws NoSuchFunctionException, IllegalArgumentException {
    // TODO: Implement when FunctionEntity is available
    throw new UnsupportedOperationException("alterFunction: FunctionEntity not yet implemented");
  }

  @Override
  public boolean dropFunction(NameIdentifier ident) {
    // TODO: Implement when FunctionEntity is available
    throw new UnsupportedOperationException("dropFunction: FunctionEntity not yet implemented");
  }
}
