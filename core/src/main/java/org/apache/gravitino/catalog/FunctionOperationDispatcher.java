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

import com.google.common.base.Preconditions;
import java.util.Arrays;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.FunctionAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchFunctionException;
import org.apache.gravitino.exceptions.NoSuchFunctionVersionException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.function.Function;
import org.apache.gravitino.function.FunctionChange;
import org.apache.gravitino.function.FunctionColumn;
import org.apache.gravitino.function.FunctionDefinition;
import org.apache.gravitino.function.FunctionType;
import org.apache.gravitino.lock.LockType;
import org.apache.gravitino.lock.TreeLockUtils;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.storage.IdGenerator;

/**
 * {@code FunctionOperationDispatcher} is responsible for dispatching function-related operations.
 *
 * <p>Unlike ModelCatalog which manages its own schemas (via ManagedSchemaOperations), functions are
 * registered under schemas managed by the underlying catalog (e.g., Hive, Iceberg). Therefore, this
 * dispatcher validates schema existence by calling the underlying catalog's schema operations, then
 * delegates actual storage operations to {@link ManagedFunctionOperations}.
 */
public class FunctionOperationDispatcher extends OperationDispatcher implements FunctionDispatcher {

  private final SchemaOperationDispatcher schemaOps;
  private final ManagedFunctionOperations managedFunctionOps;

  /**
   * Creates a new FunctionOperationDispatcher instance.
   *
   * @param catalogManager The CatalogManager instance to be used for function operations.
   * @param store The EntityStore instance to be used for function operations.
   * @param idGenerator The IdGenerator instance to be used for function operations.
   */
  public FunctionOperationDispatcher(
      CatalogManager catalogManager,
      SchemaOperationDispatcher schemaOps,
      EntityStore store,
      IdGenerator idGenerator) {
    super(catalogManager, store, idGenerator);
    this.schemaOps = schemaOps;
    this.managedFunctionOps = new ManagedFunctionOperations(store, idGenerator);
  }

  /**
   * List the functions in a namespace from the catalog.
   *
   * @param namespace A namespace.
   * @return An array of function identifiers in the namespace.
   * @throws NoSuchSchemaException If the schema does not exist.
   */
  @Override
  public NameIdentifier[] listFunctions(Namespace namespace) throws NoSuchSchemaException {
    return Arrays.stream(listFunctionInfos(namespace))
        .map(f -> NameIdentifier.of(namespace, f.name()))
        .toArray(NameIdentifier[]::new);
  }

  @Override
  public Function[] listFunctionInfos(Namespace namespace) throws NoSuchSchemaException {
    NameIdentifier schemaIdent = NameIdentifier.of(namespace.levels());
    // Validate schema exists in the underlying catalog
    schemaOps.loadSchema(schemaIdent);

    return TreeLockUtils.doWithTreeLock(
        schemaIdent, LockType.READ, () -> managedFunctionOps.listFunctionInfos(namespace));
  }

  /**
   * Get a function by {@link NameIdentifier} from the catalog. Returns the latest version.
   *
   * @param ident A function identifier.
   * @return The latest version of the function with the given name.
   * @throws NoSuchFunctionException If the function does not exist.
   */
  @Override
  public Function getFunction(NameIdentifier ident) throws NoSuchFunctionException {
    NameIdentifier schemaIdent = NameIdentifier.of(ident.namespace().levels());
    // Validate schema exists in the underlying catalog
    if (!schemaOps.schemaExists(schemaIdent)) {
      throw new NoSuchFunctionException("Schema does not exist: %s", schemaIdent);
    }

    return TreeLockUtils.doWithTreeLock(
        ident, LockType.READ, () -> managedFunctionOps.getFunction(ident));
  }

  /**
   * Get a function by {@link NameIdentifier} and version from the catalog.
   *
   * @param ident A function identifier.
   * @param version The function version, counted from 0.
   * @return The function with the given name and version.
   * @throws NoSuchFunctionException If the function does not exist.
   * @throws NoSuchFunctionVersionException If the function version does not exist.
   */
  @Override
  public Function getFunction(NameIdentifier ident, int version)
      throws NoSuchFunctionException, NoSuchFunctionVersionException {
    Preconditions.checkArgument(version >= 0, "Function version must be non-negative");
    NameIdentifier schemaIdent = NameIdentifier.of(ident.namespace().levels());
    // Validate schema exists in the underlying catalog
    if (!schemaOps.schemaExists(schemaIdent)) {
      throw new NoSuchFunctionException("Schema does not exist: %s", schemaIdent);
    }

    return TreeLockUtils.doWithTreeLock(
        ident, LockType.READ, () -> managedFunctionOps.getFunction(ident, version));
  }

  /**
   * Register a scalar or aggregate function with one or more definitions (overloads).
   *
   * @param ident The function identifier.
   * @param comment The optional function comment.
   * @param functionType The function type.
   * @param deterministic Whether the function is deterministic.
   * @param returnType The return type.
   * @param definitions The function definitions.
   * @return The registered function.
   * @throws NoSuchSchemaException If the schema does not exist.
   * @throws FunctionAlreadyExistsException If the function already exists.
   */
  @Override
  public Function registerFunction(
      NameIdentifier ident,
      String comment,
      FunctionType functionType,
      boolean deterministic,
      Type returnType,
      FunctionDefinition[] definitions)
      throws NoSuchSchemaException, FunctionAlreadyExistsException {
    Preconditions.checkArgument(
        functionType == FunctionType.SCALAR || functionType == FunctionType.AGGREGATE,
        "This method is for scalar or aggregate functions only");
    Preconditions.checkArgument(returnType != null, "Return type is required");
    Preconditions.checkArgument(
        definitions != null && definitions.length > 0, "At least one definition is required");

    NameIdentifier schemaIdent = NameIdentifier.of(ident.namespace().levels());
    // Validate schema exists in the underlying catalog
    schemaOps.loadSchema(schemaIdent);

    return TreeLockUtils.doWithTreeLock(
        ident,
        LockType.WRITE,
        () ->
            managedFunctionOps.registerFunction(
                ident, comment, functionType, deterministic, returnType, definitions));
  }

  /**
   * Register a table-valued function with one or more definitions (overloads).
   *
   * @param ident The function identifier.
   * @param comment The optional function comment.
   * @param deterministic Whether the function is deterministic.
   * @param returnColumns The return columns.
   * @param definitions The function definitions.
   * @return The registered function.
   * @throws NoSuchSchemaException If the schema does not exist.
   * @throws FunctionAlreadyExistsException If the function already exists.
   */
  @Override
  public Function registerFunction(
      NameIdentifier ident,
      String comment,
      boolean deterministic,
      FunctionColumn[] returnColumns,
      FunctionDefinition[] definitions)
      throws NoSuchSchemaException, FunctionAlreadyExistsException {
    Preconditions.checkArgument(
        returnColumns != null && returnColumns.length > 0,
        "At least one return column is required for table-valued function");
    Preconditions.checkArgument(
        definitions != null && definitions.length > 0, "At least one definition is required");

    NameIdentifier schemaIdent = NameIdentifier.of(ident.namespace().levels());
    // Validate schema exists in the underlying catalog
    schemaOps.loadSchema(schemaIdent);

    return TreeLockUtils.doWithTreeLock(
        ident,
        LockType.WRITE,
        () ->
            managedFunctionOps.registerFunction(
                ident, comment, deterministic, returnColumns, definitions));
  }

  /**
   * Applies {@link FunctionChange changes} to a function in the catalog.
   *
   * @param ident the {@link NameIdentifier} instance of the function to alter.
   * @param changes the several {@link FunctionChange} instances to apply to the function.
   * @return the updated {@link Function} instance.
   * @throws NoSuchFunctionException If the function does not exist.
   * @throws IllegalArgumentException If the change is rejected by the implementation.
   */
  @Override
  public Function alterFunction(NameIdentifier ident, FunctionChange... changes)
      throws NoSuchFunctionException, IllegalArgumentException {
    Preconditions.checkArgument(
        changes != null && changes.length > 0, "At least one change is required");
    NameIdentifier schemaIdent = NameIdentifier.of(ident.namespace().levels());
    if (!schemaOps.schemaExists(schemaIdent)) {
      throw new NoSuchFunctionException("Schema does not exist: %s", schemaIdent);
    }

    return TreeLockUtils.doWithTreeLock(
        ident, LockType.WRITE, () -> managedFunctionOps.alterFunction(ident, changes));
  }

  /**
   * Drop a function by name.
   *
   * @param ident The name identifier of the function.
   * @return True if the function is deleted, false if the function does not exist.
   */
  @Override
  public boolean dropFunction(NameIdentifier ident) {
    NameIdentifier schemaIdent = NameIdentifier.of(ident.namespace().levels());
    if (!schemaOps.schemaExists(schemaIdent)) {
      return false;
    }

    return TreeLockUtils.doWithTreeLock(
        ident, LockType.WRITE, () -> managedFunctionOps.dropFunction(ident));
  }
}
