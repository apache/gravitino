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
package org.apache.gravitino.function;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.exceptions.FunctionAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchFunctionException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.rel.types.Type;

/** The FunctionCatalog interface defines the public API for managing functions in a schema. */
@Evolving
public interface FunctionCatalog {

  /**
   * List the functions in a namespace from the catalog.
   *
   * @param namespace A namespace.
   * @return An array of function identifiers in the namespace.
   * @throws NoSuchSchemaException If the schema does not exist.
   */
  NameIdentifier[] listFunctions(Namespace namespace) throws NoSuchSchemaException;

  /**
   * List the functions with details in a namespace from the catalog.
   *
   * @param namespace A namespace.
   * @return An array of functions in the namespace.
   * @throws NoSuchSchemaException If the schema does not exist.
   */
  Function[] listFunctionInfos(Namespace namespace) throws NoSuchSchemaException;

  /**
   * Get a function by {@link NameIdentifier} from the catalog. The identifier only contains the
   * schema and function name. A function may include multiple definitions (overloads) in the
   * result. This method returns the current version of the function.
   *
   * <p>Note: Currently, the current version is always the latest version. In the future, when
   * rollback functionality is supported, the current version may differ from the latest version.
   *
   * @param ident A function identifier.
   * @return The current version of the function with the given name.
   * @throws NoSuchFunctionException If the function does not exist.
   */
  // todo: update the the JavaDoc when function rollback is supported
  Function getFunction(NameIdentifier ident) throws NoSuchFunctionException;

  /**
   * Check if a function with the given name exists in the catalog.
   *
   * @param ident The function identifier.
   * @return True if the function exists, false otherwise.
   */
  default boolean functionExists(NameIdentifier ident) {
    try {
      getFunction(ident);
      return true;
    } catch (NoSuchFunctionException e) {
      return false;
    }
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
  Function registerFunction(
      NameIdentifier ident,
      String comment,
      FunctionType functionType,
      boolean deterministic,
      Type returnType,
      FunctionDefinition[] definitions)
      throws NoSuchSchemaException, FunctionAlreadyExistsException;

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
  Function registerFunction(
      NameIdentifier ident,
      String comment,
      boolean deterministic,
      FunctionColumn[] returnColumns,
      FunctionDefinition[] definitions)
      throws NoSuchSchemaException, FunctionAlreadyExistsException;

  /**
   * Applies {@link FunctionChange changes} to a function in the catalog.
   *
   * <p>Implementations may reject the changes. If any change is rejected, no changes should be
   * applied to the function.
   *
   * @param ident the {@link NameIdentifier} instance of the function to alter.
   * @param changes the several {@link FunctionChange} instances to apply to the function.
   * @return the updated {@link Function} instance.
   * @throws NoSuchFunctionException If the function does not exist.
   * @throws IllegalArgumentException If the change is rejected by the implementation.
   */
  Function alterFunction(NameIdentifier ident, FunctionChange... changes)
      throws NoSuchFunctionException, IllegalArgumentException;

  /**
   * Drop a function by name.
   *
   * @param ident The name identifier of the function.
   * @return True if the function is deleted, false if the function does not exist.
   */
  boolean dropFunction(NameIdentifier ident);
}
