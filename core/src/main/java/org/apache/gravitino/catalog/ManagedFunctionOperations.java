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
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.FunctionAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchFunctionException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.function.Function;
import org.apache.gravitino.function.FunctionCatalog;
import org.apache.gravitino.function.FunctionChange;
import org.apache.gravitino.function.FunctionColumn;
import org.apache.gravitino.function.FunctionDefinition;
import org.apache.gravitino.function.FunctionParam;
import org.apache.gravitino.function.FunctionType;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.FunctionEntity;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.PrincipalUtils;

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
  private final EntityStore store;
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
    return Arrays.stream(listFunctionInfos(namespace))
        .map(f -> NameIdentifier.of(namespace, f.name()))
        .toArray(NameIdentifier[]::new);
  }

  @Override
  public Function[] listFunctionInfos(Namespace namespace) throws NoSuchSchemaException {
    try {
      List<FunctionEntity> functions =
          store.list(namespace, FunctionEntity.class, Entity.EntityType.FUNCTION);
      return functions.toArray(FunctionEntity[]::new);

    } catch (NoSuchEntityException e) {
      throw new NoSuchSchemaException(e, "Schema %s does not exist", namespace);
    } catch (IOException e) {
      throw new RuntimeException("Failed to list functions in namespace " + namespace, e);
    }
  }

  @Override
  public Function getFunction(NameIdentifier ident) throws NoSuchFunctionException {
    try {
      return store.get(ident, Entity.EntityType.FUNCTION, FunctionEntity.class);
    } catch (NoSuchEntityException e) {
      throw new NoSuchFunctionException(e, "Function %s does not exist", ident);
    } catch (IOException e) {
      throw new RuntimeException("Failed to get function " + ident, e);
    }
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
    return doRegisterFunction(
        ident,
        comment,
        functionType,
        deterministic,
        Optional.of(returnType),
        Optional.empty(),
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
    return doRegisterFunction(
        ident,
        comment,
        FunctionType.TABLE,
        deterministic,
        Optional.empty(),
        Optional.of(returnColumns),
        definitions);
  }

  @Override
  public Function alterFunction(NameIdentifier ident, FunctionChange... changes)
      throws NoSuchFunctionException, IllegalArgumentException {
    // TODO: Implement when FunctionEntity is available
    throw new UnsupportedOperationException("alterFunction: FunctionEntity not yet implemented");
  }

  @Override
  public boolean dropFunction(NameIdentifier ident) {
    try {
      return store.delete(ident, Entity.EntityType.FUNCTION);
    } catch (NoSuchEntityException e) {
      return false;
    } catch (IOException e) {
      throw new RuntimeException("Failed to drop function " + ident, e);
    }
  }

  private Function doRegisterFunction(
      NameIdentifier ident,
      String comment,
      FunctionType functionType,
      boolean deterministic,
      Optional<Type> returnType,
      Optional<FunctionColumn[]> returnColumns,
      FunctionDefinition[] definitions)
      throws NoSuchSchemaException, FunctionAlreadyExistsException {
    Preconditions.checkArgument(
        definitions != null && definitions.length > 0,
        "At least one function definition must be provided");
    validateDefinitionsNoArityOverlap(definitions);

    String currentUser = PrincipalUtils.getCurrentUserName();
    Instant now = Instant.now();
    AuditInfo auditInfo = AuditInfo.builder().withCreator(currentUser).withCreateTime(now).build();

    FunctionEntity functionEntity =
        FunctionEntity.builder()
            .withId(idGenerator.nextId())
            .withName(ident.name())
            .withNamespace(ident.namespace())
            .withComment(comment)
            .withFunctionType(functionType)
            .withDeterministic(deterministic)
            .withReturnType(returnType.orElse(null))
            .withReturnColumns(returnColumns.orElse(null))
            .withDefinitions(definitions)
            .withAuditInfo(auditInfo)
            .build();

    try {
      store.put(functionEntity, false /* overwrite */);
      return functionEntity;

    } catch (NoSuchEntityException e) {
      throw new NoSuchSchemaException(e, "Schema %s does not exist", ident.namespace());
    } catch (EntityAlreadyExistsException e) {
      throw new FunctionAlreadyExistsException(e, "Function %s already exists", ident);
    } catch (IOException e) {
      throw new RuntimeException("Failed to register function " + ident, e);
    }
  }

  /**
   * Validates that all definitions in the array do not have overlapping arities. This is used when
   * registering a function with multiple definitions.
   *
   * <p>Gravitino enforces strict validation to prevent ambiguity. Operations MUST fail if any
   * definition's invocation arities overlap with another. For example, if an existing definition
   * {@code foo(int, float default 1.0)} supports arities {@code (int)} and {@code (int, float)},
   * adding a new definition {@code foo(int, string default 'x')} (which supports {@code (int)} and
   * {@code (int, string)}) will be REJECTED because both support the call {@code foo(1)}. This
   * ensures every function invocation deterministically maps to a single definition.
   *
   * @param definitions The array of definitions to validate.
   * @throws IllegalArgumentException If any two definitions have overlapping arities.
   * @see #computeArities(FunctionDefinition) for details on how arity signatures are computed
   */
  private void validateDefinitionsNoArityOverlap(FunctionDefinition[] definitions) {
    // Track each arity signature with its source definition index
    Map<String, Integer> seenArities = new HashMap<>();
    for (int i = 0; i < definitions.length; i++) {
      for (String arity : computeArities(definitions[i])) {
        Integer existingIndex = seenArities.put(arity, i);
        if (existingIndex != null) {
          throw new IllegalArgumentException(
              String.format(
                  "Cannot register function: definitions at index %d and %d have overlapping "
                      + "arity '%s'. This would create ambiguous function invocations.",
                  existingIndex, i, arity));
        }
      }
    }
  }

  /**
   * Computes all possible invocation arities for a function definition. A definition with N
   * parameters where the last M have default values supports arities from (N-M) to N parameters.
   *
   * <p>For example:
   *
   * <ul>
   *   <li>{@code foo(int a)} → arities: {@code ["int"]}
   *   <li>{@code foo(int a, float b)} → arities: {@code ["int,float"]}
   *   <li>{@code foo(int a, float b default 1.0)} → arities: {@code ["int", "int,float"]}
   *   <li>{@code foo(int a, float b default 1.0, string c default 'x')} → arities: {@code ["int",
   *       "int,float", "int,float,string"]}
   *   <li>{@code foo()} (no args) → arities: {@code [""]}
   * </ul>
   *
   * @param definition The function definition.
   * @return A set of arity signatures (e.g., "int", "int,float", "").
   */
  private Set<String> computeArities(FunctionDefinition definition) {
    FunctionParam[] params = definition.parameters();
    int firstOptionalIndex = findFirstOptionalParamIndex(params);

    // Generate all possible arities from firstOptionalIndex to params.length
    Set<String> arities = new HashSet<>();
    for (int paramCount = firstOptionalIndex; paramCount <= params.length; paramCount++) {
      String arity =
          Arrays.stream(params, 0, paramCount)
              .map(p -> p.dataType().simpleString())
              .collect(Collectors.joining(","));
      arities.add(arity);
    }
    return arities;
  }

  /**
   * Finds the index of the first optional parameter (one with a default value). Also validates that
   * all optional parameters appear at the end of the parameter list.
   *
   * @param params The function parameters.
   * @return The index of the first optional parameter, or params.length if all are required.
   * @throws IllegalArgumentException If a required parameter follows an optional one.
   */
  private int findFirstOptionalParamIndex(FunctionParam[] params) {
    int firstOptionalIndex = params.length;
    for (int i = 0; i < params.length; i++) {
      boolean hasDefault = hasDefaultValue(params[i]);

      if (firstOptionalIndex < params.length && !hasDefault) {
        // Found a required param after an optional one - invalid order
        throw new IllegalArgumentException(
            String.format(
                "Invalid parameter order: required parameter '%s' at position %d "
                    + "follows optional parameter(s). All parameters with default values "
                    + "must appear at the end of the parameter list.",
                params[i].name(), i));
      }

      if (hasDefault && firstOptionalIndex == params.length) {
        firstOptionalIndex = i;
      }
    }
    return firstOptionalIndex;
  }

  private boolean hasDefaultValue(FunctionParam param) {
    Expression defaultValue = param.defaultValue();
    return defaultValue != null && defaultValue != Column.DEFAULT_VALUE_NOT_SET;
  }
}
