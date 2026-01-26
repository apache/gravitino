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
import java.util.ArrayList;
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
import org.apache.gravitino.function.FunctionDefinitions;
import org.apache.gravitino.function.FunctionImpl;
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
    try {
      return store.update(
          ident,
          FunctionEntity.class,
          Entity.EntityType.FUNCTION,
          oldEntity -> applyChanges(oldEntity, changes));

    } catch (NoSuchEntityException e) {
      throw new NoSuchFunctionException(e, "Function %s does not exist", ident);
    } catch (EntityAlreadyExistsException e) {
      throw new IllegalArgumentException("Failed to alter function " + ident, e);
    } catch (IOException e) {
      throw new RuntimeException("Failed to alter function " + ident, e);
    }
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

  private FunctionEntity applyChanges(FunctionEntity oldEntity, FunctionChange... changes) {
    String newComment = oldEntity.comment();
    List<FunctionDefinition> newDefinitions =
        new ArrayList<>(Arrays.asList(oldEntity.definitions()));

    for (FunctionChange change : changes) {
      if (change instanceof FunctionChange.UpdateComment) {
        newComment = ((FunctionChange.UpdateComment) change).newComment();

      } else if (change instanceof FunctionChange.AddDefinition) {
        FunctionDefinition defToAdd = ((FunctionChange.AddDefinition) change).definition();
        validateNoArityOverlap(newDefinitions, defToAdd);
        newDefinitions.add(defToAdd);

      } else if (change instanceof FunctionChange.RemoveDefinition) {
        FunctionParam[] paramsToRemove = ((FunctionChange.RemoveDefinition) change).parameters();
        validateRemoveDefinition(newDefinitions, paramsToRemove);
        newDefinitions.removeIf(def -> parametersMatch(def.parameters(), paramsToRemove));

      } else if (change instanceof FunctionChange.AddImpl) {
        FunctionChange.AddImpl addImpl = (FunctionChange.AddImpl) change;
        FunctionParam[] targetParams = addImpl.parameters();
        FunctionImpl implToAdd = addImpl.implementation();
        newDefinitions = addImplToDefinition(newDefinitions, targetParams, implToAdd);

      } else if (change instanceof FunctionChange.UpdateImpl) {
        FunctionChange.UpdateImpl updateImpl = (FunctionChange.UpdateImpl) change;
        FunctionParam[] targetParams = updateImpl.parameters();
        FunctionImpl.RuntimeType runtime = updateImpl.runtime();
        FunctionImpl newImpl = updateImpl.implementation();
        newDefinitions = updateImplInDefinition(newDefinitions, targetParams, runtime, newImpl);

      } else if (change instanceof FunctionChange.RemoveImpl) {
        FunctionChange.RemoveImpl removeImpl = (FunctionChange.RemoveImpl) change;
        FunctionParam[] targetParams = removeImpl.parameters();
        FunctionImpl.RuntimeType runtime = removeImpl.runtime();
        newDefinitions = removeImplFromDefinition(newDefinitions, targetParams, runtime);

      } else {
        throw new IllegalArgumentException(
            "Unknown function change: " + change.getClass().getSimpleName());
      }
    }

    String currentUser = PrincipalUtils.getCurrentUserName();
    Instant now = Instant.now();
    AuditInfo newAuditInfo =
        AuditInfo.builder()
            .withCreator(oldEntity.auditInfo().creator())
            .withCreateTime(oldEntity.auditInfo().createTime())
            .withLastModifier(currentUser)
            .withLastModifiedTime(now)
            .build();

    return FunctionEntity.builder()
        .withId(oldEntity.id())
        .withName(oldEntity.name())
        .withNamespace(oldEntity.namespace())
        .withComment(newComment)
        .withFunctionType(oldEntity.functionType())
        .withDeterministic(oldEntity.deterministic())
        .withReturnType(oldEntity.returnType())
        .withReturnColumns(oldEntity.returnColumns())
        .withDefinitions(newDefinitions.toArray(new FunctionDefinition[0]))
        .withAuditInfo(newAuditInfo)
        .build();
  }

  /**
   * Validates that a new definition does not create ambiguous function arities with existing
   * definitions. Each definition can support multiple arities based on parameters with default
   * values.
   *
   * <p>Gravitino enforces strict validation to prevent ambiguity. Operations MUST fail if a new
   * definition's invocation arities overlap with existing ones. For example, if an existing
   * definition {@code foo(int, float default 1.0)} supports arities {@code (int)} and {@code (int,
   * float)}, adding a new definition {@code foo(int, string default 'x')} (which supports {@code
   * (int)} and {@code (int, string)}) will be REJECTED because both support the call {@code
   * foo(1)}. This ensures every function invocation deterministically maps to a single definition.
   *
   * @param existingDefinitions The current definitions.
   * @param newDefinition The definition to add.
   * @throws IllegalArgumentException If the new definition creates overlapping arities.
   */
  private void validateNoArityOverlap(
      List<FunctionDefinition> existingDefinitions, FunctionDefinition newDefinition) {
    Set<String> newArities = computeArities(newDefinition);

    for (FunctionDefinition existing : existingDefinitions) {
      Set<String> existingArities = computeArities(existing);
      for (String arity : newArities) {
        if (existingArities.contains(arity)) {
          throw new IllegalArgumentException(
              String.format(
                  "Cannot add definition: arity '%s' overlaps with an existing definition. "
                      + "This would create ambiguous function invocations.",
                  arity));
        }
      }
    }
  }

  /**
   * Computes all possible invocation arities for a function definition. A definition with N
   * parameters where the last M has default values supports arities from (N-M) to N parameters.
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

  /**
   * Validates that a definition can be removed.
   *
   * @param definitions The current definitions.
   * @param paramsToRemove The parameters identifying the definition to remove.
   * @throws IllegalArgumentException If the definition doesn't exist or is the only one.
   */
  private void validateRemoveDefinition(
      List<FunctionDefinition> definitions, FunctionParam[] paramsToRemove) {
    if (definitions.size() == 1) {
      throw new IllegalArgumentException(
          "Cannot remove the only definition. Use dropFunction to remove the entire function.");
    }

    boolean found = false;
    for (FunctionDefinition def : definitions) {
      if (parametersMatch(def.parameters(), paramsToRemove)) {
        found = true;
        break;
      }
    }

    if (!found) {
      throw new IllegalArgumentException(
          "Cannot remove definition: no definition found with the specified parameters");
    }
  }

  private boolean parametersMatch(FunctionParam[] params1, FunctionParam[] params2) {
    if (params1.length != params2.length) {
      return false;
    }
    for (int i = 0; i < params1.length; i++) {
      if (!params1[i].name().equals(params2[i].name())
          || !params1[i].dataType().equals(params2[i].dataType())) {
        return false;
      }
    }
    return true;
  }

  private List<FunctionDefinition> addImplToDefinition(
      List<FunctionDefinition> definitions, FunctionParam[] targetParams, FunctionImpl implToAdd) {
    List<FunctionDefinition> result = new ArrayList<>();
    boolean found = false;

    for (FunctionDefinition def : definitions) {
      if (parametersMatch(def.parameters(), targetParams)) {
        found = true;
        // Check if runtime already exists
        for (FunctionImpl existingImpl : def.impls()) {
          if (existingImpl.runtime() == implToAdd.runtime()) {
            throw new IllegalArgumentException(
                String.format(
                    "Cannot add implementation: runtime '%s' already exists in this definition. "
                        + "Use updateImpl to replace it.",
                    implToAdd.runtime()));
          }
        }
        List<FunctionImpl> impls = new ArrayList<>(Arrays.asList(def.impls()));
        impls.add(implToAdd);
        result.add(FunctionDefinitions.of(def.parameters(), impls.toArray(new FunctionImpl[0])));
      } else {
        result.add(def);
      }
    }

    if (!found) {
      throw new IllegalArgumentException(
          "Cannot add implementation: no definition found with the specified parameters");
    }

    return result;
  }

  private List<FunctionDefinition> updateImplInDefinition(
      List<FunctionDefinition> definitions,
      FunctionParam[] targetParams,
      FunctionImpl.RuntimeType runtime,
      FunctionImpl newImpl) {
    List<FunctionDefinition> result = new ArrayList<>();
    boolean definitionFound = false;
    boolean runtimeFound = false;

    for (FunctionDefinition def : definitions) {
      if (parametersMatch(def.parameters(), targetParams)) {
        definitionFound = true;
        List<FunctionImpl> impls = new ArrayList<>();
        for (FunctionImpl impl : def.impls()) {
          if (impl.runtime() == runtime) {
            runtimeFound = true;
            impls.add(newImpl);
          } else {
            impls.add(impl);
          }
        }
        result.add(FunctionDefinitions.of(def.parameters(), impls.toArray(new FunctionImpl[0])));
      } else {
        result.add(def);
      }
    }

    if (!definitionFound) {
      throw new IllegalArgumentException(
          "Cannot update implementation: no definition found with the specified parameters");
    }

    if (!runtimeFound) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot update implementation: runtime '%s' not found in the definition", runtime));
    }

    return result;
  }

  private List<FunctionDefinition> removeImplFromDefinition(
      List<FunctionDefinition> definitions,
      FunctionParam[] targetParams,
      FunctionImpl.RuntimeType runtime) {
    List<FunctionDefinition> result = new ArrayList<>();
    boolean definitionFound = false;
    boolean runtimeFound = false;

    for (FunctionDefinition def : definitions) {
      if (parametersMatch(def.parameters(), targetParams)) {
        definitionFound = true;

        // Check if this is the only implementation
        if (def.impls().length == 1) {
          if (def.impls()[0].runtime() == runtime) {
            throw new IllegalArgumentException(
                "Cannot remove the only implementation. Use removeDefinition to remove the entire definition.");
          }
        }

        List<FunctionImpl> impls = new ArrayList<>();
        for (FunctionImpl impl : def.impls()) {
          if (impl.runtime() == runtime) {
            runtimeFound = true;
          } else {
            impls.add(impl);
          }
        }
        result.add(FunctionDefinitions.of(def.parameters(), impls.toArray(new FunctionImpl[0])));
      } else {
        result.add(def);
      }
    }

    if (!definitionFound) {
      throw new IllegalArgumentException(
          "Cannot remove implementation: no definition found with the specified parameters");
    }

    if (!runtimeFound) {
      throw new IllegalArgumentException(
          String.format(
              "Cannot remove implementation: runtime '%s' not found in the definition", runtime));
    }

    return result;
  }
}
