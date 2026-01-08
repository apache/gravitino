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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.FunctionAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchFunctionException;
import org.apache.gravitino.exceptions.NoSuchFunctionVersionException;
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

  private static final int INIT_VERSION = 0;

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
    return getFunction(ident, FunctionEntity.LATEST_VERSION);
  }

  @Override
  public Function getFunction(NameIdentifier ident, int version)
      throws NoSuchFunctionException, NoSuchFunctionVersionException {
    NameIdentifier versionedIdent = toVersionedIdent(ident, version);
    try {
      return store.get(versionedIdent, Entity.EntityType.FUNCTION, FunctionEntity.class);

    } catch (NoSuchEntityException e) {
      if (version == FunctionEntity.LATEST_VERSION) {
        throw new NoSuchFunctionException(e, "Function %s does not exist", ident);
      }
      // Check if the function exists at all
      try {
        store.get(
            toVersionedIdent(ident, FunctionEntity.LATEST_VERSION),
            Entity.EntityType.FUNCTION,
            FunctionEntity.class);
        // Function exists, but version doesn't
        throw new NoSuchFunctionVersionException(
            e, "Function %s version %d does not exist", ident, version);
      } catch (NoSuchEntityException | IOException ex) {
        throw new NoSuchFunctionException(e, "Function %s does not exist", ident);
      }
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
        ident, comment, functionType, deterministic, returnType, null, definitions);
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
        ident, comment, FunctionType.TABLE, deterministic, null, returnColumns, definitions);
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

  /**
   * Converts a function identifier to a versioned identifier. The versioned identifier uses the
   * version number as the name to allow the store to retrieve specific versions.
   *
   * @param ident The function identifier.
   * @param version The version number, or {@link FunctionEntity#LATEST_VERSION} for the latest.
   * @return The versioned identifier.
   */
  private NameIdentifier toVersionedIdent(NameIdentifier ident, int version) {
    return NameIdentifier.of(
        ident.namespace().level(0),
        ident.namespace().level(1),
        ident.namespace().level(2),
        ident.name(),
        String.valueOf(version));
  }

  private Function doRegisterFunction(
      NameIdentifier ident,
      String comment,
      FunctionType functionType,
      boolean deterministic,
      Type returnType,
      FunctionColumn[] returnColumns,
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
            .withReturnType(returnType)
            .withReturnColumns(returnColumns)
            .withDefinitions(definitions)
            .withVersion(INIT_VERSION)
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
   */
  private void validateDefinitionsNoArityOverlap(FunctionDefinition[] definitions) {
    for (int i = 0; i < definitions.length; i++) {
      Set<String> aritiesI = computeArities(definitions[i]);
      for (int j = i + 1; j < definitions.length; j++) {
        Set<String> aritiesJ = computeArities(definitions[j]);
        for (String arity : aritiesI) {
          if (aritiesJ.contains(arity)) {
            throw new IllegalArgumentException(
                String.format(
                    "Cannot register function: definitions at index %d and %d have overlapping "
                        + "arity '%s'. This would create ambiguous function invocations.",
                    i, j, arity));
          }
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
    Set<String> arities = new HashSet<>();
    FunctionParam[] params = definition.parameters();

    // Ensure optional parameters come last to prevent incorrect arity computation.
    // Without this check, func(a=1, b) would wrongly generate arity "" (0 args),
    // but 0-arg calls are invalid since 'b' is required
    boolean foundDefault = false;
    for (int i = 0; i < params.length; i++) {
      Expression defaultValue = params[i].defaultValue();
      boolean hasDefault = defaultValue != null && defaultValue != Column.DEFAULT_VALUE_NOT_SET;
      if (foundDefault && !hasDefault) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid parameter order: required parameter '%s' at position %d "
                    + "follows optional parameter(s). All parameters with default values "
                    + "must appear at the end of the parameter list.",
                params[i].name(), i));
      }
      if (hasDefault) {
        foundDefault = true;
      }
    }

    // Find the first parameter with a default value
    int firstDefaultIndex = params.length;
    for (int i = 0; i < params.length; i++) {
      if (params[i].defaultValue() != Column.DEFAULT_VALUE_NOT_SET) {
        firstDefaultIndex = i;
        break;
      }
    }

    // Generate all possible arities from firstDefaultIndex to params.length
    for (int i = firstDefaultIndex; i <= params.length; i++) {
      StringBuilder arity = new StringBuilder();
      for (int j = 0; j < i; j++) {
        if (j > 0) {
          arity.append(",");
        }
        arity.append(params[j].dataType().simpleString());
      }
      arities.add(arity.toString());
    }

    return arities;
  }
}
