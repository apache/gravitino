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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
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
import org.apache.gravitino.function.Function;
import org.apache.gravitino.function.FunctionDefinition;
import org.apache.gravitino.function.FunctionDefinitions;
import org.apache.gravitino.function.FunctionImpl;
import org.apache.gravitino.function.FunctionImpls;
import org.apache.gravitino.function.FunctionParam;
import org.apache.gravitino.function.FunctionParams;
import org.apache.gravitino.function.FunctionType;
import org.apache.gravitino.meta.FunctionEntity;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestManagedFunctionOperations {

  private static final String METALAKE_NAME = "test_metalake";
  private static final String CATALOG_NAME = "test_catalog";
  private static final String SCHEMA_NAME = "schema1";

  private final IdGenerator idGenerator = new RandomIdGenerator();
  private final Map<NameIdentifier, FunctionEntity> entityMap = new HashMap<>();

  private EntityStore store;
  private ManagedFunctionOperations functionOperations;

  @BeforeEach
  public void setUp() throws Exception {
    entityMap.clear();
    store = createMockEntityStore();
    functionOperations = new ManagedFunctionOperations(store, idGenerator);
  }

  @Test
  public void testRegisterAndListFunctions() {
    NameIdentifier func1Ident = getFunctionIdent("func1");
    FunctionParam[] params1 = new FunctionParam[] {FunctionParams.of("a", Types.IntegerType.get())};
    FunctionDefinition[] definitions1 = new FunctionDefinition[] {createSimpleDefinition(params1)};

    functionOperations.registerFunction(
        func1Ident,
        "Test function 1",
        FunctionType.SCALAR,
        true,
        Types.StringType.get(),
        definitions1);

    NameIdentifier func2Ident = getFunctionIdent("func2");
    FunctionParam[] params2 =
        new FunctionParam[] {
          FunctionParams.of("x", Types.StringType.get()),
          FunctionParams.of("y", Types.StringType.get())
        };
    FunctionDefinition[] definitions2 = new FunctionDefinition[] {createSimpleDefinition(params2)};

    functionOperations.registerFunction(
        func2Ident,
        "Test function 2",
        FunctionType.SCALAR,
        false,
        Types.IntegerType.get(),
        definitions2);

    // List functions
    NameIdentifier[] functionIdents = functionOperations.listFunctions(getFunctionNamespace());
    Assertions.assertEquals(2, functionIdents.length);
    Set<String> functionNames =
        Arrays.stream(functionIdents).map(NameIdentifier::name).collect(Collectors.toSet());

    Assertions.assertTrue(functionNames.contains("func1"));
    Assertions.assertTrue(functionNames.contains("func2"));
  }

  @Test
  public void testRegisterAndGetFunction() {
    NameIdentifier funcIdent = getFunctionIdent("my_func");
    FunctionParam[] params =
        new FunctionParam[] {FunctionParams.of("input", Types.StringType.get())};
    FunctionDefinition[] definitions = new FunctionDefinition[] {createSimpleDefinition(params)};

    Function newFunc =
        functionOperations.registerFunction(
            funcIdent,
            "My test function",
            FunctionType.SCALAR,
            true,
            Types.IntegerType.get(),
            definitions);

    Assertions.assertEquals("my_func", newFunc.name());
    Assertions.assertEquals("My test function", newFunc.comment());
    Assertions.assertEquals(FunctionType.SCALAR, newFunc.functionType());
    Assertions.assertTrue(newFunc.deterministic());
    Assertions.assertEquals(Types.IntegerType.get(), newFunc.returnType());
    Assertions.assertEquals(0, newFunc.version());

    // Get function (latest version)
    Function loadedFunc = functionOperations.getFunction(funcIdent);
    Assertions.assertEquals(newFunc.name(), loadedFunc.name());
    Assertions.assertEquals(newFunc.comment(), loadedFunc.comment());

    // Test register function that already exists
    Assertions.assertThrows(
        FunctionAlreadyExistsException.class,
        () ->
            functionOperations.registerFunction(
                funcIdent,
                "Another function",
                FunctionType.SCALAR,
                true,
                Types.StringType.get(),
                definitions));

    // Test get non-existing function
    NameIdentifier nonExistingIdent = getFunctionIdent("non_existing_func");
    Assertions.assertThrows(
        NoSuchFunctionException.class, () -> functionOperations.getFunction(nonExistingIdent));
  }

  @Test
  public void testRegisterAndDropFunction() {
    NameIdentifier funcIdent = getFunctionIdent("func_to_drop");
    FunctionParam[] params = new FunctionParam[] {FunctionParams.of("a", Types.IntegerType.get())};
    FunctionDefinition[] definitions = new FunctionDefinition[] {createSimpleDefinition(params)};

    functionOperations.registerFunction(
        funcIdent,
        "Function to drop",
        FunctionType.SCALAR,
        true,
        Types.StringType.get(),
        definitions);

    // Drop the function
    boolean dropped = functionOperations.dropFunction(funcIdent);
    Assertions.assertTrue(dropped);

    // Verify the function is dropped
    Assertions.assertThrows(
        NoSuchFunctionException.class, () -> functionOperations.getFunction(funcIdent));

    // Test drop non-existing function
    Assertions.assertFalse(functionOperations.dropFunction(funcIdent));
  }

  @Test
  public void testRegisterFunctionWithOverlappingDefinitions() {
    NameIdentifier funcIdent = getFunctionIdent("func_overlap_register");

    // Try to register with two definitions that have overlapping arities
    FunctionParam[] params1 =
        new FunctionParam[] {
          FunctionParams.of("a", Types.IntegerType.get()),
          FunctionParams.of("b", Types.FloatType.get(), null, Literals.floatLiteral(1.0f))
        };
    FunctionParam[] params2 =
        new FunctionParam[] {
          FunctionParams.of("a", Types.IntegerType.get()),
          FunctionParams.of("c", Types.StringType.get(), null, Literals.stringLiteral("x"))
        };

    FunctionDefinition[] definitions =
        new FunctionDefinition[] {createSimpleDefinition(params1), createSimpleDefinition(params2)};

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            functionOperations.registerFunction(
                funcIdent,
                "Test function",
                FunctionType.SCALAR,
                true,
                Types.StringType.get(),
                definitions));
  }

  @Test
  public void testInvalidParameterOrder() {
    // Test that parameters with default values must appear at the end
    NameIdentifier funcIdent = getFunctionIdent("func_invalid_params");

    // Create params with invalid order: (a default 1, b required, c default 2)
    FunctionParam[] invalidParams =
        new FunctionParam[] {
          FunctionParams.of("a", Types.IntegerType.get(), "param a", Literals.integerLiteral(1)),
          FunctionParams.of("b", Types.StringType.get()), // Required param after optional
          FunctionParams.of("c", Types.IntegerType.get(), "param c", Literals.integerLiteral(2))
        };
    FunctionDefinition[] definitions =
        new FunctionDefinition[] {createSimpleDefinition(invalidParams)};

    // Should throw IllegalArgumentException when trying to register
    IllegalArgumentException ex =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                functionOperations.registerFunction(
                    funcIdent,
                    "Invalid function",
                    FunctionType.SCALAR,
                    true,
                    Types.StringType.get(),
                    definitions));

    Assertions.assertTrue(
        ex.getMessage().contains("Invalid parameter order"),
        "Expected error about invalid parameter order, got: " + ex.getMessage());
    Assertions.assertTrue(
        ex.getMessage().contains("required parameter 'b'"),
        "Expected error to mention parameter 'b', got: " + ex.getMessage());
    Assertions.assertTrue(
        ex.getMessage().contains("position 1"),
        "Expected error to mention position 1, got: " + ex.getMessage());

    // Test with valid order: all optional params at the end
    FunctionParam[] validParams =
        new FunctionParam[] {
          FunctionParams.of("a", Types.IntegerType.get()),
          FunctionParams.of("b", Types.StringType.get()),
          FunctionParams.of("c", Types.IntegerType.get(), "param c", Literals.integerLiteral(1)),
          FunctionParams.of("d", Types.IntegerType.get(), "param d", Literals.integerLiteral(2))
        };
    FunctionDefinition[] validDefinitions =
        new FunctionDefinition[] {createSimpleDefinition(validParams)};

    // This should succeed
    functionOperations.registerFunction(
        funcIdent,
        "Valid function",
        FunctionType.SCALAR,
        true,
        Types.StringType.get(),
        validDefinitions);

    // Verify the function was registered
    Function func = functionOperations.getFunction(funcIdent);
    Assertions.assertNotNull(func);
    Assertions.assertEquals("Valid function", func.comment());
  }

  @Test
  public void testNonOverlappingDefinitions() {
    // Test that definitions with different arities can coexist
    NameIdentifier funcIdent = getFunctionIdent("func_non_overlap");

    // Two definitions with completely different parameter types (no overlap)
    FunctionParam[] params1 = new FunctionParam[] {FunctionParams.of("a", Types.IntegerType.get())};
    FunctionParam[] params2 = new FunctionParam[] {FunctionParams.of("a", Types.StringType.get())};

    FunctionDefinition[] definitions =
        new FunctionDefinition[] {createSimpleDefinition(params1), createSimpleDefinition(params2)};

    // Should succeed - no arity overlap
    Function func =
        functionOperations.registerFunction(
            funcIdent,
            "Non-overlapping function",
            FunctionType.SCALAR,
            true,
            Types.StringType.get(),
            definitions);

    Assertions.assertNotNull(func);
    Assertions.assertEquals(2, func.definitions().length);
  }

  @Test
  public void testNoArgsFunction() {
    // Test function with no parameters
    NameIdentifier funcIdent = getFunctionIdent("func_no_args");

    FunctionParam[] params = new FunctionParam[] {};
    FunctionDefinition[] definitions = new FunctionDefinition[] {createSimpleDefinition(params)};

    Function func =
        functionOperations.registerFunction(
            funcIdent,
            "No args function",
            FunctionType.SCALAR,
            true,
            Types.StringType.get(),
            definitions);

    Assertions.assertNotNull(func);
    Assertions.assertEquals(0, func.definitions()[0].parameters().length);
  }

  @Test
  public void testMultipleDefaultParams() {
    // Test function with multiple default parameters generates correct arities
    NameIdentifier funcIdent = getFunctionIdent("func_multi_default");

    // foo(int a, float b default 1.0, string c default 'x')
    // Should generate arities: ["integer"], ["integer,float"], ["integer,float,string"]
    FunctionParam[] params =
        new FunctionParam[] {
          FunctionParams.of("a", Types.IntegerType.get()),
          FunctionParams.of("b", Types.FloatType.get(), "param b", Literals.floatLiteral(1.0f)),
          FunctionParams.of("c", Types.StringType.get(), "param c", Literals.stringLiteral("x"))
        };
    FunctionDefinition[] definitions = new FunctionDefinition[] {createSimpleDefinition(params)};

    Function func =
        functionOperations.registerFunction(
            funcIdent,
            "Multi default function",
            FunctionType.SCALAR,
            true,
            Types.StringType.get(),
            definitions);

    Assertions.assertNotNull(func);
    Assertions.assertEquals(3, func.definitions()[0].parameters().length);
  }

  @Test
  public void testOverlappingAritiesWithDifferentTypes() {
    // Test that two definitions with same arity count but different types don't overlap
    NameIdentifier funcIdent = getFunctionIdent("func_same_arity_diff_types");

    // foo(int, int) and foo(string, string) - same arity count but different types
    FunctionParam[] params1 =
        new FunctionParam[] {
          FunctionParams.of("a", Types.IntegerType.get()),
          FunctionParams.of("b", Types.IntegerType.get())
        };
    FunctionParam[] params2 =
        new FunctionParam[] {
          FunctionParams.of("a", Types.StringType.get()),
          FunctionParams.of("b", Types.StringType.get())
        };

    FunctionDefinition[] definitions =
        new FunctionDefinition[] {createSimpleDefinition(params1), createSimpleDefinition(params2)};

    // Should succeed - arities are "integer,integer" vs "string,string"
    Function func =
        functionOperations.registerFunction(
            funcIdent,
            "Same arity different types",
            FunctionType.SCALAR,
            true,
            Types.StringType.get(),
            definitions);

    Assertions.assertNotNull(func);
    Assertions.assertEquals(2, func.definitions().length);
  }

  @SuppressWarnings("unchecked")
  private EntityStore createMockEntityStore() throws Exception {
    EntityStore mockStore = mock(EntityStore.class);

    // Mock put operation
    doAnswer(
            invocation -> {
              FunctionEntity entity = invocation.getArgument(0);
              boolean overwrite = invocation.getArgument(1);
              NameIdentifier ident = entity.nameIdentifier();

              if (!overwrite && entityMap.containsKey(ident)) {
                throw new EntityAlreadyExistsException("Entity %s already exists", ident);
              }
              entityMap.put(ident, entity);
              return null;
            })
        .when(mockStore)
        .put(any(FunctionEntity.class), any(Boolean.class));

    // Mock get operation
    when(mockStore.get(
            any(NameIdentifier.class), eq(Entity.EntityType.FUNCTION), eq(FunctionEntity.class)))
        .thenAnswer(
            invocation -> {
              NameIdentifier ident = invocation.getArgument(0);
              FunctionEntity entity = findEntityByIdent(ident);
              if (entity == null) {
                throw new NoSuchEntityException("Entity %s does not exist", ident);
              }
              return entity;
            });

    // Mock delete operation (2 parameters - default method that calls 3-parameter version)
    when(mockStore.delete(any(NameIdentifier.class), eq(Entity.EntityType.FUNCTION)))
        .thenAnswer(
            invocation -> {
              NameIdentifier ident = invocation.getArgument(0);
              FunctionEntity entity = findEntityByIdent(ident);
              if (entity == null) {
                return false;
              }
              entityMap.remove(entity.nameIdentifier());
              return true;
            });

    // Mock list operation
    when(mockStore.list(
            any(Namespace.class), eq(FunctionEntity.class), eq(Entity.EntityType.FUNCTION)))
        .thenAnswer(
            invocation -> {
              Namespace namespace = invocation.getArgument(0);
              return entityMap.values().stream()
                  .filter(e -> e.namespace().equals(namespace))
                  .collect(Collectors.toList());
            });

    return mockStore;
  }

  /**
   * Finds an entity by identifier. This method handles both versioned identifiers (used by
   * getFunction) and original identifiers (used by alterFunction and dropFunction).
   *
   * <p>Versioned identifier format: namespace = original_namespace + function_name, name = version
   * Original identifier format: namespace = schema_namespace, name = function_name
   */
  private FunctionEntity findEntityByIdent(NameIdentifier ident) {
    // First, try to find by original identifier (direct match)
    FunctionEntity directMatch = entityMap.get(ident);
    if (directMatch != null) {
      return directMatch;
    }

    // If not found, try to interpret as versioned identifier
    String[] levels = ident.namespace().levels();
    if (levels.length < 1) {
      return null;
    }
    String functionName = levels[levels.length - 1];
    Namespace originalNamespace = Namespace.of(Arrays.copyOf(levels, levels.length - 1));

    for (FunctionEntity entity : entityMap.values()) {
      if (entity.name().equals(functionName) && entity.namespace().equals(originalNamespace)) {
        return entity;
      }
    }
    return null;
  }

  private Namespace getFunctionNamespace() {
    return Namespace.of(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME);
  }

  private NameIdentifier getFunctionIdent(String functionName) {
    return NameIdentifier.of(getFunctionNamespace(), functionName);
  }

  private FunctionDefinition createSimpleDefinition(FunctionParam[] params) {
    FunctionImpl impl = FunctionImpls.ofJava(FunctionImpl.RuntimeType.SPARK, "com.example.TestUDF");
    return FunctionDefinitions.of(params, new FunctionImpl[] {impl});
  }
}
