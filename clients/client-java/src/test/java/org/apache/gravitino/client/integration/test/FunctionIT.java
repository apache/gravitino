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
package org.apache.gravitino.client.integration.test;

import com.google.common.collect.Maps;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.FunctionAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchFunctionException;
import org.apache.gravitino.function.Function;
import org.apache.gravitino.function.FunctionCatalog;
import org.apache.gravitino.function.FunctionChange;
import org.apache.gravitino.function.FunctionColumn;
import org.apache.gravitino.function.FunctionDefinition;
import org.apache.gravitino.function.FunctionDefinitions;
import org.apache.gravitino.function.FunctionImpl;
import org.apache.gravitino.function.FunctionImpls;
import org.apache.gravitino.function.FunctionParam;
import org.apache.gravitino.function.FunctionParams;
import org.apache.gravitino.function.FunctionType;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-test")
public class FunctionIT extends BaseIT {

  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();

  private static final String METALAKE_NAME =
      GravitinoITUtils.genRandomName("function_it_metalake");

  private static String hmsUri;

  private GravitinoMetalake metalake;
  private Catalog catalog;
  private String schemaName;
  private FunctionCatalog functionCatalog;

  @BeforeAll
  public void setUp() {
    containerSuite.startHiveContainer();
    hmsUri =
        String.format(
            "thrift://%s:%d",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HIVE_METASTORE_PORT);

    metalake = client.createMetalake(METALAKE_NAME, "metalake comment", Collections.emptyMap());
  }

  @AfterAll
  public void tearDown() {
    client.dropMetalake(METALAKE_NAME, true);

    if (client != null) {
      client.close();
      client = null;
    }

    try {
      closer.close();
    } catch (Exception e) {
      // Swallow exceptions
    }
  }

  @BeforeEach
  public void createCatalogAndSchema() {
    String catalogName = GravitinoITUtils.genRandomName("function_it_catalog");
    Map<String, String> properties = Maps.newHashMap();
    properties.put("metastore.uris", hmsUri);

    catalog =
        metalake.createCatalog(
            catalogName, Catalog.Type.RELATIONAL, "hive", "catalog comment", properties);

    schemaName = GravitinoITUtils.genRandomName("function_it_schema");
    catalog.asSchemas().createSchema(schemaName, "schema comment", Collections.emptyMap());

    functionCatalog = catalog.asFunctionCatalog();
  }

  @AfterEach
  public void cleanUp() {
    metalake.dropCatalog(catalog.name(), true);
  }

  @Test
  public void testRegisterAndGetFunction() {
    // Test 1: Register and get a deterministic scalar function
    String functionName = GravitinoITUtils.genRandomName("test_func");
    NameIdentifier ident = NameIdentifier.of(schemaName, functionName);

    FunctionParam param =
        FunctionParams.of("x", Types.IntegerType.get(), null, Literals.integerLiteral(0));
    FunctionImpl impl = FunctionImpls.ofSql(FunctionImpl.RuntimeType.SPARK, "SELECT x + 1");
    FunctionDefinition definition =
        FunctionDefinitions.of(
            new FunctionParam[] {param}, Types.IntegerType.get(), new FunctionImpl[] {impl});

    Function registered =
        functionCatalog.registerFunction(
            ident,
            "Add one to input",
            FunctionType.SCALAR,
            true,
            new FunctionDefinition[] {definition});

    Assertions.assertEquals(functionName, registered.name());
    Assertions.assertEquals(FunctionType.SCALAR, registered.functionType());
    Assertions.assertTrue(registered.deterministic());
    Assertions.assertEquals("Add one to input", registered.comment());
    Assertions.assertEquals(Types.IntegerType.get(), registered.definitions()[0].returnType());
    Assertions.assertEquals(1, registered.definitions().length);

    Function loaded = functionCatalog.getFunction(ident);
    Assertions.assertEquals(functionName, loaded.name());
    Assertions.assertEquals(FunctionType.SCALAR, loaded.functionType());
    Assertions.assertTrue(loaded.deterministic());
    Assertions.assertEquals("Add one to input", loaded.comment());
    Assertions.assertEquals(Types.IntegerType.get(), loaded.definitions()[0].returnType());

    Assertions.assertTrue(functionCatalog.functionExists(ident));
    Assertions.assertFalse(
        functionCatalog.functionExists(NameIdentifier.of(schemaName, "non_existent_func")));

    // Test 2: Register a non-deterministic function
    String nonDetFuncName = GravitinoITUtils.genRandomName("nondeterministic_func");
    NameIdentifier nonDetIdent = NameIdentifier.of(schemaName, nonDetFuncName);

    FunctionImpl nonDetImpl = FunctionImpls.ofSql(FunctionImpl.RuntimeType.SPARK, "SELECT RAND()");
    FunctionDefinition nonDetDefinition =
        FunctionDefinitions.of(
            new FunctionParam[0], Types.DoubleType.get(), new FunctionImpl[] {nonDetImpl});

    Function nonDetRegistered =
        functionCatalog.registerFunction(
            nonDetIdent,
            "Non-deterministic function",
            FunctionType.SCALAR,
            false,
            new FunctionDefinition[] {nonDetDefinition});

    Assertions.assertFalse(nonDetRegistered.deterministic());

    // Test 3: Register a table function
    String tableFuncName = GravitinoITUtils.genRandomName("table_func");
    NameIdentifier tableFuncIdent = NameIdentifier.of(schemaName, tableFuncName);

    FunctionParam tableParam = FunctionParams.of("n", Types.IntegerType.get());
    FunctionImpl tableImpl =
        FunctionImpls.ofJava(FunctionImpl.RuntimeType.SPARK, "com.example.GenerateRowsUDTF");
    FunctionColumn[] returnColumns =
        new FunctionColumn[] {FunctionColumn.of("x", Types.StringType.get(), "comment")};
    FunctionDefinition tableDefinition =
        FunctionDefinitions.of(
            new FunctionParam[] {tableParam}, returnColumns, new FunctionImpl[] {tableImpl});

    Function tableRegistered =
        functionCatalog.registerFunction(
            tableFuncIdent,
            "Table function",
            FunctionType.TABLE,
            true,
            new FunctionDefinition[] {tableDefinition});

    Assertions.assertEquals(FunctionType.TABLE, tableRegistered.functionType());
    Assertions.assertArrayEquals(returnColumns, tableRegistered.definitions()[0].returnColumns());
  }

  @Test
  public void testRegisterFunctionConflict() {
    // Test 1: Register function with same name should fail
    String functionName = GravitinoITUtils.genRandomName("test_func");
    NameIdentifier ident = NameIdentifier.of(schemaName, functionName);

    FunctionParam param = FunctionParams.of("x", Types.IntegerType.get());
    FunctionImpl impl = FunctionImpls.ofSql(FunctionImpl.RuntimeType.SPARK, "SELECT x + 1");
    FunctionDefinition definition =
        FunctionDefinitions.of(
            new FunctionParam[] {param}, Types.IntegerType.get(), new FunctionImpl[] {impl});

    functionCatalog.registerFunction(
        ident, "comment", FunctionType.SCALAR, true, new FunctionDefinition[] {definition});

    Assertions.assertThrows(
        FunctionAlreadyExistsException.class,
        () ->
            functionCatalog.registerFunction(
                ident,
                "comment",
                FunctionType.SCALAR,
                true,
                new FunctionDefinition[] {definition}));

    // Test 2: Register function with ambiguous definitions should fail
    // foo(int, float default 1.0) and foo(int, string default 'x') both support call foo(1)
    String ambiguousFuncName = GravitinoITUtils.genRandomName("ambiguous_func");
    NameIdentifier ambiguousIdent = NameIdentifier.of(schemaName, ambiguousFuncName);

    FunctionParam intParam = FunctionParams.of("x", Types.IntegerType.get());
    FunctionParam floatParamWithDefault =
        FunctionParams.of("y", Types.FloatType.get(), "1.0", Literals.floatLiteral(1.0f));
    FunctionParam stringParamWithDefault =
        FunctionParams.of("z", Types.StringType.get(), "'x'", Literals.stringLiteral("x"));

    FunctionImpl ambiguousImpl =
        FunctionImpls.ofSql(FunctionImpl.RuntimeType.SPARK, "SELECT x + 1");

    // Definition 1: foo(int, float default 1.0) supports arities: (int), (int, float)
    FunctionDefinition def1 =
        FunctionDefinitions.of(
            new FunctionParam[] {intParam, floatParamWithDefault},
            Types.IntegerType.get(),
            new FunctionImpl[] {ambiguousImpl});

    // Definition 2: foo(int, string default 'x') supports arities: (int), (int, string)
    FunctionDefinition def2 =
        FunctionDefinitions.of(
            new FunctionParam[] {intParam, stringParamWithDefault},
            Types.IntegerType.get(),
            new FunctionImpl[] {ambiguousImpl});

    // Both definitions support call foo(1), so this should fail
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            functionCatalog.registerFunction(
                ambiguousIdent,
                "Ambiguous function",
                FunctionType.SCALAR,
                true,
                new FunctionDefinition[] {def1, def2}));
  }

  @Test
  public void testGetFunctionNotFound() {
    // Test 1: Function not found in existing schema
    NameIdentifier ident = NameIdentifier.of(schemaName, "non_existent_func");

    Assertions.assertThrows(
        NoSuchFunctionException.class, () -> functionCatalog.getFunction(ident));

    // Test 2: Function not found when schema does not exist
    NameIdentifier identWithNonExistentSchema =
        NameIdentifier.of("non_existent_schema", "some_func");

    Assertions.assertThrows(
        NoSuchFunctionException.class,
        () -> functionCatalog.getFunction(identWithNonExistentSchema));
  }

  @Test
  public void testListFunctions() {
    // Register multiple functions
    String func1Name = GravitinoITUtils.genRandomName("list_func1");
    String func2Name = GravitinoITUtils.genRandomName("list_func2");

    FunctionParam param = FunctionParams.of("x", Types.IntegerType.get());
    FunctionImpl impl = FunctionImpls.ofSql(FunctionImpl.RuntimeType.SPARK, "SELECT x + 1");
    FunctionDefinition definition =
        FunctionDefinitions.of(
            new FunctionParam[] {param}, Types.IntegerType.get(), new FunctionImpl[] {impl});

    Function func1 =
        functionCatalog.registerFunction(
            NameIdentifier.of(schemaName, func1Name),
            "comment1",
            FunctionType.SCALAR,
            true,
            new FunctionDefinition[] {definition});

    Function func2 =
        functionCatalog.registerFunction(
            NameIdentifier.of(schemaName, func2Name),
            "comment2",
            FunctionType.SCALAR,
            true,
            new FunctionDefinition[] {definition});

    // List functions
    NameIdentifier[] functions = functionCatalog.listFunctions(Namespace.of(schemaName));
    Assertions.assertEquals(2, functions.length);

    Set<String> functionNames =
        Arrays.stream(functions).map(NameIdentifier::name).collect(Collectors.toSet());
    Assertions.assertTrue(functionNames.contains(func1Name));
    Assertions.assertTrue(functionNames.contains(func2Name));

    // List function infos
    Function[] functionInfos = functionCatalog.listFunctionInfos(Namespace.of(schemaName));
    Assertions.assertEquals(2, functionInfos.length);

    Set<Function> functionSet = new HashSet<>(Arrays.asList(functionInfos));
    Assertions.assertTrue(functionSet.contains(func1));
    Assertions.assertTrue(functionSet.contains(func2));
  }

  @Test
  public void testDropFunction() {
    String functionName = GravitinoITUtils.genRandomName("drop_func");
    NameIdentifier ident = NameIdentifier.of(schemaName, functionName);

    FunctionParam param = FunctionParams.of("x", Types.IntegerType.get());
    FunctionImpl impl = FunctionImpls.ofSql(FunctionImpl.RuntimeType.SPARK, "SELECT x + 1");
    FunctionDefinition definition =
        FunctionDefinitions.of(
            new FunctionParam[] {param}, Types.IntegerType.get(), new FunctionImpl[] {impl});

    functionCatalog.registerFunction(
        ident, "comment", FunctionType.SCALAR, true, new FunctionDefinition[] {definition});

    // Function exists
    Assertions.assertTrue(functionCatalog.functionExists(ident));

    // Drop function
    Assertions.assertTrue(functionCatalog.dropFunction(ident));

    // Function no longer exists
    Assertions.assertFalse(functionCatalog.functionExists(ident));

    // Drop again should return false
    Assertions.assertFalse(functionCatalog.dropFunction(ident));
  }

  @Test
  public void testAlterFunctionUpdateComment() {
    String functionName = GravitinoITUtils.genRandomName("alter_func");
    NameIdentifier ident = NameIdentifier.of(schemaName, functionName);

    FunctionParam param = FunctionParams.of("x", Types.IntegerType.get());
    FunctionImpl impl = FunctionImpls.ofSql(FunctionImpl.RuntimeType.SPARK, "SELECT x + 1");
    FunctionDefinition definition =
        FunctionDefinitions.of(
            new FunctionParam[] {param}, Types.IntegerType.get(), new FunctionImpl[] {impl});

    functionCatalog.registerFunction(
        ident,
        "original comment",
        FunctionType.SCALAR,
        true,
        new FunctionDefinition[] {definition});

    // Alter comment
    Function altered =
        functionCatalog.alterFunction(ident, FunctionChange.updateComment("updated comment"));

    Assertions.assertEquals("updated comment", altered.comment());

    // Verify the change persisted
    Function loaded = functionCatalog.getFunction(ident);
    Assertions.assertEquals("updated comment", loaded.comment());
  }

  @Test
  public void testAlterFunctionAddDefinition() {
    String functionName = GravitinoITUtils.genRandomName("alter_def_func");
    NameIdentifier ident = NameIdentifier.of(schemaName, functionName);

    // Create initial function with one definition
    FunctionParam param1 = FunctionParams.of("x", Types.IntegerType.get());
    FunctionImpl impl1 = FunctionImpls.ofSql(FunctionImpl.RuntimeType.SPARK, "SELECT x + 1");
    FunctionDefinition definition1 =
        FunctionDefinitions.of(
            new FunctionParam[] {param1}, Types.IntegerType.get(), new FunctionImpl[] {impl1});

    functionCatalog.registerFunction(
        ident, "comment", FunctionType.SCALAR, true, new FunctionDefinition[] {definition1});

    // Add a new definition with different parameter type
    FunctionParam param2 = FunctionParams.of("x", Types.StringType.get());
    FunctionImpl impl2 =
        FunctionImpls.ofSql(FunctionImpl.RuntimeType.SPARK, "SELECT CONCAT(x, '_suffix')");
    FunctionDefinition definition2 =
        FunctionDefinitions.of(
            new FunctionParam[] {param2}, Types.StringType.get(), new FunctionImpl[] {impl2});

    Function altered =
        functionCatalog.alterFunction(ident, FunctionChange.addDefinition(definition2));

    Assertions.assertEquals(2, altered.definitions().length);
  }

  @Test
  public void testAlterFunctionImplOperations() {
    String functionName = GravitinoITUtils.genRandomName("impl_ops_func");
    NameIdentifier ident = NameIdentifier.of(schemaName, functionName);

    // Create function with Spark implementation
    FunctionParam param = FunctionParams.of("x", Types.IntegerType.get());
    FunctionImpl sparkImpl = FunctionImpls.ofSql(FunctionImpl.RuntimeType.SPARK, "SELECT x + 1");
    FunctionDefinition definition =
        FunctionDefinitions.of(
            new FunctionParam[] {param}, Types.IntegerType.get(), new FunctionImpl[] {sparkImpl});

    functionCatalog.registerFunction(
        ident, "comment", FunctionType.SCALAR, true, new FunctionDefinition[] {definition});

    // Test 1: Add Trino implementation
    FunctionImpl trinoImpl = FunctionImpls.ofSql(FunctionImpl.RuntimeType.TRINO, "SELECT x + 1");
    Function afterAdd =
        functionCatalog.alterFunction(
            ident, FunctionChange.addImpl(new FunctionParam[] {param}, trinoImpl));

    Assertions.assertEquals(1, afterAdd.definitions().length);
    Assertions.assertEquals(2, afterAdd.definitions()[0].impls().length);

    // Test 2: Update Spark implementation
    FunctionImpl newSparkImpl =
        FunctionImpls.ofSql(FunctionImpl.RuntimeType.SPARK, "SELECT x + 10");
    Function afterUpdate =
        functionCatalog.alterFunction(
            ident,
            FunctionChange.updateImpl(
                new FunctionParam[] {param}, FunctionImpl.RuntimeType.SPARK, newSparkImpl));

    Assertions.assertEquals(1, afterUpdate.definitions().length);
    Assertions.assertEquals(2, afterUpdate.definitions()[0].impls().length);

    // Test 3: Remove Trino implementation
    Function afterRemove =
        functionCatalog.alterFunction(
            ident,
            FunctionChange.removeImpl(new FunctionParam[] {param}, FunctionImpl.RuntimeType.TRINO));

    Assertions.assertEquals(1, afterRemove.definitions().length);
    Assertions.assertEquals(1, afterRemove.definitions()[0].impls().length);
    Assertions.assertEquals(
        FunctionImpl.RuntimeType.SPARK, afterRemove.definitions()[0].impls()[0].runtime());

    // Test 4: Remove the last impl should fail
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            functionCatalog.alterFunction(
                ident,
                FunctionChange.removeImpl(
                    new FunctionParam[] {param}, FunctionImpl.RuntimeType.SPARK)));
  }

  @Test
  public void testRegisterFunctionWithImpls() {
    // Test 1: Register function with Java impl
    String javaFuncName = GravitinoITUtils.genRandomName("java_func");
    NameIdentifier javaIdent = NameIdentifier.of(schemaName, javaFuncName);

    FunctionParam javaParam = FunctionParams.of("x", Types.IntegerType.get());
    FunctionImpl javaImpl =
        FunctionImpls.ofJava(FunctionImpl.RuntimeType.SPARK, "com.example.AddOneUDF");
    FunctionDefinition javaDefinition =
        FunctionDefinitions.of(
            new FunctionParam[] {javaParam},
            Types.IntegerType.get(),
            new FunctionImpl[] {javaImpl});

    Function javaRegistered =
        functionCatalog.registerFunction(
            javaIdent,
            "Java UDF",
            FunctionType.SCALAR,
            true,
            new FunctionDefinition[] {javaDefinition});

    Assertions.assertEquals(javaFuncName, javaRegistered.name());
    Assertions.assertEquals(
        FunctionImpl.Language.JAVA, javaRegistered.definitions()[0].impls()[0].language());

    // Test 2: Register function with Python impl
    String pythonFuncName = GravitinoITUtils.genRandomName("python_func");
    NameIdentifier pythonIdent = NameIdentifier.of(schemaName, pythonFuncName);

    FunctionParam pythonParam = FunctionParams.of("x", Types.IntegerType.get());
    FunctionImpl pythonImpl =
        FunctionImpls.ofPython(
            FunctionImpl.RuntimeType.SPARK,
            "add_one",
            "def add_one(x):\n    return x + 1",
            null,
            null);
    FunctionDefinition pythonDefinition =
        FunctionDefinitions.of(
            new FunctionParam[] {pythonParam},
            Types.IntegerType.get(),
            new FunctionImpl[] {pythonImpl});

    Function pythonRegistered =
        functionCatalog.registerFunction(
            pythonIdent,
            "Python UDF",
            FunctionType.SCALAR,
            true,
            new FunctionDefinition[] {pythonDefinition});

    Assertions.assertEquals(pythonFuncName, pythonRegistered.name());
    Assertions.assertEquals(
        FunctionImpl.Language.PYTHON, pythonRegistered.definitions()[0].impls()[0].language());

    // Test 3: Register function with multiple definitions
    String multiFuncName = GravitinoITUtils.genRandomName("multi_def_func");
    NameIdentifier multiIdent = NameIdentifier.of(schemaName, multiFuncName);

    FunctionParam param1 = FunctionParams.of("x", Types.IntegerType.get());
    FunctionImpl impl1 = FunctionImpls.ofSql(FunctionImpl.RuntimeType.SPARK, "SELECT x + 1");
    FunctionDefinition definition1 =
        FunctionDefinitions.of(
            new FunctionParam[] {param1}, Types.IntegerType.get(), new FunctionImpl[] {impl1});

    FunctionParam param2 = FunctionParams.of("x", Types.StringType.get());
    FunctionImpl impl2 =
        FunctionImpls.ofSql(FunctionImpl.RuntimeType.SPARK, "SELECT CONCAT(x, '_suffix')");
    FunctionDefinition definition2 =
        FunctionDefinitions.of(
            new FunctionParam[] {param2}, Types.StringType.get(), new FunctionImpl[] {impl2});

    Function multiRegistered =
        functionCatalog.registerFunction(
            multiIdent,
            "Overloaded function",
            FunctionType.SCALAR,
            true,
            new FunctionDefinition[] {definition1, definition2});

    Assertions.assertEquals(2, multiRegistered.definitions().length);
  }

  @Test
  public void testAlterFunctionRemoveDefinition() {
    String functionName = GravitinoITUtils.genRandomName("remove_def_func");
    NameIdentifier ident = NameIdentifier.of(schemaName, functionName);

    // Create function with two definitions
    FunctionParam param1 = FunctionParams.of("x", Types.IntegerType.get());
    FunctionImpl impl1 = FunctionImpls.ofSql(FunctionImpl.RuntimeType.SPARK, "SELECT x + 1");
    FunctionDefinition definition1 =
        FunctionDefinitions.of(
            new FunctionParam[] {param1}, Types.IntegerType.get(), new FunctionImpl[] {impl1});

    FunctionParam param2 = FunctionParams.of("x", Types.StringType.get());
    FunctionImpl impl2 =
        FunctionImpls.ofSql(FunctionImpl.RuntimeType.SPARK, "SELECT CONCAT(x, '_suffix')");
    FunctionDefinition definition2 =
        FunctionDefinitions.of(
            new FunctionParam[] {param2}, Types.StringType.get(), new FunctionImpl[] {impl2});

    functionCatalog.registerFunction(
        ident,
        "comment",
        FunctionType.SCALAR,
        true,
        new FunctionDefinition[] {definition1, definition2});

    // Remove one definition
    Function altered =
        functionCatalog.alterFunction(
            ident, FunctionChange.removeDefinition(new FunctionParam[] {param1}));

    Assertions.assertEquals(1, altered.definitions().length);
    // The remaining definition should be the one with String parameter
    Assertions.assertEquals(
        Types.StringType.get(), altered.definitions()[0].parameters()[0].dataType());

    // Test removing the last definition should fail
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            functionCatalog.alterFunction(
                ident, FunctionChange.removeDefinition(new FunctionParam[] {param2})));
  }
}
