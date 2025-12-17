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

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.FunctionAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchFunctionException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.function.Function;
import org.apache.gravitino.function.FunctionCatalog;
import org.apache.gravitino.function.FunctionChange;
import org.apache.gravitino.function.FunctionImpl;
import org.apache.gravitino.function.FunctionParam;
import org.apache.gravitino.function.FunctionParams;
import org.apache.gravitino.function.FunctionSignature;
import org.apache.gravitino.function.FunctionType;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Integration test that verifies function operations against an Iceberg catalog with memory
 * backend.
 */
public class FunctionCatalogIT extends BaseIT {

  /** Metalake used for function catalog verification. */
  private static final String metalakeName = GravitinoITUtils.genRandomName("function_metalake");

  /** Warehouse directory for the Iceberg memory catalog. */
  private static Path warehousePath;

  /** The metalake instance for the test. */
  private static GravitinoMetalake metalake;

  /** The Iceberg catalog under test. */
  private static Catalog icebergCatalog;

  /** The schema used for registering functions. */
  private static Schema schema;

  /**
   * Create the metalake, Iceberg catalog, and schema for function tests.
   *
   * @throws IOException if creating temporary warehouse directory fails
   */
  @BeforeAll
  public void setUp() throws IOException {
    warehousePath = Files.createTempDirectory("iceberg_memory_warehouse");
    Assertions.assertFalse(client.metalakeExists(metalakeName));
    metalake =
        client.createMetalake(
            metalakeName, "Metalake for function catalog tests", Collections.emptyMap());

    String catalogName = GravitinoITUtils.genRandomName("function_catalog");
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put("catalog-backend", "memory")
            .put("uri", "")
            .put("warehouse", warehousePath.toString())
            .build();
    icebergCatalog =
        metalake.createCatalog(
            catalogName,
            Catalog.Type.RELATIONAL,
            "lakehouse-iceberg",
            "Iceberg catalog for function tests",
            properties);

    String schemaName = GravitinoITUtils.genRandomName("function_schema");
    schema =
        icebergCatalog
            .asSchemas()
            .createSchema(schemaName, "Schema for functions", Collections.emptyMap());
  }

  /**
   * Drop created resources and clean temporary files.
   *
   * @throws IOException if deleting warehouse directory fails
   */
  @AfterAll
  public void tearDown() throws IOException {
    if (icebergCatalog != null && schema != null) {
      metalake.dropCatalog(icebergCatalog.name(), true);
    }

    if (metalake != null) {
      client.dropMetalake(metalakeName, true);
    }

    if (warehousePath != null) {
      FileUtils.deleteQuietly(warehousePath.toFile());
    }

    if (client != null) {
      client.close();
    }

    try {
      closer.close();
    } catch (Exception e) {
      // Ignore close exception
    }
  }

  /**
   * Verify registering, listing, loading, altering, and deleting functions on an Iceberg catalog.
   */
  @Test
  public void testFunctionLifecycleOnIcebergCatalog()
      throws NoSuchFunctionException, NoSuchSchemaException, FunctionAlreadyExistsException {
    FunctionCatalog functionCatalog = icebergCatalog.asFunctionCatalog();
    Namespace functionNamespace = Namespace.of(schema.name());

    String functionName = GravitinoITUtils.genRandomName("function");
    NameIdentifier functionIdent = NameIdentifier.of(schema.name(), functionName);

    Assertions.assertEquals(0, functionCatalog.listFunctions(functionNamespace).length);
    Assertions.assertFalse(functionCatalog.functionExists(functionIdent));

    FunctionImpl initialImpl =
        FunctionImpl.ofSql(
            FunctionImpl.RuntimeType.SPARK,
            "CREATE FUNCTION " + functionName + "(x INT) RETURNS INT RETURN x + 1");
    Function createdFunction =
        functionCatalog.registerFunction(
            functionIdent,
            "initial comment",
            FunctionType.SCALAR,
            true,
            new FunctionParam[] {FunctionParams.of("x", Types.IntegerType.get(), "input value")},
            Types.IntegerType.get(),
            new FunctionImpl[] {initialImpl});

    Assertions.assertEquals(functionName, createdFunction.signature().name());
    Assertions.assertEquals(FunctionType.SCALAR, createdFunction.functionType());
    Assertions.assertTrue(createdFunction.deterministic());
    Assertions.assertEquals("initial comment", createdFunction.comment());
    Assertions.assertEquals(Types.IntegerType.get(), createdFunction.returnType());
    Assertions.assertEquals(1, createdFunction.version());
    Assertions.assertEquals(1, createdFunction.impls().length);

    NameIdentifier[] listedFunctions = functionCatalog.listFunctions(functionNamespace);
    Assertions.assertEquals(1, listedFunctions.length);
    Assertions.assertEquals(functionIdent, listedFunctions[0]);

    Function[] loadedFunctions = functionCatalog.getFunction(functionIdent);
    Assertions.assertEquals(1, loadedFunctions.length);
    Assertions.assertEquals(createdFunction.signature(), loadedFunctions[0].signature());

    Function[] versionedFunctions =
        functionCatalog.getFunction(functionIdent, createdFunction.version());
    Assertions.assertEquals(1, versionedFunctions.length);
    Assertions.assertEquals(createdFunction.signature(), versionedFunctions[0].signature());

    FunctionImpl additionalImpl =
        FunctionImpl.ofSql(
            FunctionImpl.RuntimeType.TRINO,
            "CREATE FUNCTION " + functionName + "(x INT) RETURNS INT RETURN x * 2");
    Function updatedFunction =
        functionCatalog.alterFunction(
            functionIdent,
            FunctionChange.updateComment("updated comment"),
            FunctionChange.addImplementation(additionalImpl));

    Assertions.assertEquals(createdFunction.version() + 1, updatedFunction.version());
    Assertions.assertEquals("updated comment", updatedFunction.comment());
    Assertions.assertEquals(2, updatedFunction.impls().length);

    Function[] historicalVersion =
        functionCatalog.getFunction(functionIdent, createdFunction.version());
    Assertions.assertEquals(1, historicalVersion.length);
    Assertions.assertEquals("initial comment", historicalVersion[0].comment());
    Assertions.assertEquals(1, historicalVersion[0].impls().length);

    FunctionSignature functionSignature =
        FunctionSignature.of(functionName, createdFunction.signature().functionParams());
    Assertions.assertTrue(functionCatalog.deleteFunction(functionIdent, functionSignature));
    Assertions.assertEquals(0, functionCatalog.listFunctions(functionNamespace).length);
    Assertions.assertThrows(
        NoSuchFunctionException.class, () -> functionCatalog.getFunction(functionIdent));
  }

  @Test
  public void testListFunctionInfos()
      throws NoSuchSchemaException, FunctionAlreadyExistsException, NoSuchFunctionException {
    FunctionCatalog functionCatalog = icebergCatalog.asFunctionCatalog();
    Namespace functionNamespace = Namespace.of(schema.name());
    String functionName = GravitinoITUtils.genRandomName("function_info");
    NameIdentifier functionIdent = NameIdentifier.of(schema.name(), functionName);

    FunctionImpl impl =
        FunctionImpl.ofSql(
            FunctionImpl.RuntimeType.SPARK,
            "CREATE FUNCTION " + functionName + "(x INT) RETURNS INT RETURN x + 1");

    functionCatalog.registerFunction(
        functionIdent,
        "info comment",
        FunctionType.SCALAR,
        true,
        new FunctionParam[] {FunctionParams.of("x", Types.IntegerType.get(), "input value")},
        Types.IntegerType.get(),
        new FunctionImpl[] {impl});

    Function[] functions = functionCatalog.listFunctionInfos(functionNamespace);
    Assertions.assertEquals(1, functions.length);
    Assertions.assertEquals(functionName, functions[0].signature().name());
    Assertions.assertEquals(FunctionImpl.RuntimeType.SPARK, functions[0].impls()[0].runtime());
  }
}
