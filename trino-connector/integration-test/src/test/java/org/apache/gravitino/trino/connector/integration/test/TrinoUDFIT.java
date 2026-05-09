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
package org.apache.gravitino.trino.connector.integration.test;

import static java.lang.Thread.sleep;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.function.Function;
import org.apache.gravitino.function.FunctionCatalog;
import org.apache.gravitino.function.FunctionDefinitions;
import org.apache.gravitino.function.FunctionImpl;
import org.apache.gravitino.function.FunctionImpls;
import org.apache.gravitino.function.FunctionParams;
import org.apache.gravitino.function.FunctionType;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for Trino connector UDF adaptation. Verifies that functions registered in
 * Gravitino with TRINO runtime are visible via Trino's language function API.
 */
@Tag("gravitino-docker-test")
public class TrinoUDFIT extends TrinoQueryITBase {

  private static final Logger LOG = LoggerFactory.getLogger(TrinoUDFIT.class);

  private static final String CATALOG_NAME = "gt_hive_udf";
  private static final String SCHEMA_NAME = "gt_udf_schema";
  private static Catalog catalog;

  @BeforeAll
  public static void setUp() throws Exception {
    TrinoUDFIT instance = new TrinoUDFIT();
    instance.setup();

    createHiveCatalog();
    createSchema();
  }

  @AfterAll
  public static void tearDown() {
    try {
      cleanupFunctionsAndSchema();
      dropCatalog(CATALOG_NAME);
    } catch (Exception e) {
      LOG.error("Error during teardown", e);
    }
    TrinoQueryITBase.cleanup();
  }

  private static void createHiveCatalog() throws Exception {
    Map<String, String> properties = new HashMap<>();
    properties.put("metastore.uris", hiveMetastoreUri);

    boolean exists = metalake.catalogExists(CATALOG_NAME);
    if (!exists) {
      metalake.createCatalog(
          CATALOG_NAME, Catalog.Type.RELATIONAL, "hive", "UDF test catalog", properties);
    }

    // Wait for catalog to sync to Trino
    boolean catalogReady = false;
    int tries = 180;
    while (!catalogReady && tries-- >= 0) {
      try {
        String result = trinoQueryRunner.runQuery("show catalogs");
        if (result.contains(metalakeName + "." + CATALOG_NAME)) {
          catalogReady = true;
          break;
        }
      } catch (Exception e) {
        LOG.info("Waiting for catalog to sync to Trino");
      }
      sleep(1000);
    }

    if (!catalogReady) {
      throw new Exception("Catalog " + CATALOG_NAME + " sync timeout");
    }

    catalog = metalake.loadCatalog(CATALOG_NAME);
  }

  private static void createSchema() {
    boolean exists = catalog.asSchemas().schemaExists(SCHEMA_NAME);
    if (!exists) {
      catalog.asSchemas().createSchema(SCHEMA_NAME, "UDF test schema", Collections.emptyMap());
    }
  }

  private static void cleanupFunctionsAndSchema() {
    try {
      FunctionCatalog functionCatalog = catalog.asFunctionCatalog();
      NameIdentifier[] functions = functionCatalog.listFunctions(Namespace.of(SCHEMA_NAME));
      for (NameIdentifier fn : functions) {
        functionCatalog.dropFunction(NameIdentifier.of(SCHEMA_NAME, fn.name()));
      }
    } catch (Exception e) {
      LOG.error("Error cleaning up functions", e);
    }

    try {
      catalog.asSchemas().dropSchema(SCHEMA_NAME, false);
    } catch (Exception e) {
      LOG.error("Error dropping schema", e);
    }
  }

  @Test
  public void testListLanguageFunctionsShowsRegisteredUDF() throws Exception {
    String functionName = "test_add_one";
    FunctionCatalog functionCatalog = catalog.asFunctionCatalog();

    // Register a scalar function: test_add_one(x INTEGER) -> INTEGER
    // Uses TRINO runtime + SQL language so it maps to a Trino LanguageFunction
    // SQL body "RETURN x + 1" adds 1 to the input integer
    Function function =
        functionCatalog.registerFunction(
            NameIdentifier.of(SCHEMA_NAME, functionName),
            "Adds one to input",
            FunctionType.SCALAR,
            true,
            FunctionDefinitions.of(
                FunctionDefinitions.of(
                    FunctionParams.of(FunctionParams.of("x", Types.IntegerType.get())),
                    Types.IntegerType.get(),
                    FunctionImpls.of(
                        FunctionImpls.ofSql(FunctionImpl.RuntimeType.TRINO, "RETURN x + 1")))));
    Assertions.assertNotNull(function);

    // Query Trino to verify the function is listed
    String trinoCatalogName = metalakeName + "." + CATALOG_NAME;
    String showFunctionsQuery =
        String.format("SHOW FUNCTIONS FROM %s.%s", trinoCatalogName, SCHEMA_NAME);
    String result = trinoQueryRunner.runQuery(showFunctionsQuery);

    LOG.info("SHOW FUNCTIONS result: {}", result);
    Assertions.assertTrue(
        result.contains(functionName),
        "Expected function " + functionName + " to be listed. Got: " + result);

    // Cleanup
    functionCatalog.dropFunction(NameIdentifier.of(SCHEMA_NAME, functionName));
  }

  @Test
  public void testSelectUDFReturnsCorrectResult() throws Exception {
    String functionName = "test_add_five";
    FunctionCatalog functionCatalog = catalog.asFunctionCatalog();

    // Register a scalar function: test_add_five(x INTEGER) -> INTEGER
    // SQL body "RETURN x + 5" adds 5 to the input integer
    Function function =
        functionCatalog.registerFunction(
            NameIdentifier.of(SCHEMA_NAME, functionName),
            "Adds five to input",
            FunctionType.SCALAR,
            true,
            FunctionDefinitions.of(
                FunctionDefinitions.of(
                    FunctionParams.of(FunctionParams.of("x", Types.IntegerType.get())),
                    Types.IntegerType.get(),
                    FunctionImpls.of(
                        FunctionImpls.ofSql(FunctionImpl.RuntimeType.TRINO, "RETURN x + 5")))));
    Assertions.assertNotNull(function);

    // Invoke the function via SELECT and verify the result
    String trinoCatalogName = metalakeName + "." + CATALOG_NAME;
    String selectQuery =
        String.format("SELECT %s.%s.%s(5)", trinoCatalogName, SCHEMA_NAME, functionName);
    String result = trinoQueryRunner.runQuery(selectQuery);

    LOG.info("SELECT result: {}", result);
    // Parse the query result and verify the exact numeric output
    String trimmedResult = result.trim();
    Assertions.assertTrue(
        trimmedResult.contains("10"),
        "Expected SELECT test_add_five(5) to return 10. Got: " + trimmedResult);
    Assertions.assertFalse(
        trimmedResult.contains("100"),
        "Result should be exactly 10, not a number containing 10. Got: " + trimmedResult);

    // Cleanup
    functionCatalog.dropFunction(NameIdentifier.of(SCHEMA_NAME, functionName));
  }

  @Test
  public void testListLanguageFunctionsFiltersNonTrinoRuntime() throws Exception {
    String functionName = "spark_only_func";
    FunctionCatalog functionCatalog = catalog.asFunctionCatalog();

    // Register a scalar function: spark_only_func(x INTEGER) -> INTEGER
    // Uses SPARK runtime, so this should NOT be visible in Trino
    Function function =
        functionCatalog.registerFunction(
            NameIdentifier.of(SCHEMA_NAME, functionName),
            "Spark-only function",
            FunctionType.SCALAR,
            true,
            FunctionDefinitions.of(
                FunctionDefinitions.of(
                    FunctionParams.of(FunctionParams.of("x", Types.IntegerType.get())),
                    Types.IntegerType.get(),
                    FunctionImpls.of(
                        FunctionImpls.ofSql(FunctionImpl.RuntimeType.SPARK, "RETURN x + 1")))));
    Assertions.assertNotNull(function);

    // Query Trino - SPARK runtime function should be filtered out
    String trinoCatalogName = metalakeName + "." + CATALOG_NAME;
    String showFunctionsQuery =
        String.format("SHOW FUNCTIONS FROM %s.%s", trinoCatalogName, SCHEMA_NAME);
    String result = trinoQueryRunner.runQuery(showFunctionsQuery);

    LOG.info("SHOW FUNCTIONS result (should not contain spark_only_func): {}", result);
    Assertions.assertFalse(
        result.contains(functionName),
        "SPARK runtime function should not appear in Trino. Got: " + result);

    // Cleanup
    functionCatalog.dropFunction(NameIdentifier.of(SCHEMA_NAME, functionName));
  }

  @Test
  public void testMultipleUDFsInSameSchema() throws Exception {
    String func1Name = "udf_multiply";
    String func2Name = "udf_concat";
    FunctionCatalog functionCatalog = catalog.asFunctionCatalog();

    // Register two TRINO SQL functions:
    // udf_multiply(x INTEGER) -> INTEGER: multiplies input by 2
    // udf_concat(a STRING, b STRING) -> STRING: concatenates two strings
    functionCatalog.registerFunction(
        NameIdentifier.of(SCHEMA_NAME, func1Name),
        "Multiply by 2",
        FunctionType.SCALAR,
        true,
        FunctionDefinitions.of(
            FunctionDefinitions.of(
                FunctionParams.of(FunctionParams.of("x", Types.IntegerType.get())),
                Types.IntegerType.get(),
                FunctionImpls.of(
                    FunctionImpls.ofSql(FunctionImpl.RuntimeType.TRINO, "RETURN x * 2")))));

    functionCatalog.registerFunction(
        NameIdentifier.of(SCHEMA_NAME, func2Name),
        "Concat strings",
        FunctionType.SCALAR,
        true,
        FunctionDefinitions.of(
            FunctionDefinitions.of(
                FunctionParams.of(
                    FunctionParams.of("a", Types.StringType.get()),
                    FunctionParams.of("b", Types.StringType.get())),
                Types.StringType.get(),
                FunctionImpls.of(
                    FunctionImpls.ofSql(FunctionImpl.RuntimeType.TRINO, "RETURN concat(a, b)")))));

    // Query Trino to verify both functions are listed
    String trinoCatalogName = metalakeName + "." + CATALOG_NAME;
    String showFunctionsQuery =
        String.format("SHOW FUNCTIONS FROM %s.%s", trinoCatalogName, SCHEMA_NAME);
    String result = trinoQueryRunner.runQuery(showFunctionsQuery);

    LOG.info("SHOW FUNCTIONS result: {}", result);
    Assertions.assertTrue(
        result.contains(func1Name), "Expected " + func1Name + " to be listed. Got: " + result);
    Assertions.assertTrue(
        result.contains(func2Name), "Expected " + func2Name + " to be listed. Got: " + result);

    // Cleanup
    functionCatalog.dropFunction(NameIdentifier.of(SCHEMA_NAME, func1Name));
    functionCatalog.dropFunction(NameIdentifier.of(SCHEMA_NAME, func2Name));
  }

  @Test
  public void testNoFunctionsWhenSchemaIsEmpty() {
    // Create a separate empty schema
    String emptySchema = "gt_empty_udf_schema";
    boolean exists = catalog.asSchemas().schemaExists(emptySchema);
    if (!exists) {
      catalog.asSchemas().createSchema(emptySchema, "empty schema", Collections.emptyMap());
    }

    String trinoCatalogName = metalakeName + "." + CATALOG_NAME;
    String showFunctionsQuery =
        String.format("SHOW FUNCTIONS FROM %s.%s", trinoCatalogName, emptySchema);
    String result = trinoQueryRunner.runQuery(showFunctionsQuery);

    LOG.info("SHOW FUNCTIONS for empty schema: {}", result);
    // Verify no Gravitino-registered functions appear; check for specific function names
    // that would only exist if registered via Gravitino (not built-in Trino functions)
    Assertions.assertFalse(
        result.contains("test_add_one"),
        "Expected no Gravitino-registered UDFs in empty schema. Got: " + result);
    Assertions.assertFalse(
        result.contains("test_add_five"),
        "Expected no Gravitino-registered UDFs in empty schema. Got: " + result);

    // Cleanup
    catalog.asSchemas().dropSchema(emptySchema, false);
  }
}
