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
package org.apache.gravitino.spark.connector.catalog;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.function.Function;
import org.apache.gravitino.function.FunctionCatalog;
import org.apache.gravitino.function.FunctionDefinition;
import org.apache.gravitino.function.FunctionImpl;
import org.apache.gravitino.function.FunctionImpls;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.gravitino.spark.connector.SparkTransformConverter;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.catalyst.analysis.NoSuchFunctionException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.functions.BoundFunction;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestBaseCatalogFunctions {

  private static final String SCHEMA = "db";
  private static final FunctionImpl SPARK_SQL =
      FunctionImpls.ofSql(FunctionImpl.RuntimeType.SPARK, "x + 1");
  private static final FunctionImpl SPARK_JAVA =
      FunctionImpls.ofJava(FunctionImpl.RuntimeType.SPARK, TestSparkFunction.class.getName());
  private static final FunctionImpl TRINO_SQL =
      FunctionImpls.ofSql(FunctionImpl.RuntimeType.TRINO, "x + 1");

  private FunctionCatalog functionCatalog;
  private BaseCatalog catalog;

  @BeforeAll
  static void initCatalogManager() {
    GravitinoClient gravitinoClient = mock(GravitinoClient.class);
    GravitinoCatalogManager.create(new SparkConf(false), "user", identity -> gravitinoClient);
  }

  @AfterAll
  static void cleanupCatalogManager() {
    GravitinoCatalogManager.get().close();
  }

  @BeforeEach
  void setUp() {
    Catalog gravitinoCatalog = mock(Catalog.class);
    functionCatalog = mock(FunctionCatalog.class);
    when(gravitinoCatalog.asFunctionCatalog()).thenReturn(functionCatalog);
    catalog = new FunctionTestCatalog();
    catalog.gravitinoCatalogClient = gravitinoCatalog;
  }

  @Test
  void testListFunctionsOnlyListsSparkJavaFunctions() throws Exception {
    Function[] functions =
        new Function[] {
          function("java_fn", SPARK_JAVA),
          function("sql_fn", SPARK_SQL),
          function("trino_fn", TRINO_SQL),
          function("mixed_fn", SPARK_SQL, SPARK_JAVA)
        };
    when(functionCatalog.listFunctionInfos(Namespace.of(SCHEMA))).thenReturn(functions);

    Identifier[] identifiers = catalog.listFunctions(new String[] {SCHEMA});

    assertArrayEquals(
        new Identifier[] {
          Identifier.of(new String[] {SCHEMA}, "java_fn"),
          Identifier.of(new String[] {SCHEMA}, "mixed_fn")
        },
        identifiers);
  }

  @Test
  void testLoadFunctionUsesJavaImplAfterNonJavaImpl() throws Exception {
    Function function = function("mixed_fn", SPARK_SQL, SPARK_JAVA);
    when(functionCatalog.getFunction(NameIdentifier.of(SCHEMA, "mixed_fn"))).thenReturn(function);

    UnboundFunction loaded = catalog.loadFunction(Identifier.of(new String[] {SCHEMA}, "mixed_fn"));

    assertEquals(TestSparkFunction.class, loaded.getClass());
  }

  @Test
  void testLoadFunctionWithoutSparkJavaImplThrowsNoSuchFunction() {
    Function function = function("sql_fn", SPARK_SQL);
    when(functionCatalog.getFunction(NameIdentifier.of(SCHEMA, "sql_fn"))).thenReturn(function);

    assertThrows(
        NoSuchFunctionException.class,
        () -> catalog.loadFunction(Identifier.of(new String[] {SCHEMA}, "sql_fn")));
  }

  private static Function function(String name, FunctionImpl... impls) {
    FunctionDefinition definition = mock(FunctionDefinition.class);
    when(definition.impls()).thenReturn(impls);
    Function function = mock(Function.class);
    when(function.name()).thenReturn(name);
    when(function.definitions()).thenReturn(new FunctionDefinition[] {definition});
    return function;
  }

  private static class FunctionTestCatalog extends BaseCatalog {
    @Override
    protected TableCatalog createAndInitSparkCatalog(
        String name, CaseInsensitiveStringMap options, Map<String, String> properties) {
      return null;
    }

    @Override
    protected Table createSparkTable(
        Identifier identifier,
        org.apache.gravitino.rel.Table gravitinoTable,
        Table sparkTable,
        TableCatalog sparkCatalog,
        PropertiesConverter propertiesConverter,
        SparkTransformConverter sparkTransformConverter,
        SparkTypeConverter sparkTypeConverter) {
      return null;
    }

    @Override
    protected PropertiesConverter getPropertiesConverter() {
      return null;
    }

    @Override
    protected SparkTransformConverter getSparkTransformConverter() {
      return null;
    }
  }

  public static class TestSparkFunction implements UnboundFunction {
    @Override
    public BoundFunction bind(StructType inputType) {
      return null;
    }

    @Override
    public String description() {
      return "test";
    }

    @Override
    public String name() {
      return "test";
    }
  }
}
