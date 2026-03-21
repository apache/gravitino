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
package org.apache.gravitino.trino.connector;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.LanguageFunction;
import io.trino.spi.function.SchemaFunctionName;
import java.util.Collection;
import java.util.List;
import org.apache.gravitino.Audit;
import org.apache.gravitino.function.Function;
import org.apache.gravitino.function.FunctionDefinition;
import org.apache.gravitino.function.FunctionImpl;
import org.apache.gravitino.function.FunctionImpls;
import org.apache.gravitino.function.FunctionParam;
import org.apache.gravitino.function.FunctionType;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorMetadata;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorMetadataAdapter;
import org.junit.jupiter.api.Test;

public class TestGravitinoMetadataFunction {

  @Test
  public void testListLanguageFunctionsReturnsTrinoSqlFunctions() {
    Function function =
        createMockFunction("my_func", "RETURN x + 1", FunctionImpl.RuntimeType.TRINO);
    CatalogConnectorMetadata catalogMetadata = mock(CatalogConnectorMetadata.class);
    when(catalogMetadata.supportsFunctions()).thenReturn(true);
    when(catalogMetadata.listFunctionInfos("test_schema")).thenReturn(new Function[] {function});

    GravitinoMetadata metadata = createTestMetadata(catalogMetadata);
    ConnectorSession session = mock(ConnectorSession.class);

    Collection<LanguageFunction> functions = metadata.listLanguageFunctions(session, "test_schema");
    assertEquals(1, functions.size());

    LanguageFunction langFunc = functions.iterator().next();
    assertEquals("RETURN x + 1", langFunc.sql());
    assertEquals("my_func(integer)", langFunc.signatureToken());
  }

  @Test
  public void testGetLanguageFunctionsReturnsTrinoSqlFunctions() {
    Function function =
        createMockFunction("my_func", "RETURN x + 1", FunctionImpl.RuntimeType.TRINO);
    CatalogConnectorMetadata catalogMetadata = mock(CatalogConnectorMetadata.class);
    when(catalogMetadata.supportsFunctions()).thenReturn(true);
    when(catalogMetadata.getFunction("test_schema", "my_func")).thenReturn(function);

    GravitinoMetadata metadata = createTestMetadata(catalogMetadata);
    ConnectorSession session = mock(ConnectorSession.class);

    Collection<LanguageFunction> functions =
        metadata.getLanguageFunctions(session, new SchemaFunctionName("test_schema", "my_func"));
    assertEquals(1, functions.size());

    LanguageFunction langFunc = functions.iterator().next();
    assertEquals("RETURN x + 1", langFunc.sql());
  }

  @Test
  public void testListLanguageFunctionsFiltersNonTrinoRuntime() {
    Function sparkFunction =
        createMockFunction("spark_func", "RETURN 1", FunctionImpl.RuntimeType.SPARK);
    Function trinoFunction =
        createMockFunction("trino_func", "RETURN 2", FunctionImpl.RuntimeType.TRINO);

    CatalogConnectorMetadata catalogMetadata = mock(CatalogConnectorMetadata.class);
    when(catalogMetadata.supportsFunctions()).thenReturn(true);
    when(catalogMetadata.listFunctionInfos("test_schema"))
        .thenReturn(new Function[] {sparkFunction, trinoFunction});

    GravitinoMetadata metadata = createTestMetadata(catalogMetadata);
    ConnectorSession session = mock(ConnectorSession.class);

    Collection<LanguageFunction> functions = metadata.listLanguageFunctions(session, "test_schema");
    assertEquals(1, functions.size());
    assertEquals("RETURN 2", functions.iterator().next().sql());
  }

  @Test
  public void testListLanguageFunctionsWhenUnsupported() {
    CatalogConnectorMetadata catalogMetadata = mock(CatalogConnectorMetadata.class);
    when(catalogMetadata.supportsFunctions()).thenReturn(false);

    GravitinoMetadata metadata = createTestMetadata(catalogMetadata);
    ConnectorSession session = mock(ConnectorSession.class);

    Collection<LanguageFunction> functions = metadata.listLanguageFunctions(session, "test_schema");
    assertTrue(functions.isEmpty());
  }

  @Test
  public void testGetLanguageFunctionsWhenFunctionNotFound() {
    CatalogConnectorMetadata catalogMetadata = mock(CatalogConnectorMetadata.class);
    when(catalogMetadata.supportsFunctions()).thenReturn(true);
    when(catalogMetadata.getFunction("test_schema", "no_such_func")).thenReturn(null);

    GravitinoMetadata metadata = createTestMetadata(catalogMetadata);
    ConnectorSession session = mock(ConnectorSession.class);

    Collection<LanguageFunction> functions =
        metadata.getLanguageFunctions(
            session, new SchemaFunctionName("test_schema", "no_such_func"));
    assertTrue(functions.isEmpty());
  }

  @Test
  public void testMultipleDefinitionsAndImpls() {
    FunctionParam param1 = createMockParam("x", Types.IntegerType.get());
    FunctionParam param2 = createMockParam("x", Types.StringType.get());

    FunctionImpl trinoSqlImpl1 =
        FunctionImpls.ofSql(FunctionImpl.RuntimeType.TRINO, "RETURN x + 1");
    FunctionImpl trinoSqlImpl2 =
        FunctionImpls.ofSql(FunctionImpl.RuntimeType.TRINO, "RETURN length(x)");
    FunctionImpl sparkImpl = FunctionImpls.ofSql(FunctionImpl.RuntimeType.SPARK, "RETURN x * 2");

    FunctionDefinition def1 = createMockDefinition(new FunctionParam[] {param1}, trinoSqlImpl1);
    FunctionDefinition def2 =
        createMockDefinition(new FunctionParam[] {param2}, trinoSqlImpl2, sparkImpl);

    Function function = createMockFunctionWithDefinitions("multi_func", def1, def2);

    CatalogConnectorMetadata catalogMetadata = mock(CatalogConnectorMetadata.class);
    when(catalogMetadata.supportsFunctions()).thenReturn(true);
    when(catalogMetadata.listFunctionInfos("test_schema")).thenReturn(new Function[] {function});

    GravitinoMetadata metadata = createTestMetadata(catalogMetadata);
    ConnectorSession session = mock(ConnectorSession.class);

    Collection<LanguageFunction> functions = metadata.listLanguageFunctions(session, "test_schema");
    // Should have 2 Trino SQL impls (one from def1, one from def2; sparkImpl filtered out)
    assertEquals(2, functions.size());

    List<String> sqlBodies = functions.stream().map(LanguageFunction::sql).sorted().toList();
    assertEquals("RETURN length(x)", sqlBodies.get(0));
    assertEquals("RETURN x + 1", sqlBodies.get(1));
  }

  @Test
  public void testSignatureTokenFormat() {
    FunctionParam param1 = createMockParam("a", Types.IntegerType.get());
    FunctionParam param2 = createMockParam("b", Types.StringType.get());

    FunctionImpl impl = FunctionImpls.ofSql(FunctionImpl.RuntimeType.TRINO, "RETURN a");
    FunctionDefinition def = createMockDefinition(new FunctionParam[] {param1, param2}, impl);
    Function function = createMockFunctionWithDefinitions("test_func", def);

    CatalogConnectorMetadata catalogMetadata = mock(CatalogConnectorMetadata.class);
    when(catalogMetadata.supportsFunctions()).thenReturn(true);
    when(catalogMetadata.getFunction("s", "test_func")).thenReturn(function);

    GravitinoMetadata metadata = createTestMetadata(catalogMetadata);
    ConnectorSession session = mock(ConnectorSession.class);

    Collection<LanguageFunction> functions =
        metadata.getLanguageFunctions(session, new SchemaFunctionName("s", "test_func"));
    assertEquals(1, functions.size());
    assertEquals("test_func(integer,string)", functions.iterator().next().signatureToken());
  }

  @Test
  public void testNoArgsFunction() {
    FunctionImpl impl = FunctionImpls.ofSql(FunctionImpl.RuntimeType.TRINO, "RETURN 42");
    FunctionDefinition def = createMockDefinition(new FunctionParam[] {}, impl);
    Function function = createMockFunctionWithDefinitions("const_func", def);

    CatalogConnectorMetadata catalogMetadata = mock(CatalogConnectorMetadata.class);
    when(catalogMetadata.supportsFunctions()).thenReturn(true);
    when(catalogMetadata.getFunction("s", "const_func")).thenReturn(function);

    GravitinoMetadata metadata = createTestMetadata(catalogMetadata);
    ConnectorSession session = mock(ConnectorSession.class);

    Collection<LanguageFunction> functions =
        metadata.getLanguageFunctions(session, new SchemaFunctionName("s", "const_func"));
    assertEquals(1, functions.size());
    LanguageFunction lf = functions.iterator().next();
    assertEquals("const_func()", lf.signatureToken());
    assertEquals("RETURN 42", lf.sql());
  }

  private GravitinoMetadata createTestMetadata(CatalogConnectorMetadata catalogMetadata) {
    CatalogConnectorMetadataAdapter metadataAdapter = mock(CatalogConnectorMetadataAdapter.class);
    ConnectorMetadata internalMetadata = mock(ConnectorMetadata.class);
    // Use a concrete anonymous subclass since GravitinoMetadata is abstract
    return new GravitinoMetadata(catalogMetadata, metadataAdapter, internalMetadata) {
      // No need to implement abstract methods for these tests since they are version-specific
    };
  }

  private Function createMockFunction(String name, String sql, FunctionImpl.RuntimeType runtime) {
    FunctionParam param = createMockParam("x", Types.IntegerType.get());
    FunctionImpl impl = FunctionImpls.ofSql(runtime, sql);
    FunctionDefinition definition = createMockDefinition(new FunctionParam[] {param}, impl);
    return createMockFunctionWithDefinitions(name, definition);
  }

  private Function createMockFunctionWithDefinitions(
      String name, FunctionDefinition... definitions) {
    Function function = mock(Function.class);
    when(function.name()).thenReturn(name);
    when(function.functionType()).thenReturn(FunctionType.SCALAR);
    when(function.deterministic()).thenReturn(true);
    when(function.definitions()).thenReturn(definitions);
    Audit audit = mock(Audit.class);
    when(function.auditInfo()).thenReturn(audit);
    return function;
  }

  private FunctionDefinition createMockDefinition(FunctionParam[] params, FunctionImpl... impls) {
    FunctionDefinition definition = mock(FunctionDefinition.class);
    when(definition.parameters()).thenReturn(params);
    when(definition.impls()).thenReturn(impls);
    return definition;
  }

  private FunctionParam createMockParam(String name, org.apache.gravitino.rel.types.Type type) {
    FunctionParam param = mock(FunctionParam.class);
    when(param.name()).thenReturn(name);
    when(param.dataType()).thenReturn(type);
    return param;
  }
}
