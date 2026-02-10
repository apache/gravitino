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
package org.apache.gravitino.client;

import static org.apache.hc.core5.http.HttpStatus.SC_CONFLICT;
import static org.apache.hc.core5.http.HttpStatus.SC_NOT_FOUND;
import static org.apache.hc.core5.http.HttpStatus.SC_OK;
import static org.apache.hc.core5.http.HttpStatus.SC_SERVER_ERROR;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.CatalogDTO;
import org.apache.gravitino.dto.function.FunctionDTO;
import org.apache.gravitino.dto.function.FunctionDefinitionDTO;
import org.apache.gravitino.dto.function.FunctionImplDTO;
import org.apache.gravitino.dto.function.FunctionParamDTO;
import org.apache.gravitino.dto.function.SQLImplDTO;
import org.apache.gravitino.dto.requests.CatalogCreateRequest;
import org.apache.gravitino.dto.responses.CatalogResponse;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.EntityListResponse;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.FunctionResponse;
import org.apache.gravitino.exceptions.FunctionAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchFunctionException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.function.Function;
import org.apache.gravitino.function.FunctionCatalog;
import org.apache.gravitino.function.FunctionDefinition;
import org.apache.gravitino.function.FunctionDefinitions;
import org.apache.gravitino.function.FunctionImpl;
import org.apache.gravitino.function.FunctionImpls;
import org.apache.gravitino.function.FunctionParam;
import org.apache.gravitino.function.FunctionType;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestFunctionCatalog extends TestBase {

  protected static Catalog catalog;

  private static GravitinoMetalake metalake;

  protected static final String metalakeName = "testMetalake";

  protected static final String catalogName = "testCatalog";

  private static final String provider = "test";

  @BeforeAll
  public static void setUp() throws Exception {
    TestBase.setUp();

    metalake = TestGravitinoMetalake.createMetalake(client, metalakeName);

    CatalogDTO mockCatalog =
        CatalogDTO.builder()
            .withName(catalogName)
            .withType(CatalogDTO.Type.RELATIONAL)
            .withProvider(provider)
            .withComment("comment")
            .withProperties(ImmutableMap.of("k1", "k2"))
            .withAudit(
                AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();

    CatalogCreateRequest catalogCreateRequest =
        new CatalogCreateRequest(
            catalogName,
            CatalogDTO.Type.RELATIONAL,
            provider,
            "comment",
            ImmutableMap.of("k1", "k2"));
    CatalogResponse catalogResponse = new CatalogResponse(mockCatalog);
    buildMockResource(
        Method.POST,
        "/api/metalakes/" + metalakeName + "/catalogs",
        catalogCreateRequest,
        catalogResponse,
        SC_OK);

    catalog =
        metalake.createCatalog(
            catalogName,
            CatalogDTO.Type.RELATIONAL,
            provider,
            "comment",
            ImmutableMap.of("k1", "k2"));
  }

  @Test
  public void testListFunctions() throws JsonProcessingException {
    NameIdentifier func1 = NameIdentifier.of("schema1", "func1");
    NameIdentifier func2 = NameIdentifier.of("schema1", "func2");
    NameIdentifier expectedResultFunc1 =
        NameIdentifier.of(metalakeName, catalogName, "schema1", "func1");
    NameIdentifier expectedResultFunc2 =
        NameIdentifier.of(metalakeName, catalogName, "schema1", "func2");
    String functionPath =
        withSlash(formatFunctionRequestPath(Namespace.of(metalakeName, catalogName, "schema1")));

    EntityListResponse resp =
        new EntityListResponse(new NameIdentifier[] {expectedResultFunc1, expectedResultFunc2});
    buildMockResource(Method.GET, functionPath, null, resp, SC_OK);
    NameIdentifier[] functions = catalog.asFunctionCatalog().listFunctions(func1.namespace());

    Assertions.assertEquals(2, functions.length);
    Assertions.assertEquals(func1, functions[0]);
    Assertions.assertEquals(func2, functions[1]);

    // Throw schema not found exception
    ErrorResponse errResp =
        ErrorResponse.notFound(NoSuchSchemaException.class.getSimpleName(), "schema not found");
    buildMockResource(Method.GET, functionPath, null, errResp, SC_NOT_FOUND);
    Assertions.assertThrows(
        NoSuchSchemaException.class,
        () -> catalog.asFunctionCatalog().listFunctions(func1.namespace()),
        "schema not found");

    // Throw Runtime exception
    ErrorResponse errResp2 = ErrorResponse.internalError("internal error");
    buildMockResource(Method.GET, functionPath, null, errResp2, SC_SERVER_ERROR);
    Assertions.assertThrows(
        RuntimeException.class,
        () -> catalog.asFunctionCatalog().listFunctions(func1.namespace()),
        "internal error");
  }

  @Test
  public void testGetFunction() throws JsonProcessingException {
    NameIdentifier func = NameIdentifier.of("schema1", "func1");
    String functionPath =
        withSlash(
            formatFunctionRequestPath(Namespace.of(metalakeName, catalogName, "schema1"))
                + "/func1");

    FunctionDTO mockFunction =
        mockFunctionDTO(func.name(), FunctionType.SCALAR, "mock comment", true);
    FunctionResponse resp = new FunctionResponse(mockFunction);
    buildMockResource(Method.GET, functionPath, null, resp, SC_OK);
    Function loadedFunction = catalog.asFunctionCatalog().getFunction(func);
    Assertions.assertNotNull(loadedFunction);
    assertFunction(mockFunction, loadedFunction);

    // Throw schema not found exception
    ErrorResponse errResp =
        ErrorResponse.notFound(NoSuchSchemaException.class.getSimpleName(), "schema not found");
    buildMockResource(Method.GET, functionPath, null, errResp, SC_NOT_FOUND);
    Assertions.assertThrows(
        NoSuchSchemaException.class,
        () -> catalog.asFunctionCatalog().getFunction(func),
        "schema not found");

    // Throw function not found exception
    ErrorResponse errResp1 =
        ErrorResponse.notFound(NoSuchFunctionException.class.getSimpleName(), "function not found");
    buildMockResource(Method.GET, functionPath, null, errResp1, SC_NOT_FOUND);
    Assertions.assertThrows(
        NoSuchFunctionException.class,
        () -> catalog.asFunctionCatalog().getFunction(func),
        "function not found");
  }

  @Test
  public void testRegisterFunction() throws JsonProcessingException {
    NameIdentifier func = NameIdentifier.of("schema1", "func1");
    String functionPath =
        withSlash(formatFunctionRequestPath(Namespace.of(metalakeName, catalogName, "schema1")));

    FunctionDTO mockFunction =
        mockFunctionDTO(func.name(), FunctionType.SCALAR, "mock comment", true);

    FunctionResponse resp = new FunctionResponse(mockFunction);
    // Use null for request body to match any request body
    buildMockResource(Method.POST, functionPath, null, resp, SC_OK);

    Function registeredFunction =
        catalog
            .asFunctionCatalog()
            .registerFunction(
                func,
                "mock comment",
                FunctionType.SCALAR,
                true,
                FunctionDefinitions.of(mockDefinition(Types.StringType.get())));

    Assertions.assertNotNull(registeredFunction);
    assertFunction(mockFunction, registeredFunction);

    // Throw function already exists exception
    ErrorResponse errResp =
        ErrorResponse.alreadyExists(
            FunctionAlreadyExistsException.class.getSimpleName(), "function already exists");
    buildMockResource(Method.POST, functionPath, null, errResp, SC_CONFLICT);
    Assertions.assertThrows(
        FunctionAlreadyExistsException.class,
        () ->
            catalog
                .asFunctionCatalog()
                .registerFunction(
                    func,
                    "mock comment",
                    FunctionType.SCALAR,
                    true,
                    FunctionDefinitions.of(mockDefinition(Types.StringType.get()))),
        "function already exists");
  }

  @Test
  public void testAlterFunction() throws JsonProcessingException {
    NameIdentifier func = NameIdentifier.of("schema1", "func1");
    String functionPath =
        withSlash(
            formatFunctionRequestPath(Namespace.of(metalakeName, catalogName, "schema1"))
                + "/func1");

    FunctionDTO mockFunction =
        mockFunctionDTO(func.name(), FunctionType.SCALAR, "updated comment", true);

    FunctionResponse resp = new FunctionResponse(mockFunction);
    // Use null for request body to match any request body
    buildMockResource(Method.PUT, functionPath, null, resp, SC_OK);

    FunctionCatalog functionCatalog = catalog.asFunctionCatalog();
    Function alteredFunction =
        functionCatalog.alterFunction(
            func, org.apache.gravitino.function.FunctionChange.updateComment("updated comment"));

    Assertions.assertNotNull(alteredFunction);
    Assertions.assertEquals("updated comment", alteredFunction.comment());

    // Throw function not found exception
    ErrorResponse errResp =
        ErrorResponse.notFound(NoSuchFunctionException.class.getSimpleName(), "function not found");
    buildMockResource(Method.PUT, functionPath, null, errResp, SC_NOT_FOUND);
    Assertions.assertThrows(
        NoSuchFunctionException.class,
        () ->
            functionCatalog.alterFunction(
                func,
                org.apache.gravitino.function.FunctionChange.updateComment("updated comment")),
        "function not found");
  }

  @Test
  public void testDropFunction() throws JsonProcessingException {
    NameIdentifier func = NameIdentifier.of("schema1", "func1");
    String functionPath =
        withSlash(
            formatFunctionRequestPath(Namespace.of(metalakeName, catalogName, "schema1"))
                + "/func1");

    DropResponse resp = new DropResponse(true);
    buildMockResource(Method.DELETE, functionPath, null, resp, SC_OK);
    Assertions.assertTrue(catalog.asFunctionCatalog().dropFunction(func));

    // Return false when the function does not exist
    DropResponse resp1 = new DropResponse(false);
    buildMockResource(Method.DELETE, functionPath, null, resp1, SC_OK);
    Assertions.assertFalse(catalog.asFunctionCatalog().dropFunction(func));
  }

  private static FunctionDTO mockFunctionDTO(
      String name, FunctionType functionType, String comment, boolean deterministic) {
    FunctionDefinitionDTO definition = mockDefinitionDTO(Types.StringType.get());

    return FunctionDTO.builder()
        .withName(name)
        .withFunctionType(functionType)
        .withComment(comment)
        .withDeterministic(deterministic)
        .withDefinitions(new FunctionDefinitionDTO[] {definition})
        .withAudit(AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
        .build();
  }

  private static FunctionDefinitionDTO mockDefinitionDTO(Type returnType) {
    FunctionParamDTO[] params =
        new FunctionParamDTO[] {
          FunctionParamDTO.builder()
              .withName("param1")
              .withDataType(Types.IntegerType.get())
              .build()
        };

    SQLImplDTO impl =
        new SQLImplDTO(FunctionImpl.RuntimeType.SPARK.name(), null, null, "SELECT param1 + 1");

    return FunctionDefinitionDTO.builder()
        .withParameters(params)
        .withReturnType(returnType)
        .withImpls(new FunctionImplDTO[] {impl})
        .build();
  }

  private static FunctionDefinition mockDefinition(Type returnType) {
    FunctionParam[] params =
        new FunctionParam[] {
          FunctionParamDTO.builder()
              .withName("param1")
              .withDataType(Types.IntegerType.get())
              .build()
        };

    FunctionImpl[] impls =
        new FunctionImpl[] {
          FunctionImpls.ofSql(FunctionImpl.RuntimeType.SPARK, "SELECT param1 + 1")
        };

    return FunctionDefinitions.of(params, returnType, impls);
  }

  private static void assertFunction(FunctionDTO expected, Function actual) {
    Assertions.assertEquals(expected.name(), actual.name());
    Assertions.assertEquals(expected.functionType(), actual.functionType());
    Assertions.assertEquals(expected.comment(), actual.comment());
    Assertions.assertEquals(expected.deterministic(), actual.deterministic());
    Assertions.assertEquals(expected.definitions().length, actual.definitions().length);
    Assertions.assertEquals(
        expected.definitions()[0].returnType(), actual.definitions()[0].returnType());
  }

  private static String formatFunctionRequestPath(Namespace ns) {
    Namespace schemaNs = Namespace.of(ns.level(0), ns.level(1));
    return BaseSchemaCatalog.formatSchemaRequestPath(schemaNs) + "/" + ns.level(2) + "/functions";
  }
}
