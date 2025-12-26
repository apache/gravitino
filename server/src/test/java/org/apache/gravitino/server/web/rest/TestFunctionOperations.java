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
package org.apache.gravitino.server.web.rest;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.FunctionDispatcher;
import org.apache.gravitino.dto.function.FunctionDefinitionDTO;
import org.apache.gravitino.dto.function.FunctionImplDTO;
import org.apache.gravitino.dto.function.FunctionParamDTO;
import org.apache.gravitino.dto.function.SQLImplDTO;
import org.apache.gravitino.dto.requests.FunctionRegisterRequest;
import org.apache.gravitino.dto.requests.FunctionUpdateRequest;
import org.apache.gravitino.dto.requests.FunctionUpdatesRequest;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.EntityListResponse;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.FunctionListResponse;
import org.apache.gravitino.dto.responses.FunctionResponse;
import org.apache.gravitino.exceptions.FunctionAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchFunctionException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.function.Function;
import org.apache.gravitino.function.FunctionChange;
import org.apache.gravitino.function.FunctionColumn;
import org.apache.gravitino.function.FunctionDefinition;
import org.apache.gravitino.function.FunctionDefinitions;
import org.apache.gravitino.function.FunctionImpl;
import org.apache.gravitino.function.FunctionImpls;
import org.apache.gravitino.function.FunctionParam;
import org.apache.gravitino.function.FunctionParams;
import org.apache.gravitino.function.FunctionType;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestFunctionOperations extends BaseOperationsTest {

  private static class MockServletRequestFactory extends ServletRequestFactoryBase {

    @Override
    public HttpServletRequest get() {
      HttpServletRequest request = mock(HttpServletRequest.class);
      when(request.getRemoteUser()).thenReturn(null);
      return request;
    }
  }

  private final FunctionDispatcher functionDispatcher = mock(FunctionDispatcher.class);

  private final AuditInfo testAuditInfo =
      AuditInfo.builder().withCreator("user1").withCreateTime(Instant.now()).build();

  private final String metalake = "metalake_for_function_test";

  private final String catalog = "catalog_for_function_test";

  private final String schema = "schema_for_function_test";

  private final Namespace functionNs = NamespaceUtil.ofFunction(metalake, catalog, schema);

  @Override
  protected Application configure() {
    try {
      forceSet(
          TestProperties.CONTAINER_PORT, String.valueOf(RESTUtils.findAvailablePort(2000, 3000)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    ResourceConfig resourceConfig = new ResourceConfig();
    resourceConfig.register(FunctionOperations.class);
    resourceConfig.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bind(functionDispatcher).to(FunctionDispatcher.class).ranked(2);
            bindFactory(TestFunctionOperations.MockServletRequestFactory.class)
                .to(HttpServletRequest.class);
          }
        });

    return resourceConfig;
  }

  @Test
  public void testListFunctions() {
    NameIdentifier funcId1 = NameIdentifierUtil.ofFunction(metalake, catalog, schema, "func1");
    NameIdentifier funcId2 = NameIdentifierUtil.ofFunction(metalake, catalog, schema, "func2");
    NameIdentifier[] funcIds = new NameIdentifier[] {funcId1, funcId2};
    when(functionDispatcher.listFunctions(functionNs)).thenReturn(funcIds);

    Response response =
        target(functionPath())
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getMediaType());

    EntityListResponse resp = response.readEntity(EntityListResponse.class);
    Assertions.assertEquals(0, resp.getCode());
    Assertions.assertArrayEquals(funcIds, resp.identifiers());

    // test listFunctions with details=true
    Function mockFunction1 = mockFunction("func1", "comment1", FunctionType.SCALAR);
    Function mockFunction2 = mockFunction("func2", "comment2", FunctionType.SCALAR);
    Function[] functions = new Function[] {mockFunction1, mockFunction2};
    when(functionDispatcher.listFunctionInfos(functionNs)).thenReturn(functions);

    Response detailsResp =
        target(functionPath())
            .queryParam("details", true)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), detailsResp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, detailsResp.getMediaType());

    FunctionListResponse funcListResp = detailsResp.readEntity(FunctionListResponse.class);
    Assertions.assertEquals(0, funcListResp.getCode());
    Assertions.assertEquals(2, funcListResp.getFunctions().length);
    Assertions.assertEquals("func1", funcListResp.getFunctions()[0].name());
    Assertions.assertEquals("func2", funcListResp.getFunctions()[1].name());

    // Test mock return empty array for listFunctions
    when(functionDispatcher.listFunctions(functionNs)).thenReturn(new NameIdentifier[0]);
    Response resp3 =
        target(functionPath())
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp3.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp3.getMediaType());

    EntityListResponse resp4 = resp3.readEntity(EntityListResponse.class);
    Assertions.assertEquals(0, resp4.getCode());
    Assertions.assertEquals(0, resp4.identifiers().length);

    // Test mock throw NoSuchSchemaException
    doThrow(new NoSuchSchemaException("mock error"))
        .when(functionDispatcher)
        .listFunctions(functionNs);
    Response resp5 =
        target(functionPath())
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp5.getStatus());

    ErrorResponse errorResp = resp5.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResp.getCode());
    Assertions.assertEquals(NoSuchSchemaException.class.getSimpleName(), errorResp.getType());

    // Test mock throw RuntimeException
    doThrow(new RuntimeException("mock error")).when(functionDispatcher).listFunctions(functionNs);
    Response resp6 =
        target(functionPath())
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp6.getStatus());

    ErrorResponse errorResp1 = resp6.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp1.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp1.getType());
  }

  @Test
  public void testGetFunction() {
    Function mockFunction = mockFunction("func1", "test comment", FunctionType.SCALAR);
    NameIdentifier funcId = NameIdentifierUtil.ofFunction(metalake, catalog, schema, "func1");
    when(functionDispatcher.getFunction(funcId)).thenReturn(mockFunction);

    Response resp =
        target(functionPath())
            .path("func1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    FunctionResponse funcResp = resp.readEntity(FunctionResponse.class);
    Assertions.assertEquals(0, funcResp.getCode());

    Function resultFunction = funcResp.getFunction();
    compare(mockFunction, resultFunction);

    // Test mock throw NoSuchFunctionException
    doThrow(new NoSuchFunctionException("mock error")).when(functionDispatcher).getFunction(funcId);
    Response resp1 =
        target(functionPath())
            .path("func1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());

    ErrorResponse errorResp = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResp.getCode());
    Assertions.assertEquals(NoSuchFunctionException.class.getSimpleName(), errorResp.getType());

    // Test mock throw RuntimeException
    doThrow(new RuntimeException("mock error")).when(functionDispatcher).getFunction(funcId);
    Response resp2 =
        target(functionPath())
            .path("func1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResp1 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp1.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp1.getType());
  }

  @Test
  public void testRegisterScalarFunction() {
    NameIdentifier funcId = NameIdentifierUtil.ofFunction(metalake, catalog, schema, "func1");
    Function mockFunction = mockFunction("func1", "test comment", FunctionType.SCALAR);

    when(functionDispatcher.registerFunction(
            eq(funcId),
            anyString(),
            eq(FunctionType.SCALAR),
            anyBoolean(),
            any(Type.class),
            any(FunctionDefinition[].class)))
        .thenReturn(mockFunction);

    FunctionDefinitionDTO[] definitionDTOs = createMockDefinitionDTOs();
    FunctionRegisterRequest req =
        FunctionRegisterRequest.builder()
            .withName("func1")
            .withComment("test comment")
            .withFunctionType(FunctionType.SCALAR)
            .withDeterministic(true)
            .withReturnType(Types.IntegerType.get())
            .withDefinitions(definitionDTOs)
            .build();

    Response resp =
        target(functionPath())
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    FunctionResponse funcResp = resp.readEntity(FunctionResponse.class);
    Assertions.assertEquals(0, funcResp.getCode());
    compare(mockFunction, funcResp.getFunction());

    // Test mock throw NoSuchSchemaException
    doThrow(new NoSuchSchemaException("mock error"))
        .when(functionDispatcher)
        .registerFunction(
            eq(funcId),
            anyString(),
            eq(FunctionType.SCALAR),
            anyBoolean(),
            any(Type.class),
            any(FunctionDefinition[].class));

    Response resp1 =
        target(functionPath())
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());

    ErrorResponse errorResp = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResp.getCode());
    Assertions.assertEquals(NoSuchSchemaException.class.getSimpleName(), errorResp.getType());

    // Test mock throw FunctionAlreadyExistsException
    doThrow(new FunctionAlreadyExistsException("mock error"))
        .when(functionDispatcher)
        .registerFunction(
            eq(funcId),
            anyString(),
            eq(FunctionType.SCALAR),
            anyBoolean(),
            any(Type.class),
            any(FunctionDefinition[].class));

    Response resp2 =
        target(functionPath())
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.CONFLICT.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResp1 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ALREADY_EXISTS_CODE, errorResp1.getCode());
    Assertions.assertEquals(
        FunctionAlreadyExistsException.class.getSimpleName(), errorResp1.getType());

    // Test mock throw RuntimeException
    doThrow(new RuntimeException("mock error"))
        .when(functionDispatcher)
        .registerFunction(
            eq(funcId),
            anyString(),
            eq(FunctionType.SCALAR),
            anyBoolean(),
            any(Type.class),
            any(FunctionDefinition[].class));

    Response resp3 =
        target(functionPath())
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp3.getStatus());

    ErrorResponse errorResp2 = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp2.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp2.getType());
  }

  @Test
  public void testRegisterTableFunction() {
    NameIdentifier funcId = NameIdentifierUtil.ofFunction(metalake, catalog, schema, "tableFunc1");
    Function mockFunction = mockTableFunction("tableFunc1", "test comment");

    when(functionDispatcher.registerFunction(
            eq(funcId),
            anyString(),
            anyBoolean(),
            any(FunctionColumn[].class),
            any(FunctionDefinition[].class)))
        .thenReturn(mockFunction);

    FunctionRegisterRequest req =
        FunctionRegisterRequest.builder()
            .withName("tableFunc1")
            .withComment("test comment")
            .withFunctionType(FunctionType.TABLE)
            .withDeterministic(true)
            .withReturnColumns(createMockReturnColumnDTOs())
            .withDefinitions(createMockDefinitionDTOs())
            .build();

    Response resp =
        target(functionPath())
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    FunctionResponse funcResp = resp.readEntity(FunctionResponse.class);
    Assertions.assertEquals(0, funcResp.getCode());
    Assertions.assertEquals(FunctionType.TABLE, funcResp.getFunction().functionType());
  }

  @Test
  public void testAlterFunction() {
    NameIdentifier funcId = NameIdentifierUtil.ofFunction(metalake, catalog, schema, "func1");
    Function mockFunction = mockFunction("func1", "new comment", FunctionType.SCALAR);

    FunctionChange updateComment = FunctionChange.updateComment("new comment");
    when(functionDispatcher.alterFunction(funcId, updateComment)).thenReturn(mockFunction);

    FunctionUpdatesRequest req =
        new FunctionUpdatesRequest(
            Collections.singletonList(
                new FunctionUpdateRequest.UpdateCommentRequest("new comment")));

    Response resp =
        target(functionPath())
            .path("func1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    FunctionResponse funcResp = resp.readEntity(FunctionResponse.class);
    Assertions.assertEquals(0, funcResp.getCode());
    Assertions.assertEquals("new comment", funcResp.getFunction().comment());

    // Test mock throw NoSuchFunctionException
    doThrow(new NoSuchFunctionException("mock error"))
        .when(functionDispatcher)
        .alterFunction(funcId, updateComment);

    Response resp1 =
        target(functionPath())
            .path("func1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());

    ErrorResponse errorResp = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResp.getCode());
    Assertions.assertEquals(NoSuchFunctionException.class.getSimpleName(), errorResp.getType());

    // Test mock throw IllegalArgumentException
    doThrow(new IllegalArgumentException("mock error"))
        .when(functionDispatcher)
        .alterFunction(funcId, updateComment);

    Response resp2 =
        target(functionPath())
            .path("func1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResp1 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ILLEGAL_ARGUMENTS_CODE, errorResp1.getCode());

    // Test mock throw RuntimeException
    doThrow(new RuntimeException("mock error"))
        .when(functionDispatcher)
        .alterFunction(funcId, updateComment);

    Response resp3 =
        target(functionPath())
            .path("func1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp3.getStatus());

    ErrorResponse errorResp2 = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp2.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp2.getType());
  }

  @Test
  public void testAlterFunctionAddDefinition() {
    NameIdentifier funcId = NameIdentifierUtil.ofFunction(metalake, catalog, schema, "func1");
    Function mockFunction = mockFunction("func1", "comment", FunctionType.SCALAR);

    FunctionDefinitionDTO newDef = createMockDefinitionDTOs()[0];
    FunctionChange addDef = FunctionChange.addDefinition(newDef.toFunctionDefinition());
    when(functionDispatcher.alterFunction(funcId, addDef)).thenReturn(mockFunction);

    FunctionUpdatesRequest req =
        new FunctionUpdatesRequest(
            Collections.singletonList(new FunctionUpdateRequest.AddDefinitionRequest(newDef)));

    Response resp =
        target(functionPath())
            .path("func1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
  }

  @Test
  public void testAlterFunctionRemoveDefinition() {
    NameIdentifier funcId = NameIdentifierUtil.ofFunction(metalake, catalog, schema, "func1");
    Function mockFunction = mockFunction("func1", "comment", FunctionType.SCALAR);

    FunctionParamDTO[] params = createMockParamDTOs();
    FunctionParam[] functionParams = new FunctionParam[params.length];
    for (int i = 0; i < params.length; i++) {
      functionParams[i] = params[i].toFunctionParam();
    }
    FunctionChange removeDef = FunctionChange.removeDefinition(functionParams);
    when(functionDispatcher.alterFunction(funcId, removeDef)).thenReturn(mockFunction);

    FunctionUpdatesRequest req =
        new FunctionUpdatesRequest(
            Collections.singletonList(new FunctionUpdateRequest.RemoveDefinitionRequest(params)));

    Response resp =
        target(functionPath())
            .path("func1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
  }

  @Test
  public void testAlterFunctionAddImpl() {
    NameIdentifier funcId = NameIdentifierUtil.ofFunction(metalake, catalog, schema, "func1");
    Function mockFunction = mockFunction("func1", "comment", FunctionType.SCALAR);

    FunctionParamDTO[] params = createMockParamDTOs();
    FunctionParam[] functionParams = new FunctionParam[params.length];
    for (int i = 0; i < params.length; i++) {
      functionParams[i] = params[i].toFunctionParam();
    }
    FunctionImplDTO implDTO = createMockSqlImplDTO();
    FunctionImpl impl = implDTO.toFunctionImpl();
    FunctionChange addImpl = FunctionChange.addImpl(functionParams, impl);
    when(functionDispatcher.alterFunction(funcId, addImpl)).thenReturn(mockFunction);

    FunctionUpdatesRequest req =
        new FunctionUpdatesRequest(
            Collections.singletonList(new FunctionUpdateRequest.AddImplRequest(params, implDTO)));

    Response resp =
        target(functionPath())
            .path("func1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
  }

  @Test
  public void testAlterFunctionRemoveImpl() {
    NameIdentifier funcId = NameIdentifierUtil.ofFunction(metalake, catalog, schema, "func1");
    Function mockFunction = mockFunction("func1", "comment", FunctionType.SCALAR);

    FunctionParamDTO[] params = createMockParamDTOs();
    FunctionParam[] functionParams = new FunctionParam[params.length];
    for (int i = 0; i < params.length; i++) {
      functionParams[i] = params[i].toFunctionParam();
    }
    FunctionChange removeImpl =
        FunctionChange.removeImpl(functionParams, FunctionImpl.RuntimeType.SPARK);
    when(functionDispatcher.alterFunction(funcId, removeImpl)).thenReturn(mockFunction);

    FunctionUpdatesRequest req =
        new FunctionUpdatesRequest(
            Collections.singletonList(
                new FunctionUpdateRequest.RemoveImplRequest(params, "SPARK")));

    Response resp =
        target(functionPath())
            .path("func1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
  }

  @Test
  public void testDropFunction() {
    NameIdentifier funcId = NameIdentifierUtil.ofFunction(metalake, catalog, schema, "func1");
    when(functionDispatcher.dropFunction(funcId)).thenReturn(true);

    Response resp =
        target(functionPath())
            .path("func1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    DropResponse dropResp = resp.readEntity(DropResponse.class);
    Assertions.assertEquals(0, dropResp.getCode());
    Assertions.assertTrue(dropResp.dropped());

    // Test mock return false for dropFunction
    when(functionDispatcher.dropFunction(funcId)).thenReturn(false);
    Response resp1 =
        target(functionPath())
            .path("func1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp1.getMediaType());

    DropResponse dropResp1 = resp1.readEntity(DropResponse.class);
    Assertions.assertEquals(0, dropResp1.getCode());
    Assertions.assertFalse(dropResp1.dropped());

    // Test mock throw RuntimeException
    doThrow(new RuntimeException("mock error")).when(functionDispatcher).dropFunction(funcId);
    Response resp2 =
        target(functionPath())
            .path("func1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResp1 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp1.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp1.getType());
  }

  private String functionPath() {
    return "/metalakes/" + metalake + "/catalogs/" + catalog + "/schemas/" + schema + "/functions";
  }

  private Function mockFunction(String name, String comment, FunctionType functionType) {
    Function mockFunction = mock(Function.class);
    when(mockFunction.name()).thenReturn(name);
    when(mockFunction.comment()).thenReturn(comment);
    when(mockFunction.functionType()).thenReturn(functionType);
    when(mockFunction.deterministic()).thenReturn(true);
    when(mockFunction.returnType()).thenReturn(Types.IntegerType.get());
    when(mockFunction.returnColumns()).thenReturn(new FunctionColumn[0]);
    when(mockFunction.definitions()).thenReturn(createMockDefinitions());
    when(mockFunction.auditInfo()).thenReturn(testAuditInfo);
    return mockFunction;
  }

  private Function mockTableFunction(String name, String comment) {
    Function mockFunction = mock(Function.class);
    when(mockFunction.name()).thenReturn(name);
    when(mockFunction.comment()).thenReturn(comment);
    when(mockFunction.functionType()).thenReturn(FunctionType.TABLE);
    when(mockFunction.deterministic()).thenReturn(true);
    when(mockFunction.returnType()).thenReturn(null);
    when(mockFunction.returnColumns()).thenReturn(createMockReturnColumns());
    when(mockFunction.definitions()).thenReturn(createMockDefinitions());
    when(mockFunction.auditInfo()).thenReturn(testAuditInfo);
    return mockFunction;
  }

  private FunctionDefinition[] createMockDefinitions() {
    FunctionParam[] params =
        new FunctionParam[] {FunctionParams.of("param1", Types.IntegerType.get())};
    FunctionImpl[] impls =
        new FunctionImpl[] {
          FunctionImpls.ofSql(FunctionImpl.RuntimeType.SPARK, "SELECT param1 + 1")
        };
    return new FunctionDefinition[] {FunctionDefinitions.of(params, impls)};
  }

  private FunctionDefinitionDTO[] createMockDefinitionDTOs() {
    return new FunctionDefinitionDTO[] {
      FunctionDefinitionDTO.builder()
          .withParameters(createMockParamDTOs())
          .withImpls(new FunctionImplDTO[] {createMockSqlImplDTO()})
          .build()
    };
  }

  private FunctionParamDTO[] createMockParamDTOs() {
    return new FunctionParamDTO[] {
      FunctionParamDTO.builder().withName("param1").withDataType(Types.IntegerType.get()).build()
    };
  }

  private FunctionImplDTO createMockSqlImplDTO() {
    return new SQLImplDTO("SPARK", null, null, "SELECT param1 + 1");
  }

  private FunctionColumn[] createMockReturnColumns() {
    return new FunctionColumn[] {
      FunctionColumn.of("col1", Types.IntegerType.get(), "column comment")
    };
  }

  private org.apache.gravitino.dto.function.FunctionColumnDTO[] createMockReturnColumnDTOs() {
    return new org.apache.gravitino.dto.function.FunctionColumnDTO[] {
      org.apache.gravitino.dto.function.FunctionColumnDTO.builder()
          .withName("col1")
          .withDataType(Types.IntegerType.get())
          .withComment("column comment")
          .build()
    };
  }

  private void compare(Function left, Function right) {
    Assertions.assertEquals(left.name(), right.name());
    Assertions.assertEquals(left.comment(), right.comment());
    Assertions.assertEquals(left.functionType(), right.functionType());
    Assertions.assertEquals(left.deterministic(), right.deterministic());

    Assertions.assertNotNull(right.auditInfo());
    Assertions.assertEquals(left.auditInfo().creator(), right.auditInfo().creator());
    Assertions.assertEquals(left.auditInfo().createTime(), right.auditInfo().createTime());
  }
}
