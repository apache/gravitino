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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.SemanticModelDispatcher;
import org.apache.gravitino.dto.requests.SemanticModelCreateRequest;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.SemanticModelResponse;
import org.apache.gravitino.dto.semantic.DatasetDTO;
import org.apache.gravitino.dto.semantic.SemanticModelDefinitionDTO;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.IllegalSemanticModelException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchSemanticModelException;
import org.apache.gravitino.exceptions.SemanticModelAlreadyExistsException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.SemanticModelEntity;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.semantic.AIContext;
import org.apache.gravitino.semantic.CustomExtension;
import org.apache.gravitino.semantic.DataType;
import org.apache.gravitino.semantic.Dataset;
import org.apache.gravitino.semantic.DialectExpression;
import org.apache.gravitino.semantic.Dialects;
import org.apache.gravitino.semantic.Expression;
import org.apache.gravitino.semantic.Field;
import org.apache.gravitino.semantic.Metric;
import org.apache.gravitino.semantic.SemanticModel;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/** Tests the Semantic Model create/load REST resource and its error mappings. */
public class TestSemanticModelOperations extends BaseOperationsTest {

  private static final String VND_V1_JSON = "application/vnd.gravitino.v1+json";

  private static class MockServletRequestFactory extends ServletRequestFactoryBase {
    @Override
    public HttpServletRequest get() {
      return mock(HttpServletRequest.class);
    }
  }

  private final SemanticModelDispatcher dispatcher = mock(SemanticModelDispatcher.class);
  private final String metalake = "semantic_model_metalake";
  private final String catalog = "semantic_model_catalog";
  private final String schema = "semantic_model_schema";
  private final Namespace namespace = NamespaceUtil.ofSemanticModel(metalake, catalog, schema);

  /** {@inheritDoc} */
  @Override
  protected Application configure() {
    try {
      forceSet(
          TestProperties.CONTAINER_PORT, String.valueOf(RESTUtils.findAvailablePort(2000, 3000)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    ResourceConfig resourceConfig = new ResourceConfig();
    resourceConfig.register(SemanticModelOperations.class);
    resourceConfig.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bind(dispatcher).to(SemanticModelDispatcher.class).ranked(2);
            bindFactory(MockServletRequestFactory.class).to(HttpServletRequest.class);
          }
        });
    return resourceConfig;
  }

  @BeforeEach
  void resetDispatcher() {
    reset(dispatcher);
  }

  @Test
  void testLoadSemanticModelReturnsCompleteDefinitionWithoutWrites() {
    NameIdentifier ident = semanticModelIdentifier("sales");
    SemanticModel semanticModel = semanticModel("sales", "Sales definitions");
    when(dispatcher.loadSemanticModel(ident)).thenReturn(semanticModel);

    Response response = get(semanticModelPath() + "/sales");

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    SemanticModelResponse body = response.readEntity(SemanticModelResponse.class);
    body.validate();
    Assertions.assertEquals("sales", body.getSemanticModel().name());
    Assertions.assertEquals("Sales definitions", body.getSemanticModel().comment());
    Assertions.assertEquals(semanticModel.definition(), body.getSemanticModel().definition());
    Assertions.assertEquals("orders", body.getSemanticModel().definition().datasets()[0].name());
    Assertions.assertEquals(
        "order_total", body.getSemanticModel().definition().metrics()[0].name());
    Assertions.assertEquals(
        "acme", body.getSemanticModel().definition().customExtensions()[0].vendorName());
    Assertions.assertEquals(Map.of("domain", "sales"), body.getSemanticModel().properties());
    Assertions.assertEquals("tester", body.getSemanticModel().auditInfo().creator());
    verify(dispatcher).loadSemanticModel(ident);
    verifyNoMoreInteractions(dispatcher);
  }

  @Test
  void testLoadSemanticModelNotFound() {
    NameIdentifier ident = semanticModelIdentifier("sales");
    doThrow(new NoSuchSemanticModelException("sales does not exist"))
        .when(dispatcher)
        .loadSemanticModel(ident);

    assertError(
        get(semanticModelPath() + "/sales"),
        Response.Status.NOT_FOUND,
        ErrorConstants.NOT_FOUND_CODE,
        NoSuchSemanticModelException.class.getSimpleName(),
        "sales does not exist");
  }

  @Test
  void testCreateSemanticModelConvertsStructuredDefinition() {
    NameIdentifier ident = semanticModelIdentifier("sales");
    SemanticModelDefinition definition = semanticModelDefinition("TRINO");
    SemanticModelCreateRequest request = createRequest("sales", "Sales definitions", definition);
    SemanticModel semanticModel = semanticModel("sales", "Sales definitions");
    when(dispatcher.createSemanticModel(
            eq(ident),
            eq("Sales definitions"),
            any(SemanticModelDefinition.class),
            eq(Map.of("domain", "sales"))))
        .thenReturn(semanticModel);

    Response response = post(semanticModelPath(), request);

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    SemanticModelResponse body = response.readEntity(SemanticModelResponse.class);
    body.validate();
    Assertions.assertEquals("sales", body.getSemanticModel().name());
    Assertions.assertEquals(semanticModel.definition(), body.getSemanticModel().definition());

    ArgumentCaptor<SemanticModelDefinition> definitionCaptor =
        ArgumentCaptor.forClass(SemanticModelDefinition.class);
    verify(dispatcher)
        .createSemanticModel(
            eq(ident),
            eq("Sales definitions"),
            definitionCaptor.capture(),
            eq(Map.of("domain", "sales")));
    Assertions.assertEquals(definition, definitionCaptor.getValue());
    Assertions.assertEquals(
        "TRINO", definitionCaptor.getValue().metrics()[0].expression().dialects()[0].dialect());
    Assertions.assertEquals(DataType.DECIMAL, definitionCaptor.getValue().metrics()[0].datatype());
    Assertions.assertEquals(
        NameIdentifier.of(catalog, schema, "orders"),
        definitionCaptor.getValue().datasets()[0].source());
  }

  @Test
  void testCreateSemanticModelErrors() {
    SemanticModelDefinition definition = semanticModelDefinition();
    SemanticModelCreateRequest request = createRequest("sales", "Sales definitions", definition);
    NameIdentifier ident = semanticModelIdentifier("sales");

    SemanticModelCreateRequest invalidRequest =
        new SemanticModelCreateRequest("sales", null, null, Map.of());
    assertError(
        post(semanticModelPath(), invalidRequest),
        Response.Status.BAD_REQUEST,
        ErrorConstants.ILLEGAL_ARGUMENTS_CODE,
        IllegalArgumentException.class.getSimpleName(),
        "definition");

    SemanticModelCreateRequest nullDatasetRequest =
        new SemanticModelCreateRequest(
            "sales",
            null,
            SemanticModelDefinitionDTO.builder().withDatasets(new DatasetDTO[] {null}).build(),
            Map.of());
    assertError(
        post(semanticModelPath(), nullDatasetRequest),
        Response.Status.BAD_REQUEST,
        ErrorConstants.ILLEGAL_ARGUMENTS_CODE,
        IllegalArgumentException.class.getSimpleName(),
        "datasets[0] must not be null");

    SemanticModelCreateRequest emptyDatasetsRequest =
        new SemanticModelCreateRequest(
            "sales",
            null,
            SemanticModelDefinitionDTO.builder().withDatasets(new DatasetDTO[0]).build(),
            Map.of());
    assertError(
        post(semanticModelPath(), emptyDatasetsRequest),
        Response.Status.BAD_REQUEST,
        ErrorConstants.ILLEGAL_ARGUMENTS_CODE,
        IllegalArgumentException.class.getSimpleName(),
        "datasets must not be null or empty");

    doThrow(new NoSuchSchemaException("schema is missing"))
        .when(dispatcher)
        .createSemanticModel(
            eq(ident),
            eq("Sales definitions"),
            any(SemanticModelDefinition.class),
            eq(Map.of("domain", "sales")));
    assertError(
        post(semanticModelPath(), request),
        Response.Status.NOT_FOUND,
        ErrorConstants.NOT_FOUND_CODE,
        NoSuchSchemaException.class.getSimpleName(),
        "schema is missing");

    doThrow(new IllegalSemanticModelException("source column is missing"))
        .when(dispatcher)
        .createSemanticModel(
            eq(ident),
            eq("Sales definitions"),
            any(SemanticModelDefinition.class),
            eq(Map.of("domain", "sales")));
    assertError(
        post(semanticModelPath(), request),
        Response.Status.BAD_REQUEST,
        ErrorConstants.ILLEGAL_ARGUMENTS_CODE,
        IllegalSemanticModelException.class.getSimpleName(),
        "source column is missing");

    doThrow(new SemanticModelAlreadyExistsException("sales already exists"))
        .when(dispatcher)
        .createSemanticModel(
            eq(ident),
            eq("Sales definitions"),
            any(SemanticModelDefinition.class),
            eq(Map.of("domain", "sales")));
    assertError(
        post(semanticModelPath(), request),
        Response.Status.CONFLICT,
        ErrorConstants.ALREADY_EXISTS_CODE,
        SemanticModelAlreadyExistsException.class.getSimpleName(),
        "sales already exists");

    doThrow(new ForbiddenException("source metadata is not visible"))
        .when(dispatcher)
        .createSemanticModel(
            eq(ident),
            eq("Sales definitions"),
            any(SemanticModelDefinition.class),
            eq(Map.of("domain", "sales")));
    assertError(
        post(semanticModelPath(), request),
        Response.Status.FORBIDDEN,
        ErrorConstants.FORBIDDEN_CODE,
        ForbiddenException.class.getSimpleName(),
        "source metadata is not visible");

    doThrow(new ConnectionFailedException("source catalog is unavailable"))
        .when(dispatcher)
        .createSemanticModel(
            eq(ident),
            eq("Sales definitions"),
            any(SemanticModelDefinition.class),
            eq(Map.of("domain", "sales")));
    ErrorResponse unavailable =
        assertError(
            post(semanticModelPath(), request),
            Response.Status.BAD_GATEWAY,
            ErrorConstants.CONNECTION_FAILED_CODE,
            ConnectionFailedException.class.getSimpleName(),
            "source catalog is unavailable");
    Assertions.assertNotNull(unavailable.getStack());
    Assertions.assertFalse(unavailable.getStack().isEmpty());
  }

  @Test
  void testCreateSemanticModelRejectsNullBody() {
    assertError(
        postJson(semanticModelPath(), "null"),
        Response.Status.BAD_REQUEST,
        ErrorConstants.ILLEGAL_ARGUMENTS_CODE,
        IllegalArgumentException.class.getSimpleName(),
        "Request body must not be null");
    verifyNoMoreInteractions(dispatcher);
  }

  @Test
  void testCreateSemanticModelMapsUnsupportedCatalog() {
    SemanticModelCreateRequest request =
        createRequest("sales", "Sales definitions", semanticModelDefinition());
    doThrow(new UnsupportedOperationException("catalog is not relational"))
        .when(dispatcher)
        .createSemanticModel(
            eq(semanticModelIdentifier("sales")),
            eq("Sales definitions"),
            any(SemanticModelDefinition.class),
            eq(Map.of("domain", "sales")));

    assertError(
        post(semanticModelPath(), request),
        Response.Status.METHOD_NOT_ALLOWED,
        ErrorConstants.UNSUPPORTED_OPERATION_CODE,
        UnsupportedOperationException.class.getSimpleName(),
        "catalog is not relational");
  }

  private SemanticModelCreateRequest createRequest(
      String name, String comment, SemanticModelDefinition definition) {
    return new SemanticModelCreateRequest(
        name,
        comment,
        SemanticModelDefinitionDTO.fromDefinition(definition),
        Map.of("domain", "sales"));
  }

  private SemanticModel semanticModel(String name, String comment) {
    return SemanticModelEntity.builder()
        .withId(1L)
        .withName(name)
        .withNamespace(namespace)
        .withComment(comment)
        .withDefinition(semanticModelDefinition())
        .withProperties(Map.of("domain", "sales"))
        .withAuditInfo(
            AuditInfo.builder()
                .withCreator("tester")
                .withCreateTime(Instant.parse("2026-08-25T00:00:00Z"))
                .build())
        .build();
  }

  private SemanticModelDefinition semanticModelDefinition() {
    return semanticModelDefinition(Dialects.ANSI_SQL);
  }

  private SemanticModelDefinition semanticModelDefinition(String dialect) {
    Field field =
        Field.builder()
            .withName("order_id")
            .withExpression(expression(dialect, "orders.order_id"))
            .withDatatype(DataType.STRING)
            .build();
    Dataset dataset =
        Dataset.builder()
            .withName("orders")
            .withSource(NameIdentifier.of(catalog, schema, "orders"))
            .withPrimaryKey(new String[] {"order_id"})
            .withFields(new Field[] {field})
            .build();
    Metric metric =
        Metric.builder()
            .withName("order_total")
            .withExpression(expression(dialect, "SUM(orders.amount)"))
            .withDatatype(DataType.DECIMAL)
            .build();
    CustomExtension customExtension =
        CustomExtension.builder().withVendorName("acme").withData("{\"certified\":true}").build();
    return SemanticModelDefinition.builder()
        .withAIContext(AIContext.of("Use certified metrics"))
        .withDatasets(new Dataset[] {dataset})
        .withMetrics(new Metric[] {metric})
        .withCustomExtensions(new CustomExtension[] {customExtension})
        .build();
  }

  private static Expression expression(String dialect, String value) {
    DialectExpression dialectExpression =
        DialectExpression.builder().withDialect(dialect).withExpression(value).build();
    return Expression.builder().withDialects(new DialectExpression[] {dialectExpression}).build();
  }

  private NameIdentifier semanticModelIdentifier(String name) {
    return NameIdentifierUtil.ofSemanticModel(metalake, catalog, schema, name);
  }

  private String semanticModelPath() {
    return "/metalakes/"
        + metalake
        + "/catalogs/"
        + catalog
        + "/schemas/"
        + schema
        + "/semantic-models";
  }

  private Response get(String path) {
    return target(path).request(MediaType.APPLICATION_JSON_TYPE).accept(VND_V1_JSON).get();
  }

  private Response post(String path, SemanticModelCreateRequest request) {
    return target(path)
        .request(MediaType.APPLICATION_JSON_TYPE)
        .accept(VND_V1_JSON)
        .post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));
  }

  private Response postJson(String path, String json) {
    return target(path)
        .request(MediaType.APPLICATION_JSON_TYPE)
        .accept(VND_V1_JSON)
        .post(Entity.entity(json, MediaType.APPLICATION_JSON_TYPE));
  }

  private static ErrorResponse assertError(
      Response response,
      Response.Status expectedStatus,
      int expectedCode,
      String expectedType,
      String expectedMessageFragment) {
    Assertions.assertEquals(expectedStatus.getStatusCode(), response.getStatus());
    ErrorResponse error = response.readEntity(ErrorResponse.class);
    Assertions.assertEquals(expectedCode, error.getCode());
    Assertions.assertEquals(expectedType, error.getType());
    Assertions.assertTrue(error.getMessage().contains(expectedMessageFragment));
    return error;
  }
}
