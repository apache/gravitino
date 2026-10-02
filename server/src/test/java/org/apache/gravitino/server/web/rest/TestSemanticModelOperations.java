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

import static org.apache.gravitino.semantic.SemanticModel.DEFAULT_OSSIE_VERSION;
import static org.apache.gravitino.semantic.SemanticModel.PROPERTY_OSSIE_VERSION;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.catalog.SemanticModelDispatcher;
import org.apache.gravitino.catalog.TableDispatcher;
import org.apache.gravitino.catalog.ViewDispatcher;
import org.apache.gravitino.dto.requests.SemanticModelCreateRequest;
import org.apache.gravitino.dto.requests.SemanticModelUpdateRequest;
import org.apache.gravitino.dto.requests.SemanticModelUpdatesRequest;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.EntityListResponse;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.SemanticModelResponse;
import org.apache.gravitino.dto.semantic.DatasetDTO;
import org.apache.gravitino.dto.semantic.SemanticModelDefinitionDTO;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.IllegalSemanticModelException;
import org.apache.gravitino.exceptions.MetalakeNotInUseException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchSemanticModelException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.NoSuchViewException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.exceptions.SemanticModelAlreadyExistsException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.SemanticModelEntity;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
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
import org.apache.gravitino.semantic.OssieFormat;
import org.apache.gravitino.semantic.SemanticModel;
import org.apache.gravitino.semantic.SemanticModelChange;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.apache.gravitino.server.authorization.MetadataAuthzHelper;
import org.apache.gravitino.server.authorization.PassThroughAuthorizer;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

/** Tests Semantic Model REST lifecycle operations and their error mappings. */
public class TestSemanticModelOperations extends BaseOperationsTest {

  private static final String VND_V1_JSON = "application/vnd.gravitino.v1+json";
  private static final JsonMapper JSON_MAPPER = JsonMapper.builder().build();

  private static class MockServletRequestFactory extends ServletRequestFactoryBase {
    @Override
    public HttpServletRequest get() {
      return mock(HttpServletRequest.class);
    }
  }

  private final SemanticModelDispatcher dispatcher = mock(SemanticModelDispatcher.class);
  private final TableDispatcher tables = mock(TableDispatcher.class);
  private final ViewDispatcher views = mock(ViewDispatcher.class);
  private boolean viewsSupported = true;
  private GravitinoAuthorizer sourceAuthorizer = new PassThroughAuthorizer();
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
            bind(new SemanticModelSourceValidator(
                    tables, views, () -> sourceAuthorizer, ident -> viewsSupported))
                .to(SemanticModelSourceValidator.class);
            bindFactory(MockServletRequestFactory.class).to(HttpServletRequest.class);
          }
        });
    return resourceConfig;
  }

  @BeforeEach
  void resetDispatcher() {
    reset(dispatcher, tables, views);
    viewsSupported = true;
    sourceAuthorizer = new PassThroughAuthorizer();
    Table table = mock(Table.class);
    Column column = mock(Column.class);
    when(column.name()).thenReturn("order_id");
    when(table.columns()).thenReturn(new Column[] {column});
    when(tables.loadTable(any())).thenReturn(table);
    doCallRealMethod().when(dispatcher).exportOssieSemanticModel(any(), any());
  }

  @Test
  void testListSemanticModels() {
    NameIdentifier first = semanticModelIdentifier("sales");
    NameIdentifier second = semanticModelIdentifier("finance");
    when(dispatcher.listSemanticModels(namespace)).thenReturn(new NameIdentifier[] {first, second});

    Response response = get(semanticModelPath());

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    EntityListResponse body = response.readEntity(EntityListResponse.class);
    Assertions.assertEquals(0, body.getCode());
    Assertions.assertArrayEquals(new NameIdentifier[] {first, second}, body.identifiers());

    doThrow(new NoSuchSchemaException("schema is missing"))
        .when(dispatcher)
        .listSemanticModels(namespace);
    assertError(
        get(semanticModelPath()),
        Response.Status.NOT_FOUND,
        ErrorConstants.NOT_FOUND_CODE,
        NoSuchSchemaException.class.getSimpleName(),
        "schema is missing");
  }

  @Test
  void testListSemanticModelsFiltersUnauthorizedEntries() throws IllegalAccessException {
    NameIdentifier visible = semanticModelIdentifier("visible");
    NameIdentifier hidden = semanticModelIdentifier("hidden");
    NameIdentifier[] listed = {visible, hidden};
    NameIdentifier[] filtered = {visible};
    when(dispatcher.listSemanticModels(namespace)).thenReturn(listed);
    SemanticModelOperations operations =
        new SemanticModelOperations(
            dispatcher,
            new SemanticModelSourceValidator(tables, views, () -> sourceAuthorizer, ident -> true));
    FieldUtils.writeField(operations, "httpRequest", mock(HttpServletRequest.class), true);
    try (MockedStatic<MetadataAuthzHelper> helper = mockStatic(MetadataAuthzHelper.class)) {
      helper
          .when(
              () ->
                  MetadataAuthzHelper.filterByExpression(
                      metalake,
                      AuthorizationExpressionConstants
                          .FILTER_SEMANTIC_MODEL_AUTHORIZATION_EXPRESSION,
                      EntityType.SEMANTIC_MODEL,
                      listed))
          .thenReturn(filtered);
      Response response = operations.listSemanticModels(metalake, catalog, schema);
      Assertions.assertEquals(200, response.getStatus());
      Assertions.assertArrayEquals(
          filtered, ((EntityListResponse) response.getEntity()).identifiers());
      helper.verify(
          () ->
              MetadataAuthzHelper.filterByExpression(
                  metalake,
                  AuthorizationExpressionConstants.FILTER_SEMANTIC_MODEL_AUTHORIZATION_EXPRESSION,
                  EntityType.SEMANTIC_MODEL,
                  listed));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"missing", "denied", "connection", "column"})
  void testOssieImportValidatesSourcesBeforePersistence(String failure) {
    String yaml =
        "version: 0.2.0.dev0\nname: sales\ndatasets:\n"
            + "  - name: orders\n    source: catalog.schema.orders\n    primary_key: [order_id]\n";
    Response.Status status = Response.Status.BAD_REQUEST;
    int code = ErrorConstants.ILLEGAL_ARGUMENTS_CODE;
    Class<? extends Exception> type = IllegalSemanticModelException.class;
    String message = "does not exist";
    switch (failure) {
      case "missing":
        viewsSupported = false;
        when(tables.loadTable(any())).thenThrow(new NoSuchTableException("missing"));
        break;
      case "denied":
        sourceAuthorizer = mock(GravitinoAuthorizer.class);
        status = Response.Status.FORBIDDEN;
        code = ErrorConstants.FORBIDDEN_CODE;
        type = ForbiddenException.class;
        message = "Not authorized";
        break;
      case "connection":
        when(tables.loadTable(any())).thenThrow(new ConnectionFailedException("offline"));
        status = Response.Status.BAD_GATEWAY;
        code = ErrorConstants.CONNECTION_FAILED_CODE;
        type = ConnectionFailedException.class;
        message = "offline";
        break;
      case "column":
        Table table = mock(Table.class);
        when(table.columns()).thenReturn(new Column[0]);
        when(tables.loadTable(any())).thenReturn(table);
        message = "order_id";
        break;
      default:
        throw new AssertionError(failure);
    }
    assertError(
        postDocument(semanticModelPath() + "/ossie", yaml, "application/yaml"),
        status,
        code,
        type.getSimpleName(),
        message);
    verifyNoInteractions(dispatcher, views);
    if (failure.equals("denied")) {
      verifyNoInteractions(tables);
    }
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
    verifyNoInteractions(tables, views);
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
  void testLoadSemanticModelNotInUse() {
    NameIdentifier ident = semanticModelIdentifier("sales");
    doThrow(new MetalakeNotInUseException("metalake is not in use"))
        .when(dispatcher)
        .loadSemanticModel(ident);

    assertError(
        get(semanticModelPath() + "/sales"),
        Response.Status.CONFLICT,
        ErrorConstants.NOT_IN_USE_CODE,
        MetalakeNotInUseException.class.getSimpleName(),
        "metalake is not in use");
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
  void testCreateSemanticModelDefaultsOmittedProperties() {
    NameIdentifier ident = semanticModelIdentifier("sales");
    SemanticModelCreateRequest request =
        new SemanticModelCreateRequest(
            "sales",
            "Sales definitions",
            SemanticModelDefinitionDTO.fromDefinition(semanticModelDefinition()),
            null);
    SemanticModel semanticModel = semanticModel("sales", "Sales definitions");
    when(dispatcher.createSemanticModel(
            eq(ident), eq("Sales definitions"), any(SemanticModelDefinition.class), eq(Map.of())))
        .thenReturn(semanticModel);

    Response response = post(semanticModelPath(), request);

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    verify(dispatcher)
        .createSemanticModel(
            eq(ident), eq("Sales definitions"), any(SemanticModelDefinition.class), eq(Map.of()));
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
        Response.Status.NOT_IMPLEMENTED,
        ErrorConstants.UNSUPPORTED_OPERATION_CODE,
        UnsupportedOperationException.class.getSimpleName(),
        "catalog is not relational");
  }

  @Test
  void testAlterSemanticModelConvertsAllChangesAtomically() {
    NameIdentifier ident = semanticModelIdentifier("sales");
    SemanticModelDefinition replacement = semanticModelDefinition("TRINO");
    SemanticModelUpdatesRequest request =
        new SemanticModelUpdatesRequest(
            List.of(
                new SemanticModelUpdateRequest.RenameSemanticModelRequest("sales_v2"),
                new SemanticModelUpdateRequest.UpdateSemanticModelCommentRequest("Updated"),
                new SemanticModelUpdateRequest.SetSemanticModelPropertyRequest("owner", "finance"),
                new SemanticModelUpdateRequest.RemoveSemanticModelPropertyRequest("legacy"),
                new SemanticModelUpdateRequest.ReplaceSemanticModelDefinitionRequest(
                    SemanticModelDefinitionDTO.fromDefinition(replacement))));
    when(dispatcher.alterSemanticModel(eq(ident), any(SemanticModelChange[].class)))
        .thenReturn(semanticModel("sales_v2", "Updated"));

    Response response = put(semanticModelPath() + "/sales", request);

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    SemanticModelResponse body = response.readEntity(SemanticModelResponse.class);
    body.validate();
    Assertions.assertEquals("sales_v2", body.getSemanticModel().name());

    ArgumentCaptor<SemanticModelChange[]> changesCaptor =
        ArgumentCaptor.forClass(SemanticModelChange[].class);
    verify(dispatcher).alterSemanticModel(eq(ident), changesCaptor.capture());
    SemanticModelChange[] changes = changesCaptor.getValue();
    Assertions.assertEquals(5, changes.length);
    Assertions.assertInstanceOf(SemanticModelChange.RenameSemanticModel.class, changes[0]);
    Assertions.assertInstanceOf(SemanticModelChange.UpdateComment.class, changes[1]);
    Assertions.assertInstanceOf(SemanticModelChange.SetProperty.class, changes[2]);
    Assertions.assertInstanceOf(SemanticModelChange.RemoveProperty.class, changes[3]);
    Assertions.assertInstanceOf(SemanticModelChange.ReplaceDefinition.class, changes[4]);
    Assertions.assertEquals(
        replacement, ((SemanticModelChange.ReplaceDefinition) changes[4]).getDefinition());
  }

  @Test
  void testAlterSemanticModelRejectsInvalidBatchBeforeDispatch() {
    SemanticModelDefinitionDTO invalidDefinition =
        SemanticModelDefinitionDTO.builder().withDatasets(new DatasetDTO[] {null}).build();
    SemanticModelUpdatesRequest request =
        new SemanticModelUpdatesRequest(
            List.of(
                new SemanticModelUpdateRequest.UpdateSemanticModelCommentRequest("Updated"),
                new SemanticModelUpdateRequest.ReplaceSemanticModelDefinitionRequest(
                    invalidDefinition)));

    assertError(
        put(semanticModelPath() + "/sales", request),
        Response.Status.BAD_REQUEST,
        ErrorConstants.ILLEGAL_ARGUMENTS_CODE,
        IllegalArgumentException.class.getSimpleName(),
        "datasets[0] must not be null");
    verifyNoMoreInteractions(dispatcher);
  }

  @Test
  void testAlterSemanticModelRejectsNullBody() {
    assertError(
        putJson(semanticModelPath() + "/sales", "null"),
        Response.Status.BAD_REQUEST,
        ErrorConstants.ILLEGAL_ARGUMENTS_CODE,
        IllegalArgumentException.class.getSimpleName(),
        "Request body must not be null");
    verifyNoMoreInteractions(dispatcher);
  }

  @Test
  void testAlterSemanticModelErrors() {
    NameIdentifier ident = semanticModelIdentifier("sales");
    SemanticModelUpdatesRequest request =
        new SemanticModelUpdatesRequest(
            List.of(new SemanticModelUpdateRequest.UpdateSemanticModelCommentRequest("Updated")));

    doThrow(new NoSuchSemanticModelException("sales does not exist"))
        .when(dispatcher)
        .alterSemanticModel(eq(ident), any(SemanticModelChange[].class));
    assertError(
        put(semanticModelPath() + "/sales", request),
        Response.Status.NOT_FOUND,
        ErrorConstants.NOT_FOUND_CODE,
        NoSuchSemanticModelException.class.getSimpleName(),
        "sales does not exist");

    doThrow(new OptimisticLockException("sales changed in this transaction"))
        .when(dispatcher)
        .alterSemanticModel(eq(ident), any(SemanticModelChange[].class));
    assertError(
        put(semanticModelPath() + "/sales", request),
        Response.Status.CONFLICT,
        ErrorConstants.OPTIMISTIC_LOCK_CONFLICT_CODE,
        OptimisticLockException.class.getSimpleName(),
        "sales changed in this transaction");
  }

  @Test
  void testDropSemanticModel() {
    NameIdentifier ident = semanticModelIdentifier("sales");
    when(dispatcher.dropSemanticModel(ident)).thenReturn(true, false);

    Response dropped = delete(semanticModelPath() + "/sales");
    Assertions.assertTrue(dropped.readEntity(DropResponse.class).dropped());
    Response missing = delete(semanticModelPath() + "/sales");
    Assertions.assertFalse(missing.readEntity(DropResponse.class).dropped());

    doThrow(new OptimisticLockException("sales changed in this transaction"))
        .when(dispatcher)
        .dropSemanticModel(ident);
    assertError(
        delete(semanticModelPath() + "/sales"),
        Response.Status.CONFLICT,
        ErrorConstants.OPTIMISTIC_LOCK_CONFLICT_CODE,
        OptimisticLockException.class.getSimpleName(),
        "sales changed in this transaction");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testSourceValidationRunsWithAuthorizationDisabled(boolean replaceDefinition) {
    when(tables.loadTable(any())).thenThrow(new NoSuchTableException("missing table"));
    when(views.loadView(any())).thenThrow(new NoSuchViewException("missing view"));
    assertError(
        writeDefinition(replaceDefinition, semanticModelDefinition()),
        Response.Status.BAD_REQUEST,
        ErrorConstants.ILLEGAL_ARGUMENTS_CODE,
        IllegalSemanticModelException.class.getSimpleName(),
        "does not exist");
    verifyNoInteractions(dispatcher);
  }

  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void testMissingSourceWithoutViewSupport(
      boolean replaceDefinition, boolean authorizationEnabled) {
    viewsSupported = false;
    if (authorizationEnabled) {
      sourceAuthorizer = mock(GravitinoAuthorizer.class);
      when(sourceAuthorizer.authorize(any(), any(), any(), any(), any())).thenReturn(true);
    }
    when(tables.loadTable(any())).thenThrow(new NoSuchTableException("missing table"));
    when(views.loadView(any()))
        .thenThrow(new UnsupportedOperationException("Catalog does not support view operations"));
    assertError(
        writeDefinition(replaceDefinition, semanticModelDefinition()),
        Response.Status.BAD_REQUEST,
        ErrorConstants.ILLEGAL_ARGUMENTS_CODE,
        IllegalSemanticModelException.class.getSimpleName(),
        "does not exist");
    verifyNoInteractions(dispatcher, views);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testMissingColumnWithAuthorizationDisabled(boolean replaceDefinition) {
    Table table = mock(Table.class);
    when(table.columns()).thenReturn(new Column[0]);
    when(tables.loadTable(any())).thenReturn(table);
    assertError(
        writeDefinition(replaceDefinition, semanticModelDefinition()),
        Response.Status.BAD_REQUEST,
        ErrorConstants.ILLEGAL_ARGUMENTS_CODE,
        IllegalSemanticModelException.class.getSimpleName(),
        "order_id");
    verifyNoInteractions(dispatcher);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testDeniedSourceIsNotResolvedOrPersisted(boolean replaceDefinition) {
    sourceAuthorizer = mock(GravitinoAuthorizer.class);
    assertError(
        writeDefinition(replaceDefinition, semanticModelDefinition()),
        Response.Status.FORBIDDEN,
        ErrorConstants.FORBIDDEN_CODE,
        ForbiddenException.class.getSimpleName(),
        "Not authorized");
    verifyNoInteractions(tables, views, dispatcher);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testSourceConnectionFailureIsBadGateway(boolean replaceDefinition) {
    when(tables.loadTable(any())).thenThrow(new ConnectionFailedException("source unavailable"));
    assertError(
        writeDefinition(replaceDefinition, semanticModelDefinition()),
        Response.Status.BAD_GATEWAY,
        ErrorConstants.CONNECTION_FAILED_CODE,
        ConnectionFailedException.class.getSimpleName(),
        "source unavailable");
    verifyNoInteractions(views, dispatcher);
  }

  @ParameterizedTest
  @MethodSource("whitespaceSources")
  void testWhitespaceSourceValidation(
      int segment, boolean replaceDefinition, boolean authorizationEnabled, boolean sourceExists) {
    if (authorizationEnabled) {
      sourceAuthorizer = mock(GravitinoAuthorizer.class);
      when(sourceAuthorizer.authorize(any(), any(), any(), any(), any())).thenReturn(true);
    }
    String[] parts = {catalog, schema, "orders"};
    parts[segment] = "   ";
    NameIdentifier source = NameIdentifier.of(parts);
    NameIdentifier fullSource = NameIdentifier.of(metalake, parts[0], parts[1], parts[2]);
    SemanticModelDefinition definition =
        SemanticModelDefinition.builder()
            .withDatasets(
                new Dataset[] {
                  Dataset.builder()
                      .withName("orders")
                      .withSource(source)
                      .withPrimaryKey(new String[] {"order_id"})
                      .build()
                })
            .build();
    if (!sourceExists) {
      when(tables.loadTable(fullSource)).thenThrow(new NoSuchTableException("missing table"));
      when(views.loadView(fullSource)).thenThrow(new NoSuchViewException("missing view"));
    }
    when(dispatcher.createSemanticModel(any(), any(), any(), any()))
        .thenReturn(semanticModel("sales", null));
    when(dispatcher.alterSemanticModel(any(), any(SemanticModelChange[].class)))
        .thenReturn(semanticModel("sales", null));
    Response response = writeDefinition(replaceDefinition, definition);
    if (segment == 0) {
      assertError(
          response,
          Response.Status.BAD_REQUEST,
          ErrorConstants.ILLEGAL_ARGUMENTS_CODE,
          IllegalArgumentException.class.getSimpleName(),
          "Metadata object full name cannot be blank");
      verifyNoInteractions(tables, views, dispatcher);
      return;
    }
    if (sourceExists) {
      // Characterize existing behavior: resolvable names are not rejected solely for whitespace.
      Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
      if (replaceDefinition) {
        verify(dispatcher).alterSemanticModel(any(), any(SemanticModelChange[].class));
      } else {
        verify(dispatcher).createSemanticModel(any(), any(), eq(definition), any());
      }
      verifyNoInteractions(views);
    } else {
      assertError(
          response,
          Response.Status.BAD_REQUEST,
          ErrorConstants.ILLEGAL_ARGUMENTS_CODE,
          IllegalSemanticModelException.class.getSimpleName(),
          "does not exist");
      verify(views).loadView(fullSource);
      verifyNoInteractions(dispatcher);
    }
    verify(tables).loadTable(fullSource);
  }

  private static Stream<Arguments> whitespaceSources() {
    return IntStream.range(0, 3)
        .boxed()
        .flatMap(
            segment ->
                Stream.of(false, true)
                    .flatMap(
                        replace ->
                            Stream.of(false, true)
                                .flatMap(
                                    authorized ->
                                        Stream.of(false, true)
                                            .map(
                                                exists ->
                                                    Arguments.of(
                                                        segment, replace, authorized, exists)))));
  }

  private Response writeDefinition(boolean replaceDefinition, SemanticModelDefinition definition) {
    return replaceDefinition
        ? put(
            semanticModelPath() + "/sales",
            new SemanticModelUpdatesRequest(
                List.of(
                    new SemanticModelUpdateRequest.ReplaceSemanticModelDefinitionRequest(
                        SemanticModelDefinitionDTO.fromDefinition(definition)))))
        : post(semanticModelPath(), createRequest("sales", null, definition));
  }

  @ParameterizedTest
  @ValueSource(strings = {"application/json", "application/json; charset=UTF-8"})
  void testImportOssieYamlAndJsonDocuments(String jsonMediaType) {
    NameIdentifier salesIdent = semanticModelIdentifier("sales");
    NameIdentifier inventoryIdent = semanticModelIdentifier("inventory");
    when(dispatcher.createSemanticModel(
            eq(salesIdent),
            eq("Sales definitions"),
            any(SemanticModelDefinition.class),
            eq(Map.of("domain", "sales", PROPERTY_OSSIE_VERSION, DEFAULT_OSSIE_VERSION))))
        .thenReturn(semanticModel("sales", "Sales definitions"));
    when(dispatcher.createSemanticModel(
            eq(inventoryIdent),
            eq(null),
            any(SemanticModelDefinition.class),
            eq(Map.of(PROPERTY_OSSIE_VERSION, "future-version"))))
        .thenReturn(semanticModel("inventory", null));

    String yaml =
        """
        version: 0.2.0.dev0
        name: sales
        description: Sales definitions
        datasets:
          - name: orders
            source: semantic_model_catalog.semantic_model_schema.orders
            primary_key: [order_id]
            fields:
              - name: order_id
                expression:
                  dialects:
                    - dialect: ANSI_SQL
                      expression: orders.order_id
                datatype: String
        custom_extensions:
          - vendor_name: GRAVITINO_PROPERTIES
            data: '{"domain":"sales"}'
        """;
    Response yamlResponse = postDocument(semanticModelPath() + "/ossie", yaml, "application/yaml");

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), yamlResponse.getStatus());
    SemanticModelResponse yamlBody = yamlResponse.readEntity(SemanticModelResponse.class);
    yamlBody.validate();
    Assertions.assertEquals("sales", yamlBody.getSemanticModel().name());

    String json =
        """
        {
          "version": "future-version",
          "name": "inventory",
          "datasets": [
            {
              "name": "orders",
              "source": "semantic_model_catalog.semantic_model_schema.orders",
              "fields": []
            }
          ]
        }
        """;
    Response jsonResponse = postDocument(semanticModelPath() + "/ossie", json, jsonMediaType);

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), jsonResponse.getStatus());
    SemanticModelResponse jsonBody = jsonResponse.readEntity(SemanticModelResponse.class);
    jsonBody.validate();
    Assertions.assertEquals("inventory", jsonBody.getSemanticModel().name());

    ArgumentCaptor<SemanticModelDefinition> definitionCaptor =
        ArgumentCaptor.forClass(SemanticModelDefinition.class);
    verify(dispatcher)
        .createSemanticModel(
            eq(salesIdent),
            eq("Sales definitions"),
            definitionCaptor.capture(),
            eq(Map.of("domain", "sales", PROPERTY_OSSIE_VERSION, DEFAULT_OSSIE_VERSION)));
    Assertions.assertEquals(
        NameIdentifier.of(catalog, schema, "orders"),
        definitionCaptor.getValue().datasets()[0].source());
    Assertions.assertEquals(
        Dialects.ANSI_SQL,
        definitionCaptor.getValue().datasets()[0].fields()[0].expression().dialects()[0].dialect());
    verify(dispatcher)
        .createSemanticModel(
            eq(inventoryIdent),
            eq(null),
            any(SemanticModelDefinition.class),
            eq(Map.of(PROPERTY_OSSIE_VERSION, "future-version")));
    verifyNoMoreInteractions(dispatcher);
  }

  @ParameterizedTest
  @ValueSource(strings = {"text/yaml", "application/x-yaml"})
  void testImportOssieYamlMediaTypeAliases(String mediaType) {
    NameIdentifier ident = semanticModelIdentifier("marketing");
    when(dispatcher.createSemanticModel(
            eq(ident),
            eq(null),
            any(SemanticModelDefinition.class),
            eq(Map.of(PROPERTY_OSSIE_VERSION, DEFAULT_OSSIE_VERSION))))
        .thenReturn(semanticModel("marketing", null));

    String yaml =
        """
        version: 0.2.0.dev0
        name: marketing
        datasets:
          - name: campaigns
            source: semantic_model_catalog.semantic_model_schema.campaigns
            fields: []
        """;
    Response response = postDocument(semanticModelPath() + "/ossie", yaml, mediaType);

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    SemanticModelResponse body = response.readEntity(SemanticModelResponse.class);
    body.validate();
    Assertions.assertEquals("marketing", body.getSemanticModel().name());
    verify(dispatcher)
        .createSemanticModel(
            eq(ident),
            eq(null),
            any(SemanticModelDefinition.class),
            eq(Map.of(PROPERTY_OSSIE_VERSION, DEFAULT_OSSIE_VERSION)));
    verifyNoMoreInteractions(dispatcher);
  }

  @Test
  void testExportOssieYamlAndJsonDocuments() throws Exception {
    NameIdentifier ident = semanticModelIdentifier("sales");
    when(dispatcher.loadSemanticModel(ident))
        .thenReturn(semanticModel("sales", "Sales definitions"));

    Response yamlResponse = getOssie(semanticModelPath() + "/sales/ossie");

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), yamlResponse.getStatus());
    Assertions.assertEquals("application/yaml", yamlResponse.getMediaType().toString());
    Assertions.assertEquals(
        "attachment; filename=\"sales.ossie.yaml\"",
        yamlResponse.getHeaderString("Content-Disposition"));
    String yaml = yamlResponse.readEntity(String.class);
    Assertions.assertTrue(yaml.contains("version:"));
    Assertions.assertTrue(yaml.contains("name:"));
    Assertions.assertFalse(yaml.contains("semantic_model:"));
    Assertions.assertFalse(yaml.contains("definition:"));

    Response jsonResponse =
        target(semanticModelPath() + "/sales/ossie")
            .queryParam("format", "json")
            .request("application/yaml")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), jsonResponse.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON, jsonResponse.getMediaType().toString());
    Assertions.assertEquals(
        "attachment; filename=\"sales.ossie.json\"",
        jsonResponse.getHeaderString("Content-Disposition"));
    JsonNode json = JSON_MAPPER.readTree(jsonResponse.readEntity(String.class));
    Assertions.assertEquals("0.2.0.dev0", json.path("version").textValue());
    Assertions.assertEquals("sales", json.path("name").textValue());
    Assertions.assertEquals(
        "semantic_model_catalog.semantic_model_schema.orders",
        json.at("/datasets/0/source").textValue());
    Assertions.assertFalse(json.has("semantic_model"));
    Assertions.assertFalse(json.has("definition"));
    verify(dispatcher).exportOssieSemanticModel(ident, OssieFormat.YAML);
    verify(dispatcher).exportOssieSemanticModel(ident, OssieFormat.JSON);
    verify(dispatcher, times(2)).loadSemanticModel(ident);
    verifyNoMoreInteractions(dispatcher);
  }

  @Test
  void testOssieConversionErrorsDoNotCallNativeOperations() {
    String querySource =
        """
        version: 0.2.0.dev0
        name: sales
        datasets:
          - name: orders
            source: SELECT * FROM orders
        """;
    assertError(
        postDocument(semanticModelPath() + "/ossie", querySource, "application/x-yaml"),
        Response.Status.BAD_REQUEST,
        ErrorConstants.ILLEGAL_ARGUMENTS_CODE,
        IllegalSemanticModelException.class.getSimpleName(),
        "query sources are not supported");

    assertError(
        target(semanticModelPath() + "/sales/ossie").queryParam("format", "csv").request().get(),
        Response.Status.BAD_REQUEST,
        ErrorConstants.ILLEGAL_ARGUMENTS_CODE,
        IllegalArgumentException.class.getSimpleName(),
        "expected yaml or json");
    verifyNoMoreInteractions(dispatcher);
  }

  @Test
  void testOssieImportRejectsYamlDeclaredAsJson() {
    String yaml = "version: 0.2.0.dev0\nname: sales\ndatasets: []\n";
    assertError(
        postDocument(semanticModelPath() + "/ossie", yaml, MediaType.APPLICATION_JSON),
        Response.Status.BAD_REQUEST,
        ErrorConstants.ILLEGAL_ARGUMENTS_CODE,
        IllegalSemanticModelException.class.getSimpleName(),
        "Cannot parse Apache Ossie JSON");
    verifyNoMoreInteractions(dispatcher);
  }

  @Test
  void testOssieImportRejectsUnsupportedMediaType() {
    try (Response response =
        postDocument(semanticModelPath() + "/ossie", "name: sales", "text/plain")) {
      Assertions.assertEquals(
          Response.Status.UNSUPPORTED_MEDIA_TYPE.getStatusCode(), response.getStatus());
    }
    verifyNoMoreInteractions(dispatcher);
  }

  @Test
  void testOssieImportPropagatesCreateErrors() {
    String yaml =
        "version: 0.2.0.dev0\nname: sales\ndatasets:\n"
            + "  - name: orders\n    source: sales.mart.orders\n";
    doThrow(new SemanticModelAlreadyExistsException("sales already exists"))
        .when(dispatcher)
        .createSemanticModel(eq(semanticModelIdentifier("sales")), eq(null), any(), any());
    assertError(
        postDocument(semanticModelPath() + "/ossie", yaml, "application/yaml"),
        Response.Status.CONFLICT,
        ErrorConstants.ALREADY_EXISTS_CODE,
        SemanticModelAlreadyExistsException.class.getSimpleName(),
        "sales already exists");
  }

  @Test
  void testOssieExportPropagatesLoadErrors() {
    doThrow(new NoSuchSemanticModelException("sales does not exist"))
        .when(dispatcher)
        .loadSemanticModel(semanticModelIdentifier("sales"));
    assertError(
        getOssie(semanticModelPath() + "/sales/ossie"),
        Response.Status.NOT_FOUND,
        ErrorConstants.NOT_FOUND_CODE,
        NoSuchSemanticModelException.class.getSimpleName(),
        "sales does not exist");
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

  private Response put(String path, SemanticModelUpdatesRequest request) {
    return target(path)
        .request(MediaType.APPLICATION_JSON_TYPE)
        .accept(VND_V1_JSON)
        .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));
  }

  private Response putJson(String path, String json) {
    return target(path)
        .request(MediaType.APPLICATION_JSON_TYPE)
        .accept(VND_V1_JSON)
        .put(Entity.entity(json, MediaType.APPLICATION_JSON_TYPE));
  }

  private Response delete(String path) {
    return target(path).request(MediaType.APPLICATION_JSON_TYPE).accept(VND_V1_JSON).delete();
  }

  private Response postDocument(String path, String document, String mediaType) {
    return target(path)
        .request(MediaType.APPLICATION_JSON_TYPE)
        .accept(VND_V1_JSON)
        .post(Entity.entity(document, mediaType));
  }

  private Response getOssie(String path) {
    return target(path).request().get();
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
