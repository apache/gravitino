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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.catalog.SemanticModelDispatcher;
import org.apache.gravitino.dto.responses.EntityListResponse;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.SemanticModelEventDispatcher;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.server.authorization.MetadataAuthzHelper;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

/** Exercises the wire contract and full cross-catalog model load authorization. */
public class TestMetadataObjectSemanticModelOperations extends BaseOperationsTest {
  private static final String MEDIA_TYPE = "application/vnd.gravitino.v1+json";
  private static final NameIdentifier SOURCE =
      NameIdentifier.of("lake", "sources", "hr", "employees");
  private static final String FILTER =
      AuthorizationExpressionConstants.LOAD_SEMANTIC_MODEL_AUTHORIZATION_EXPRESSION;
  private final SemanticModelDispatcher dispatcher = mock(SemanticModelDispatcher.class);
  private final EventBus eventBus = mock(EventBus.class);

  private static class MockServletRequestFactory extends ServletRequestFactoryBase {
    @Override
    public HttpServletRequest get() {
      return mock(HttpServletRequest.class);
    }
  }

  @Override
  protected Application configure() {
    try {
      forceSet(
          TestProperties.CONTAINER_PORT, String.valueOf(RESTUtils.findAvailablePort(2000, 3000)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return new ResourceConfig()
        .register(MetadataObjectSemanticModelOperations.class)
        .register(
            new AbstractBinder() {
              @Override
              protected void configure() {
                bind(new SemanticModelEventDispatcher(eventBus, dispatcher))
                    .to(SemanticModelDispatcher.class)
                    .ranked(2);
                bindFactory(MockServletRequestFactory.class).to(HttpServletRequest.class);
              }
            });
  }

  @BeforeEach
  void resetDispatcher() {
    reset(dispatcher, eventBus);
  }

  @ParameterizedTest
  @ValueSource(strings = {"table", "view"})
  void testStructuredDatasetIdentifiersAndEmptyResults(String type) {
    Entity.EntityType entityType =
        type.equals("table") ? Entity.EntityType.TABLE : Entity.EntityType.VIEW;
    NameIdentifier[] names = {
      dataset("models", "semantic", "staff", "employee.with.dot"),
      dataset("other", "semantic", "staff", "经理")
    };
    when(dispatcher.listSemanticModelDatasets(SOURCE, entityType)).thenReturn(names);
    try (Response response = get(type)) {
      assertEquals(200, response.getStatus());
      EntityListResponse body = response.readEntity(EntityListResponse.class);
      body.validate();
      assertArrayEquals(names, body.identifiers());
    }
    when(dispatcher.listSemanticModelDatasets(SOURCE, entityType))
        .thenReturn(new NameIdentifier[0]);
    try (Response response = get(type)) {
      assertEquals(200, response.getStatus());
      assertEquals(0, response.readEntity(EntityListResponse.class).identifiers().length);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"schema", "catalog", "column", "unknown"})
  void testUnsupportedSourceTypes(String type) {
    try (Response response = get(type)) {
      assertEquals(400, response.getStatus());
    }
  }

  @Test
  void testSourceAndStorageFailuresFailWholeRequest() {
    List<RuntimeException> errors =
        List.of(
            new NoSuchMetadataObjectException("missing"),
            new ForbiddenException("denied"),
            new RuntimeException("storage failed"));
    int[] statuses = {404, 403, 500};
    for (int i = 0; i < errors.size(); i++) {
      doThrow(errors.get(i))
          .when(dispatcher)
          .listSemanticModelDatasets(SOURCE, Entity.EntityType.TABLE);
      try (Response response = get("table")) {
        assertEquals(statuses[i], response.getStatus());
      }
    }
  }

  @Test
  void testEachModelIsFilteredOnceAndItsDatasetsStayTogether() throws Exception {
    NameIdentifier[] datasets = {
      dataset("models", "semantic", "visible", "a"),
      dataset("models", "semantic", "visible", "b"),
      dataset("hidden", "semantic", "hidden", "c")
    };
    NameIdentifier[] models =
        Arrays.stream(datasets)
            .map(id -> NameIdentifier.of(id.namespace().levels()))
            .distinct()
            .toArray(NameIdentifier[]::new);
    when(dispatcher.listSemanticModelDatasets(SOURCE, Entity.EntityType.TABLE))
        .thenReturn(datasets);
    try (MockedStatic<MetadataAuthzHelper> helper = mockStatic(MetadataAuthzHelper.class)) {
      helper
          .when(
              () ->
                  MetadataAuthzHelper.filterByExpression(
                      "lake", FILTER, Entity.EntityType.SEMANTIC_MODEL, models))
          .thenReturn(new NameIdentifier[] {models[0]});
      try (Response response =
          operations().listSemanticModelDatasets("lake", "table", "sources.hr.employees")) {
        assertEquals(200, response.getStatus());
        assertArrayEquals(
            Arrays.copyOf(datasets, 2), ((EntityListResponse) response.getEntity()).identifiers());
      }
      helper.verify(
          () ->
              MetadataAuthzHelper.filterByExpression(
                  "lake", FILTER, Entity.EntityType.SEMANTIC_MODEL, models));
    }
  }

  @Test
  void testModelOwnershipDoesNotBypassCatalogAndSchemaAccess() throws Exception {
    Config previous = GravitinoEnv.getInstance().config();
    Config config = new Config(false) {};
    config.set(Configs.ENABLE_AUTHORIZATION, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "config", config, true);
    GravitinoAuthorizer authorizer = mock(GravitinoAuthorizer.class);
    when(authorizer.isOwner(any(), eq("lake"), any(), any()))
        .thenAnswer(
            call -> {
              MetadataObject object = call.getArgument(2);
              if (object.type() != MetadataObject.Type.SEMANTIC_MODEL) return false;
              if (object.name().equals("failed"))
                throw new IllegalStateException("authorization unavailable");
              return true;
            });
    when(authorizer.authorize(any(), eq("lake"), any(), any(), any()))
        .thenAnswer(
            call -> {
              MetadataObject object = call.getArgument(2);
              Privilege.Name privilege = call.getArgument(3);
              return (privilege == Privilege.Name.USE_CATALOG
                      && object.type() == MetadataObject.Type.CATALOG
                      && object.name().equals("allowed"))
                  || (privilege == Privilege.Name.USE_SCHEMA
                      && object.type() == MetadataObject.Type.SCHEMA
                      && object.name().equals("allowed_schema"));
            });
    NameIdentifier[] datasets = {
      dataset("allowed", "allowed_schema", "visible", "a"),
      dataset("allowed", "allowed_schema", "visible", "b"),
      dataset("blocked", "allowed_schema", "hidden_catalog", "a"),
      dataset("allowed", "blocked_schema", "hidden_schema", "a"),
      dataset("allowed", "allowed_schema", "failed", "a")
    };
    when(dispatcher.listSemanticModelDatasets(SOURCE, Entity.EntityType.TABLE))
        .thenReturn(datasets);
    try (MockedStatic<MetadataAuthzHelper> helper =
        mockStatic(MetadataAuthzHelper.class, CALLS_REAL_METHODS)) {
      helper
          .when(
              () ->
                  MetadataAuthzHelper.filterByExpression(
                      eq("lake"),
                      eq(FILTER),
                      eq(Entity.EntityType.SEMANTIC_MODEL),
                      any(NameIdentifier[].class)))
          .thenAnswer(
              call ->
                  MetadataAuthzHelper.filterByExpression(
                      "lake",
                      FILTER,
                      Entity.EntityType.SEMANTIC_MODEL,
                      call.getArgument(3, NameIdentifier[].class),
                      value -> value,
                      new UserPrincipal("tester"),
                      authorizer));
      try (Response response =
          operations().listSemanticModelDatasets("lake", "table", "sources.hr.employees")) {
        assertEquals(200, response.getStatus());
        assertArrayEquals(
            Arrays.copyOf(datasets, 2), ((EntityListResponse) response.getEntity()).identifiers());
      }
    } finally {
      FieldUtils.writeField(GravitinoEnv.getInstance(), "config", previous, true);
    }
  }

  private Response get(String type) {
    return target(
            "metalakes/lake/objects/" + type + "/sources.hr.employees/semantic-model-datasets")
        .request(MEDIA_TYPE)
        .get();
  }

  private MetadataObjectSemanticModelOperations operations() throws Exception {
    MetadataObjectSemanticModelOperations operations =
        new MetadataObjectSemanticModelOperations(dispatcher);
    FieldUtils.writeField(operations, "httpRequest", mock(HttpServletRequest.class), true);
    return operations;
  }

  private static NameIdentifier dataset(String catalog, String schema, String model, String name) {
    return NameIdentifier.of(Namespace.of("lake", catalog, schema, model), name);
  }
}
