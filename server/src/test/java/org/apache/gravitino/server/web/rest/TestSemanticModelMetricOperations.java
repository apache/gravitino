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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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
import org.apache.gravitino.catalog.SemanticModelDispatcher;
import org.apache.gravitino.dto.responses.EntityListResponse;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.MetricInfoListResponse;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.SemanticModelEventDispatcher;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.semantic.DialectExpression;
import org.apache.gravitino.semantic.Expression;
import org.apache.gravitino.semantic.ImmutableSemanticModelMetric;
import org.apache.gravitino.semantic.Metric;
import org.apache.gravitino.semantic.SemanticModelMetric;
import org.apache.gravitino.server.authorization.MetadataAuthzHelper;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

/** Verifies both REST discovery modes, visibility filtering, and failure responses. */
public class TestSemanticModelMetricOperations extends BaseOperationsTest {
  private static final String MEDIA_TYPE = "application/vnd.gravitino.v1+json";
  private static final Namespace SCHEMA = Namespace.of("lake", "catalog", "schema");
  private static final String PATH =
      "metalakes/lake/catalogs/catalog/schemas/schema/semantic-model-metrics";
  private static final String FILTER =
      AuthorizationExpressionConstants.FILTER_SEMANTIC_MODEL_AUTHORIZATION_EXPRESSION;

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
        .register(SemanticModelMetricOperations.class)
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

  @Test
  void testDefaultAndExplicitIdentifierMode() {
    NameIdentifier[] names = {identifier("a", "revenue"), identifier("b", "revenue")};
    when(dispatcher.listMetrics(SCHEMA)).thenReturn(names);
    try (Response response = target(PATH).request(MEDIA_TYPE).get()) {
      assertEquals(200, response.getStatus());
      assertArrayEquals(names, response.readEntity(EntityListResponse.class).identifiers());
    }
    try (Response response = get(false)) {
      assertEquals(200, response.getStatus());
      assertArrayEquals(names, response.readEntity(EntityListResponse.class).identifiers());
    }
    verify(dispatcher, times(2)).listMetrics(SCHEMA);
  }

  @Test
  void testDetailsContainFullNamespaceAndMetricDefinition() {
    SemanticModelMetric first = info("a", "revenue");
    SemanticModelMetric second = info("b", "revenue");
    when(dispatcher.listMetricInfos(SCHEMA)).thenReturn(new SemanticModelMetric[] {first, second});
    try (Response response = get(true)) {
      assertEquals(200, response.getStatus());
      MetricInfoListResponse body = response.readEntity(MetricInfoListResponse.class);
      body.validate();
      assertEquals(0, body.getCode());
      assertEquals(2, body.getMetrics().length);
      assertEquals(first.namespace(), body.getMetrics()[0].namespace());
      assertEquals(first.metric(), body.getMetrics()[0].metric());
      assertEquals(second.namespace(), body.getMetrics()[1].namespace());
    }
    verify(dispatcher).listMetricInfos(SCHEMA);
  }

  @ParameterizedTest
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void testEmptyResultsUseModeSpecificArrays(boolean details, boolean nullResult) {
    when(dispatcher.listMetrics(SCHEMA)).thenReturn(nullResult ? null : new NameIdentifier[0]);
    when(dispatcher.listMetricInfos(SCHEMA))
        .thenReturn(nullResult ? null : new SemanticModelMetric[0]);
    try (Response response = get(details)) {
      assertEquals(200, response.getStatus());
      if (details)
        assertEquals(0, response.readEntity(MetricInfoListResponse.class).getMetrics().length);
      else assertEquals(0, response.readEntity(EntityListResponse.class).identifiers().length);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testErrorsNeverReturnPartialResults(boolean details) {
    List<RuntimeException> failures =
        List.of(
            new NoSuchSchemaException("missing"),
            new ForbiddenException("denied"),
            new UnsupportedOperationException("unsupported"),
            new IllegalStateException("invalid stored metrics"),
            new RuntimeException("storage failure"));
    int[] statuses = {404, 403, 501, 500, 500};
    for (int i = 0; i < failures.size(); i++) {
      if (details) doThrow(failures.get(i)).when(dispatcher).listMetricInfos(SCHEMA);
      else doThrow(failures.get(i)).when(dispatcher).listMetrics(SCHEMA);
      try (Response response = get(details)) {
        assertEquals(statuses[i], response.getStatus());
        ErrorResponse error = response.readEntity(ErrorResponse.class);
        assertEquals(
            i >= 3
                ? RuntimeException.class.getSimpleName()
                : failures.get(i).getClass().getSimpleName(),
            error.getType());
        assertTrue(error.getMessage().contains(failures.get(i).getMessage()));
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testVisibilityIsCheckedOncePerContainingModelBeforeDtoConversion(boolean details)
      throws Exception {
    NameIdentifier[] names = {
      identifier("visible", "a"), identifier("visible", "b"), identifier("hidden", "a")
    };
    SemanticModelMetric hidden = mock(SemanticModelMetric.class);
    when(hidden.namespace()).thenReturn(names[2].namespace());
    when(hidden.metric())
        .thenThrow(new AssertionError("Hidden result must not be converted to a DTO"));
    when(dispatcher.listMetrics(SCHEMA)).thenReturn(names);
    when(dispatcher.listMetricInfos(SCHEMA))
        .thenReturn(new SemanticModelMetric[] {info("visible", "a"), info("visible", "b"), hidden});
    NameIdentifier[] models = {
      NameIdentifier.of("lake", "catalog", "schema", "visible"),
      NameIdentifier.of("lake", "catalog", "schema", "hidden")
    };
    try (MockedStatic<MetadataAuthzHelper> helper = mockStatic(MetadataAuthzHelper.class)) {
      helper
          .when(
              () ->
                  MetadataAuthzHelper.filterByExpression(
                      "lake", FILTER, Entity.EntityType.SEMANTIC_MODEL, models))
          .thenReturn(new NameIdentifier[] {models[0]});
      try (Response response = operations().listMetrics("lake", "catalog", "schema", details)) {
        assertEquals(200, response.getStatus());
        if (details) {
          assertEquals(2, ((MetricInfoListResponse) response.getEntity()).getMetrics().length);
        } else {
          assertArrayEquals(
              Arrays.copyOf(names, 2), ((EntityListResponse) response.getEntity()).identifiers());
        }
      }
      helper.verify(
          () ->
              MetadataAuthzHelper.filterByExpression(
                  "lake", FILTER, Entity.EntityType.SEMANTIC_MODEL, models));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testAuthorizationDenialsAndEvaluationFailuresExcludeWholeModels(boolean details)
      throws Exception {
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
              return object.name().equals("visible");
            });
    NameIdentifier[] names = {
      identifier("visible", "a"), identifier("denied", "a"), identifier("failed", "a")
    };
    when(dispatcher.listMetrics(SCHEMA)).thenReturn(names);
    when(dispatcher.listMetricInfos(SCHEMA))
        .thenReturn(
            new SemanticModelMetric[] {
              info("visible", "a"), info("denied", "a"), info("failed", "a")
            });
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
      try (Response response = operations().listMetrics("lake", "catalog", "schema", details)) {
        assertEquals(200, response.getStatus());
        if (details) {
          assertEquals(1, ((MetricInfoListResponse) response.getEntity()).getMetrics().length);
          assertEquals(
              names[0].namespace(),
              ((MetricInfoListResponse) response.getEntity()).getMetrics()[0].namespace());
        } else
          assertArrayEquals(
              new NameIdentifier[] {names[0]},
              ((EntityListResponse) response.getEntity()).identifiers());
      }
    } finally {
      FieldUtils.writeField(GravitinoEnv.getInstance(), "config", previous, true);
    }
  }

  @Test
  void testDecodeFailurePrecedesVisibilityFiltering() throws Exception {
    doThrow(new IllegalStateException("invalid metrics in a hidden model"))
        .when(dispatcher)
        .listMetricInfos(SCHEMA);
    try (MockedStatic<MetadataAuthzHelper> helper = mockStatic(MetadataAuthzHelper.class);
        Response response = operations().listMetrics("lake", "catalog", "schema", true)) {
      assertEquals(500, response.getStatus());
      helper.verifyNoInteractions();
    }
  }

  private Response get(boolean details) {
    return target(PATH).queryParam("details", details).request(MEDIA_TYPE).get();
  }

  private SemanticModelMetricOperations operations() throws Exception {
    SemanticModelMetricOperations operations = new SemanticModelMetricOperations(dispatcher);
    FieldUtils.writeField(operations, "httpRequest", mock(HttpServletRequest.class), true);
    return operations;
  }

  private static NameIdentifier identifier(String model, String metric) {
    return NameIdentifier.of(Namespace.of("lake", "catalog", "schema", model), metric);
  }

  private static SemanticModelMetric info(String model, String name) {
    Metric metric =
        Metric.builder()
            .withName(name)
            .withDescription("Revenue")
            .withExpression(
                Expression.builder()
                    .withDialects(
                        new DialectExpression[] {
                          DialectExpression.builder()
                              .withDialect("ansi")
                              .withExpression("SUM(orders.amount)")
                              .build()
                        })
                    .build())
            .build();
    return new ImmutableSemanticModelMetric(identifier(model, name).namespace(), metric);
  }
}
