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

package org.apache.gravitino.server.web.filter.authorization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.catalog.ViewDispatcher;
import org.apache.gravitino.exceptions.NoSuchViewException;
import org.apache.gravitino.rel.View;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.AuthorizationRequest;
import org.apache.gravitino.server.authorization.annotations.ExpressionCondition;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionEvaluator;
import org.apache.gravitino.server.web.rest.ViewOperations;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

public class TestLoadViewAuthorizationExecutor {
  @Test
  public void testAuthorizedMissingViewProbe() throws Exception {
    assertThrows(NoSuchViewException.class, () -> check(false, true, false, true, true));
  }

  @Test
  public void testExistingViewRemainsForbidden() throws Exception {
    check(false, true, true, false, true);
  }

  @Test
  public void testNoProbePermissionDoesNotCheckExistence() throws Exception {
    check(false, false, false, false, false);
  }

  @Test
  public void testPrimaryPermissionDoesNotProbe() throws Exception {
    check(true, false, true, true, false);
  }

  @Test
  public void testViewEndpointSelectsProbeExecutor() throws Exception {
    var method =
        ViewOperations.class.getMethod(
            "loadView", String.class, String.class, String.class, String.class);
    var annotation = method.getAnnotation(AuthorizationExpression.class);
    var request = method.getParameters()[3].getAnnotation(AuthorizationRequest.class);
    assertEquals(AuthorizationRequest.RequestType.LOAD_VIEW, request.type());
    assertEquals(
        AuthorizationExpressionConstants.PROBE_TABLE_LIKE_AUTHORIZATION_EXPRESSION,
        annotation.allowCheckExistence());
    assertInstanceOf(
        LoadViewAuthorizationExecutor.class,
        AuthorizeExecutorFactory.create(
            annotation.expression(),
            request.type(),
            Map.of(),
            Map.of(),
            Optional.empty(),
            method.getParameters(),
            new Object[] {"m", "c", "s", "v"},
            "",
            ExpressionCondition.NEVER,
            annotation.allowCheckExistence()));
  }

  @Test
  void testUnsupportedViewOperationsRemainForbidden() throws Exception {
    check(
        false,
        true,
        false,
        false,
        true,
        new UnsupportedOperationException("Catalog does not support view operations"));
  }

  @Test
  void testProbeBackendFailureIsNotTreatedAsMissingView() {
    IllegalStateException failure = new IllegalStateException("View backend unavailable");
    assertSame(
        failure,
        assertThrows(
            IllegalStateException.class, () -> check(false, true, false, false, true, failure)));
  }

  private void check(
      boolean primary, boolean probe, boolean exists, boolean expected, boolean shouldProbe)
      throws Exception {
    check(primary, probe, exists, expected, shouldProbe, null);
  }

  private void check(
      boolean primary,
      boolean probe,
      boolean exists,
      boolean expected,
      boolean shouldProbe,
      @Nullable RuntimeException probeFailure)
      throws Exception {
    NameIdentifier ident = NameIdentifier.of("m", "c", "s", "v");
    var executor =
        new LoadViewAuthorizationExecutor(
            AuthorizationExpressionConstants.LOAD_VIEW_AUTHORIZATION_EXPRESSION,
            Map.of(Entity.EntityType.VIEW, ident),
            Map.of(),
            Optional.empty(),
            AuthorizationExpressionConstants.PROBE_TABLE_LIKE_AUTHORIZATION_EXPRESSION);
    var primaryEvaluator = mock(AuthorizationExpressionEvaluator.class);
    var probeEvaluator = mock(AuthorizationExpressionEvaluator.class);
    when(primaryEvaluator.evaluate(anyMap(), anyMap(), any(), any())).thenReturn(primary);
    when(probeEvaluator.evaluate(anyMap(), anyMap(), any(), any())).thenReturn(probe);
    executor.authorizationExpressionEvaluator = primaryEvaluator;
    FieldUtils.writeField(executor, "allowCheckExistenceEvaluator", probeEvaluator, true);
    var env = mock(GravitinoEnv.class);
    var dispatcher = mock(ViewDispatcher.class);
    when(env.internalViewDispatcher()).thenReturn(dispatcher);
    if (probeFailure != null) {
      when(dispatcher.loadView(ident)).thenThrow(probeFailure);
    } else if (exists) {
      when(dispatcher.loadView(ident)).thenReturn(mock(View.class));
    } else {
      when(dispatcher.loadView(ident)).thenThrow(new NoSuchViewException("View is absent"));
    }
    try (MockedStatic<GravitinoEnv> mocked = mockStatic(GravitinoEnv.class)) {
      mocked.when(GravitinoEnv::getInstance).thenReturn(env);
      assertEquals(expected, executor.execute(new AuthorizationRequestContext()));
    }
    if (shouldProbe) {
      verify(dispatcher).loadView(ident);
    } else {
      verifyNoInteractions(dispatcher);
    }
  }
}
