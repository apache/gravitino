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

package org.apache.gravitino.server.authorization;

import static org.apache.gravitino.authorization.Privilege.Name.SELECT_TABLE;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests RBAC and tag-policy composition, including explicit deny precedence. */
public class TestTagAuthorizationAdapter {
  private final GravitinoAuthorizer authorizer = mock(GravitinoAuthorizer.class);
  private final TagAuthorizationAdapter adapter = new TagAuthorizationAdapter(authorizer);
  private final UserPrincipal principal = new UserPrincipal("alice");
  private final AuthorizationRequestContext context = new AuthorizationRequestContext();
  private final MetadataObject catalog =
      MetadataObjects.parse("catalog", MetadataObject.Type.CATALOG);
  private final MetadataObject table =
      MetadataObjects.parse("catalog.schema.table", MetadataObject.Type.TABLE);

  @Test
  void testDisabledRbacMissSkipsDeniesAndTags() {
    assertFalse(evaluate());
    verify(authorizer, never()).deny(any(), any(), any(), any(), any());
    verify(authorizer, never()).matchTagPolicy(any(), any(), any(), any(), any());
  }

  /** Tests that an RBAC allow without a deny skips tag-policy resolution. */
  @Test
  public void testRbacAllowSkipsTags() {
    when(authorizer.authorize(principal, "metalake", catalog, SELECT_TABLE, context))
        .thenReturn(true);
    assertTrue(evaluate());
    verify(authorizer).deny(principal, "metalake", table, SELECT_TABLE, context);
    verify(authorizer, never()).matchTagPolicy(any(), any(), any(), any(), any());
  }

  /** Tests that a matching tag policy can satisfy an RBAC miss. */
  @Test
  public void testRbacMissAllowsMatchingTag() {
    when(authorizer.isTagAuthorizationEnabled()).thenReturn(true);
    doReturn(Optional.of(mock(PolicyEntity.class)))
        .when(authorizer)
        .matchTagPolicy(principal, "metalake", table, SELECT_TABLE, context);
    assertTrue(evaluate());
    verify(authorizer).matchTagPolicy(principal, "metalake", table, SELECT_TABLE, context);
    verify(authorizer, never())
        .matchTagPolicy(principal, "metalake", catalog, SELECT_TABLE, context);
  }

  /** Tests denial when neither RBAC nor a tag policy allows the privilege. */
  @Test
  public void testRbacMissAndNoMatchDeny() {
    when(authorizer.isTagAuthorizationEnabled()).thenReturn(true);
    assertFalse(evaluate());
  }

  /** An explicit deny skips policy reads regardless of whether RBAC also allows access. */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testExplicitDenySkipsTagPolicies(boolean rbacAllows) {
    when(authorizer.isTagAuthorizationEnabled()).thenReturn(true);
    when(authorizer.authorize(principal, "metalake", catalog, SELECT_TABLE, context))
        .thenReturn(rbacAllows);
    when(authorizer.deny(principal, "metalake", table, SELECT_TABLE, context)).thenReturn(true);
    assertFalse(evaluate());
    verify(authorizer, never()).matchTagPolicy(any(), any(), any(), any(), any());
  }

  @Test
  void testDebugTagReadFailurePreservesExplicitDeny() {
    when(authorizer.isTagAuthorizationEnabled()).thenReturn(true);
    when(authorizer.deny(principal, "metalake", catalog, SELECT_TABLE, context)).thenReturn(true);
    when(authorizer.matchTagPolicy(principal, "metalake", table, SELECT_TABLE, context))
        .thenThrow(new IllegalStateException("policy read failed"));
    var logger =
        ((LoggerContext)
                LogManager.getContext(TagAuthorizationAdapter.class.getClassLoader(), false))
            .getLogger(TagAuthorizationAdapter.class.getName());
    Level previous = logger.getLevel();
    logger.setLevel(Level.DEBUG);
    try {
      assertFalse(evaluate());
    } finally {
      logger.setLevel(previous);
    }
    verify(authorizer).matchTagPolicy(principal, "metalake", table, SELECT_TABLE, context);
  }

  /** Tests that a tag-policy read failure propagates instead of becoming a non-match. */
  @Test
  public void testReadFailureRemainsAnError() {
    when(authorizer.isTagAuthorizationEnabled()).thenReturn(true);
    RuntimeException failure = new IllegalStateException("policy read failed");
    when(authorizer.matchTagPolicy(principal, "metalake", table, SELECT_TABLE, context))
        .thenThrow(failure);
    assertSame(failure, assertThrows(IllegalStateException.class, this::evaluate));
  }

  /** Tests that a missing target cannot inherit a grant from an ancestor check. */
  @Test
  public void testMissingTargetDoesNotEvaluateAncestorTags() {
    when(authorizer.isTagAuthorizationEnabled()).thenReturn(true);
    assertFalse(
        adapter.authorize(
            principal, "metalake", Arrays.asList(catalog, null), SELECT_TABLE, context));
    verify(authorizer, never()).matchTagPolicy(any(), any(), any(), any(), any());
  }

  private boolean evaluate() {
    return adapter.authorize(principal, "metalake", List.of(catalog, table), SELECT_TABLE, context);
  }
}
