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
package org.apache.gravitino.server.authorization.jcasbin;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.security.Principal;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.domain.DomainDispatcher;
import org.apache.gravitino.exceptions.NoSuchDomainException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.DomainEntity;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionEvaluator;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class TestDomainParentAuthorization {

  @Test
  void ownerAndGrantChecksFollowCurrentParentAndDenyPrecedence() {
    JcasbinAuthorizer authorizer = mock(JcasbinAuthorizer.class);
    doCallRealMethod()
        .when(authorizer)
        .hasSetOwnerPermission(anyString(), anyString(), anyString(), any());
    doCallRealMethod()
        .when(authorizer)
        .hasMetadataPrivilegePermission(anyString(), anyString(), anyString(), any());
    GravitinoEnv env = mock(GravitinoEnv.class);
    DomainDispatcher domains = mock(DomainDispatcher.class);
    when(env.internalDomainDispatcher()).thenReturn(domains);
    Principal principal = () -> "steward";
    Set<String> owners = new HashSet<>();
    Set<String> allows = new HashSet<>();
    Set<String> denies = new HashSet<>();
    AtomicReference<String> parent = new AtomicReference<>("Finance");
    when(domains.getDomain("lake", "Trading.v1"))
        .thenAnswer(call -> domain("Trading.v1", parent.get()));
    when(domains.getDomain("lake", "Finance")).thenReturn(domain("Finance", null));
    when(domains.getDomain("lake", "Markets")).thenReturn(domain("Markets", null));
    when(authorizer.isOwner(any(), anyString(), any(), any()))
        .thenAnswer(call -> owners.contains(call.<MetadataObject>getArgument(2).fullName()));
    when(authorizer.authorize(any(), anyString(), any(), any(), any()))
        .thenAnswer(call -> allows.contains(call.<MetadataObject>getArgument(2).fullName()));
    when(authorizer.deny(any(), anyString(), any(), any(), any()))
        .thenAnswer(call -> denies.contains(call.<MetadataObject>getArgument(2).fullName()));
    try (MockedStatic<GravitinoEnv> environment = mockStatic(GravitinoEnv.class);
        MockedStatic<PrincipalUtils> principals = mockStatic(PrincipalUtils.class)) {
      environment.when(GravitinoEnv::getInstance).thenReturn(env);
      principals.when(PrincipalUtils::getCurrentPrincipal).thenReturn(principal);
      owners.add("Finance");
      assertTrue(authorizer.hasSetOwnerPermission("lake", "domain", "Trading.v1", context()));
      assertTrue(
          authorizer.hasMetadataPrivilegePermission("lake", "domain", "Trading.v1", context()));
      parent.set("Markets");
      assertFalse(authorizer.hasSetOwnerPermission("lake", "domain", "Trading.v1", context()));
      assertFalse(
          authorizer.hasMetadataPrivilegePermission("lake", "domain", "Trading.v1", context()));
      owners.clear();
      allows.add("Markets");
      assertTrue(
          authorizer.hasMetadataPrivilegePermission("lake", "domain", "Trading.v1", context()));
      denies.add("Trading.v1");
      assertFalse(
          authorizer.hasMetadataPrivilegePermission("lake", "domain", "Trading.v1", context()));
      owners.add("Markets");
      assertTrue(
          authorizer.hasMetadataPrivilegePermission("lake", "domain", "Trading.v1", context()));
      owners.clear();
      denies.clear();
      parent.set(null);
      assertFalse(
          authorizer.hasMetadataPrivilegePermission("lake", "domain", "Trading.v1", context()));
    }
  }

  @Test
  void genericDomainAccessUsesTheSuppliedAuthorizerAndStatus() {
    GravitinoEnv env = mock(GravitinoEnv.class);
    DomainDispatcher domains = mock(DomainDispatcher.class);
    when(env.internalDomainDispatcher()).thenReturn(domains);
    AtomicReference<DomainEntity> current = new AtomicReference<>(domain("Trading.v1", "Finance"));
    when(domains.getDomain("lake", "Trading.v1")).thenAnswer(call -> current.get());
    JcasbinAuthorizer authorizer = mock(JcasbinAuthorizer.class);
    doCallRealMethod().when(authorizer).canAccessDomain(any(), anyString(), any(), any());
    when(authorizer.authorize(any(), anyString(), any(), any(), any()))
        .thenAnswer(call -> call.getArgument(3) == Privilege.Name.USE_DOMAIN);
    Principal principal = () -> "reader";
    AuthorizationExpressionEvaluator evaluator =
        new AuthorizationExpressionEvaluator(
            AuthorizationExpressionConstants.CAN_ACCESS_METADATA, authorizer);
    try (MockedStatic<GravitinoEnv> environment = mockStatic(GravitinoEnv.class)) {
      environment.when(GravitinoEnv::getInstance).thenReturn(env);
      assertFalse(
          evaluator.evaluate(
              NameIdentifierUtil.splitNameIdentifier(
                  "lake",
                  Entity.EntityType.DOMAIN,
                  NameIdentifierUtil.ofDomain("lake", "Trading.v1")),
              context(),
              principal,
              Optional.of("DOMAIN")));
      current.set(current.get().toBuilder().withStatus(Domain.Status.PUBLISHED).build());
      assertTrue(
          evaluator.evaluate(
              NameIdentifierUtil.splitNameIdentifier(
                  "lake",
                  Entity.EntityType.DOMAIN,
                  NameIdentifierUtil.ofDomain("lake", "Trading.v1")),
              context(),
              principal,
              Optional.of("DOMAIN")));
    }
  }

  @Test
  void missingDomainFailsClosedForAccessOwnerAndGrantChecks() {
    GravitinoEnv env = mock(GravitinoEnv.class);
    DomainDispatcher domains = mock(DomainDispatcher.class);
    when(env.internalDomainDispatcher()).thenReturn(domains);
    when(domains.getDomain("lake", "missing"))
        .thenThrow(new NoSuchDomainException("Missing domain"));
    JcasbinAuthorizer authorizer = mock(JcasbinAuthorizer.class);
    doCallRealMethod().when(authorizer).canAccessDomain(any(), anyString(), any(), any());
    doCallRealMethod()
        .when(authorizer)
        .hasSetOwnerPermission(anyString(), anyString(), anyString(), any());
    doCallRealMethod()
        .when(authorizer)
        .hasMetadataPrivilegePermission(anyString(), anyString(), anyString(), any());
    Principal principal = () -> "reader";
    AuthorizationExpressionEvaluator evaluator =
        new AuthorizationExpressionEvaluator(
            AuthorizationExpressionConstants.CAN_ACCESS_METADATA, authorizer);
    try (MockedStatic<GravitinoEnv> environment = mockStatic(GravitinoEnv.class);
        MockedStatic<PrincipalUtils> principals = mockStatic(PrincipalUtils.class)) {
      environment.when(GravitinoEnv::getInstance).thenReturn(env);
      principals.when(PrincipalUtils::getCurrentPrincipal).thenReturn(principal);
      assertFalse(
          evaluator.evaluate(
              NameIdentifierUtil.splitNameIdentifier(
                  "lake", Entity.EntityType.DOMAIN, NameIdentifierUtil.ofDomain("lake", "missing")),
              context(),
              principal,
              Optional.of("DOMAIN")));
      assertFalse(authorizer.hasSetOwnerPermission("lake", "domain", "missing", context()));
      assertFalse(
          authorizer.hasMetadataPrivilegePermission("lake", "domain", "missing", context()));
    }
  }

  private static AuthorizationRequestContext context() {
    return new AuthorizationRequestContext();
  }

  private static DomainEntity domain(String name, String parent) {
    return DomainEntity.builder()
        .withId(1L)
        .withName(name)
        .withNamespace(NamespaceUtil.ofDomain("lake"))
        .withParent(parent)
        .withStatus(Domain.Status.DRAFT)
        .withProperties(Collections.emptyMap())
        .withAuditInfo(AuditInfo.builder().withCreator("test").build())
        .build();
  }
}
