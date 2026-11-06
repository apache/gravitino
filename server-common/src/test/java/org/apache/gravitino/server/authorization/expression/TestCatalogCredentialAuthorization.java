/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.server.authorization.expression;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.authorization.Privilege;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class TestCatalogCredentialAuthorization {
  @ParameterizedTest
  @EnumSource(
      value = MetadataObject.Type.class,
      names = {"METALAKE", "CATALOG"})
  void testGrantsDenialsAndOwnerException(MetadataObject.Type grantScope) {
    Map<Entity.EntityType, NameIdentifier> names =
        Map.of(
            Entity.EntityType.METALAKE, NameIdentifier.of("metalake"),
            Entity.EntityType.CATALOG, NameIdentifier.of("metalake", "catalog"));
    for (boolean owner : new boolean[] {false, true}) {
      for (boolean use : new boolean[] {false, true}) {
        for (boolean include : new boolean[] {false, true}) {
          for (MetadataObject.Type denyScope :
              new MetadataObject.Type[] {
                MetadataObject.Type.METALAKE, MetadataObject.Type.CATALOG
              }) {
            for (Privilege.Name denied :
                new Privilege.Name[] {
                  null, Privilege.Name.USE_SECRETS, Privilege.Name.INCLUDE_CREDENTIAL_SECRETS
                }) {
              GravitinoAuthorizer authorizer = mock(GravitinoAuthorizer.class);
              when(authorizer.isOwner(any(), any(), any(), any()))
                  .thenAnswer(
                      call ->
                          owner
                              && call.<MetadataObject>getArgument(2).type()
                                  == MetadataObject.Type.METALAKE);
              when(authorizer.authorize(any(), any(), any(), any(), any()))
                  .thenAnswer(
                      call -> {
                        if (call.<MetadataObject>getArgument(2).type() != grantScope) return false;
                        Privilege.Name privilege = call.getArgument(3);
                        // Metadata access is deliberately insufficient without both credential
                        // grants.
                        return privilege == Privilege.Name.USE_CATALOG
                            || (privilege == Privilege.Name.USE_SECRETS && use)
                            || (privilege == Privilege.Name.INCLUDE_CREDENTIAL_SECRETS && include);
                      });
              when(authorizer.deny(any(), any(), any(), any(), any()))
                  .thenAnswer(
                      call ->
                          call.<MetadataObject>getArgument(2).type() == denyScope
                              && call.<Privilege.Name>getArgument(3) == denied);
              AuthorizationExpressionEvaluator useCheck =
                  new AuthorizationExpressionEvaluator(
                      AuthorizationExpressionConstants.FILTER_USE_SECRET_AUTHORIZATION_EXPRESSION,
                      authorizer);
              AuthorizationExpressionEvaluator includeCheck =
                  new AuthorizationExpressionEvaluator(
                      AuthorizationExpressionConstants
                          .FILTER_INCLUDE_CREDENTIAL_SECRETS_AUTHORIZATION_EXPRESSION,
                      authorizer);
              UserPrincipal principal = new UserPrincipal("caller");
              boolean allowed =
                  useCheck.evaluate(
                          names, new AuthorizationRequestContext(), principal, Optional.empty())
                      && includeCheck.evaluate(
                          names, new AuthorizationRequestContext(), principal, Optional.empty());
              Assertions.assertEquals(owner || (use && include && denied == null), allowed);
            }
          }
        }
      }
    }
  }

  @ParameterizedTest
  @EnumSource(
      value = MetadataObject.Type.class,
      names = {"METALAKE", "CATALOG"})
  void testOnlyMetalakeOwnershipBypassesGrants(MetadataObject.Type ownerScope) {
    GravitinoAuthorizer authorizer = mock(GravitinoAuthorizer.class);
    when(authorizer.isOwner(any(), any(), any(), any()))
        .thenAnswer(call -> call.<MetadataObject>getArgument(2).type() == ownerScope);
    Map<Entity.EntityType, NameIdentifier> names =
        Map.of(
            Entity.EntityType.METALAKE, NameIdentifier.of("metalake"),
            Entity.EntityType.CATALOG, NameIdentifier.of("metalake", "catalog"));
    for (String expression :
        new String[] {
          AuthorizationExpressionConstants.FILTER_USE_SECRET_AUTHORIZATION_EXPRESSION,
          AuthorizationExpressionConstants
              .FILTER_INCLUDE_CREDENTIAL_SECRETS_AUTHORIZATION_EXPRESSION
        }) {
      Assertions.assertEquals(
          ownerScope == MetadataObject.Type.METALAKE,
          new AuthorizationExpressionEvaluator(expression, authorizer)
              .evaluate(
                  names,
                  new AuthorizationRequestContext(),
                  new UserPrincipal("caller"),
                  Optional.empty()));
    }
  }
}
