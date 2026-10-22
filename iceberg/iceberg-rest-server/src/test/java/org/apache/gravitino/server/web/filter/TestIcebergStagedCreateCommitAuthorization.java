/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.server.web.filter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableSet;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.ws.rs.core.Response;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.iceberg.service.provider.IcebergConfigProvider;
import org.apache.gravitino.iceberg.service.rest.IcebergTableOperations;
import org.apache.gravitino.server.authorization.GravitinoAuthorizerProvider;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/**
 * Runs the real Iceberg REST authorization interceptor against the real endpoint annotations, with
 * a privilege-set authorizer, to check which privilege a staged-create commit needs.
 *
 * <p>Gravitino documents table create as CREATE_TABLE and table alter as MODIFY_TABLE. A commit
 * that carries {@code AssertTableDoesNotExist} creates a new table, so it must need CREATE_TABLE,
 * not MODIFY_TABLE, while a regular commit still needs MODIFY_TABLE.
 */
public class TestIcebergStagedCreateCommitAuthorization {

  private static final String METALAKE = "test_metalake";
  private static final String CATALOG = "test_catalog";
  private static final String SCHEMA = "test_schema";
  private static final String TABLE = "staged_table";
  private static final String PROCEEDED = "PROCEEDED";

  private static final Set<String> USE =
      ImmutableSet.of("CATALOG::USE_CATALOG", "SCHEMA::USE_SCHEMA");

  @BeforeEach
  public void init() {
    IcebergConfigProvider configProvider = Mockito.mock(IcebergConfigProvider.class);
    Mockito.when(configProvider.getMetalakeName()).thenReturn(METALAKE);
    Mockito.when(configProvider.getDefaultCatalogName()).thenReturn(CATALOG);
    IcebergRESTServerContext.create(configProvider, false, false, false, null);
  }

  @Test
  public void testStagedCreateCommitDeniedWithModifyTableOnly() throws Throwable {
    Object result = invokeCommit(true, with("SCHEMA::MODIFY_TABLE"), "writer-without-create");
    assertForbidden(result);
  }

  @Test
  public void testStagedCreateCommitAllowedWithCreateTable() throws Throwable {
    Object result = invokeCommit(true, with("SCHEMA::CREATE_TABLE"), "creator");
    assertEquals(PROCEEDED, result);
  }

  @Test
  public void testRegularCommitAllowedWithModifyTable() throws Throwable {
    Object result = invokeCommit(false, with("SCHEMA::MODIFY_TABLE"), "writer");
    assertEquals(PROCEEDED, result);
  }

  @Test
  public void testRegularCommitDeniedWithCreateTableOnly() throws Throwable {
    Object result = invokeCommit(false, with("SCHEMA::CREATE_TABLE"), "creator-without-modify");
    assertForbidden(result);
  }

  private Object invokeCommit(boolean stagedCreate, Set<String> privileges, String user)
      throws Throwable {
    Method method =
        IcebergTableOperations.class.getMethod(
            "updateTable", String.class, String.class, String.class, UpdateTableRequest.class);
    List<UpdateRequirement> requirements =
        stagedCreate
            ? List.of(new UpdateRequirement.AssertTableDoesNotExist())
            : Collections.emptyList();
    UpdateTableRequest request = new UpdateTableRequest(requirements, Collections.emptyList());
    return invoke(method, new Object[] {CATALOG + "/", SCHEMA, TABLE, request}, privileges, user);
  }

  private Object invoke(Method method, Object[] args, Set<String> privileges, String user)
      throws Throwable {
    MethodInvocation invocation = Mockito.mock(MethodInvocation.class);
    Mockito.when(invocation.getMethod()).thenReturn(method);
    Mockito.when(invocation.getArguments()).thenReturn(args);
    Mockito.when(invocation.proceed()).thenReturn(PROCEEDED);

    GravitinoAuthorizer authorizer = privilegeSetAuthorizer(privileges);
    UserPrincipal principal = new UserPrincipal(user);
    try (MockedStatic<PrincipalUtils> principalUtils = Mockito.mockStatic(PrincipalUtils.class);
        MockedStatic<GravitinoAuthorizerProvider> providerStatic =
            Mockito.mockStatic(GravitinoAuthorizerProvider.class)) {
      principalUtils.when(PrincipalUtils::getCurrentPrincipal).thenReturn(principal);
      principalUtils.when(PrincipalUtils::getCurrentUserName).thenReturn(user);
      GravitinoAuthorizerProvider provider = Mockito.mock(GravitinoAuthorizerProvider.class);
      providerStatic.when(GravitinoAuthorizerProvider::getInstance).thenReturn(provider);
      Mockito.when(provider.getGravitinoAuthorizer()).thenReturn(authorizer);

      return new IcebergMetadataAuthorizationMethodInterceptor().invoke(invocation);
    }
  }

  /**
   * Builds an authorizer that grants exactly the given {@code TYPE::PRIVILEGE} entries and denies
   * nothing. {@code TYPE::OWNER} entries make the caller the owner of that object type.
   */
  private static GravitinoAuthorizer privilegeSetAuthorizer(Set<String> privileges) {
    GravitinoAuthorizer authorizer = Mockito.mock(GravitinoAuthorizer.class);
    Mockito.when(
            authorizer.authorize(
                ArgumentMatchers.any(),
                ArgumentMatchers.any(),
                ArgumentMatchers.any(),
                ArgumentMatchers.any(),
                ArgumentMatchers.any()))
        .thenAnswer(
            invocation -> {
              MetadataObject object = invocation.getArgument(2);
              Privilege.Name privilege = invocation.getArgument(3);
              return object != null && privileges.contains(object.type() + "::" + privilege);
            });
    Mockito.when(
            authorizer.deny(
                ArgumentMatchers.any(),
                ArgumentMatchers.any(),
                ArgumentMatchers.any(),
                ArgumentMatchers.any(),
                ArgumentMatchers.any()))
        .thenReturn(false);
    Mockito.when(
            authorizer.isOwner(
                ArgumentMatchers.any(),
                ArgumentMatchers.any(),
                ArgumentMatchers.any(),
                ArgumentMatchers.any()))
        .thenAnswer(
            invocation -> {
              MetadataObject object = invocation.getArgument(2);
              return object != null && privileges.contains(object.type() + "::OWNER");
            });
    Mockito.when(
            authorizer.findUnheldRoles(
                ArgumentMatchers.any(),
                ArgumentMatchers.any(),
                ArgumentMatchers.any(),
                ArgumentMatchers.any()))
        .thenReturn(Collections.emptySet());
    return authorizer;
  }

  private static Set<String> with(String privilege) {
    return ImmutableSet.<String>builder().addAll(USE).add(privilege).build();
  }

  private static void assertForbidden(Object result) {
    assertTrue(result instanceof Response, "expected a denial response, got: " + result);
    assertEquals(403, ((Response) result).getStatus());
  }
}
