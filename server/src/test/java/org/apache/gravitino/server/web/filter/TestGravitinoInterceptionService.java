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

package org.apache.gravitino.server.web.filter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Method;
import java.security.Principal;
import java.util.List;
import javax.ws.rs.core.Response;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.server.authorization.GravitinoAuthorizerProvider;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;
import org.apache.gravitino.server.web.Utils;
import org.apache.gravitino.utils.PrincipalUtils;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/** Test for {@link GravitinoInterceptionService}. */
public class TestGravitinoInterceptionService {

  @Test
  public void testMetadataAuthorizationMethodInterceptor() throws Throwable {
    try (MockedStatic<PrincipalUtils> principalUtilsMocked = mockStatic(PrincipalUtils.class);
        MockedStatic<GravitinoAuthorizerProvider> mockStatic =
            mockStatic(GravitinoAuthorizerProvider.class)) {
      principalUtilsMocked
          .when(PrincipalUtils::getCurrentPrincipal)
          .thenReturn(new UserPrincipal("tester"));
      principalUtilsMocked.when(PrincipalUtils::getCurrentUserName).thenReturn("tester");
      MethodInvocation methodInvocation = mock(MethodInvocation.class);
      GravitinoAuthorizerProvider mockedProvider = mock(GravitinoAuthorizerProvider.class);
      mockStatic.when(GravitinoAuthorizerProvider::getInstance).thenReturn(mockedProvider);
      when(mockedProvider.getGravitinoAuthorizer()).thenReturn(new MockGravitinoAuthorizer());
      GravitinoInterceptionService gravitinoInterceptionService =
          new GravitinoInterceptionService();
      Class<TestOperations> testOperationsClass = TestOperations.class;
      Method[] methods = testOperationsClass.getMethods();
      Method testMethod = methods[0];
      List<MethodInterceptor> methodInterceptors =
          gravitinoInterceptionService.getMethodInterceptors(testMethod);
      MethodInterceptor methodInterceptor = methodInterceptors.get(0);
      assertEquals(1, methodInterceptors.size());
      when(methodInvocation.proceed()).thenReturn(new TestOperations().testMethod("testMetalake"));
      when(methodInvocation.getMethod()).thenReturn(testMethod);
      when(methodInvocation.getArguments()).thenReturn(new Object[] {"testMetalake"});
      Response response = (Response) methodInterceptor.invoke(methodInvocation);
      assertEquals("ok", response.getEntity());
      when(methodInvocation.getMethod()).thenReturn(testMethod);
      when(methodInvocation.getArguments()).thenReturn(new Object[] {"testMetalake2"});
      Response response2 = (Response) methodInterceptor.invoke(methodInvocation);
      assertEquals(
          "User 'tester' is not authorized to perform operation 'testMethod' on metadata 'testMetalake2'",
          ((ErrorResponse) response2.getEntity()).getMessage());
    }
  }

  @Test
  public void testSystemInternalErrorHandling() throws Throwable {
    try (MockedStatic<PrincipalUtils> principalUtilsMocked = mockStatic(PrincipalUtils.class);
        MockedStatic<GravitinoAuthorizerProvider> mockStatic =
            mockStatic(GravitinoAuthorizerProvider.class)) {
      principalUtilsMocked
          .when(PrincipalUtils::getCurrentPrincipal)
          .thenReturn(new UserPrincipal("tester"));
      principalUtilsMocked.when(PrincipalUtils::getCurrentUserName).thenReturn("tester");

      MethodInvocation methodInvocation = mock(MethodInvocation.class);
      GravitinoAuthorizerProvider mockedProvider = mock(GravitinoAuthorizerProvider.class);
      mockStatic.when(GravitinoAuthorizerProvider::getInstance).thenReturn(mockedProvider);

      // Mock an exception during authorization
      when(mockedProvider.getGravitinoAuthorizer())
          .thenThrow(new RuntimeException("Database connection failed"));

      GravitinoInterceptionService gravitinoInterceptionService =
          new GravitinoInterceptionService();
      Class<TestOperations> testOperationsClass = TestOperations.class;
      Method[] methods = testOperationsClass.getMethods();
      Method testMethod = methods[0];
      List<MethodInterceptor> methodInterceptors =
          gravitinoInterceptionService.getMethodInterceptors(testMethod);
      MethodInterceptor methodInterceptor = methodInterceptors.get(0);

      // Test system internal error
      when(methodInvocation.getMethod()).thenReturn(testMethod);
      when(methodInvocation.getArguments()).thenReturn(new Object[] {"testMetalake"});
      Response response = (Response) methodInterceptor.invoke(methodInvocation);

      // Verify the system internal error message
      ErrorResponse errorResponse = (ErrorResponse) response.getEntity();
      assertEquals(
          "Authorization failed due to system internal error. Please contact administrator.",
          errorResponse.getMessage());

      // Verify correct HTTP status
      assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
    }
  }

  public static class TestOperations {

    @AuthorizationExpression(
        expression = "METALAKE::USE_CATALOG || METALAKE::OWNER",
        accessMetadataType = MetadataObject.Type.METALAKE)
    public Response testMethod(
        @AuthorizationMetadata(type = Entity.EntityType.METALAKE) String metalake) {
      return Utils.ok("ok");
    }
  }

  private static class MockGravitinoAuthorizer implements GravitinoAuthorizer {

    @Override
    public void initialize() {}

    @Override
    public boolean authorize(
        Principal principal,
        String metalake,
        MetadataObject metadataObject,
        Privilege.Name privilege) {
      return "tester".equals(principal.getName())
          && "testMetalake".equals(metalake)
          && metadataObject.type() == MetadataObject.Type.METALAKE
          && privilege == Privilege.Name.USE_CATALOG;
    }

    @Override
    public boolean deny(
        Principal principal,
        String metalake,
        MetadataObject metadataObject,
        Privilege.Name privilege) {
      return false;
    }

    @Override
    public boolean isOwner(Principal principal, String metalake, MetadataObject metadataObject) {
      return false;
    }

    @Override
    public boolean isServiceAdmin() {
      return false;
    }

    @Override
    public boolean isSelf(Entity.EntityType type, NameIdentifier nameIdentifier) {
      return true;
    }

    @Override
    public boolean isMetalakeUser(String metalake) {
      return true;
    }

    @Override
    public boolean hasSetOwnerPermission(String metalake, String type, String fullName) {
      return true;
    }

    @Override
    public boolean hasMetadataPrivilegePermission(String metalake, String type, String fullName) {
      return true;
    }

    @Override
    public void handleRolePrivilegeChange(Long roleId) {}

    @Override
    public void handleMetadataOwnerChange(
        String metalake, Long oldOwnerId, NameIdentifier nameIdentifier, Entity.EntityType type) {}

    @Override
    public void close() throws IOException {}
  }
}
