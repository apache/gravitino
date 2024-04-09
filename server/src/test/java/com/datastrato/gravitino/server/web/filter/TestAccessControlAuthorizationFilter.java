/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.filter;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastrato.gravitino.UserPrincipal;
import com.datastrato.gravitino.authorization.AccessControlManager;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.BaseMetalake;
import com.datastrato.gravitino.metalake.MetalakeManager;
import com.datastrato.gravitino.server.web.rest.MetalakeAdminOperations;
import com.datastrato.gravitino.server.web.rest.UserOperations;
import java.io.IOException;
import java.lang.reflect.Method;
import java.time.Instant;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestAccessControlAuthorizationFilter {

  private static final AccessControlManager accessControlManager = mock(AccessControlManager.class);
  private static final MetalakeManager metalakeManager = mock(MetalakeManager.class);
  private static final ContainerRequestContext requestContext = mock(ContainerRequestContext.class);
  private static final HttpServletRequest httpRequest = mock(HttpServletRequest.class);
  private static final BaseMetalake metalake = mock(BaseMetalake.class);
  private static final UriInfo uriInfo = mock(UriInfo.class);
  private static final MultivaluedMap pathParameters = mock(MultivaluedMap.class);

  private static Class<?> expectedClass;

  // Mock method can't return a value with type Class<?>, so we use create a stub class to test.
  private static final ResourceInfo resourceInfo =
      new ResourceInfo() {
        @Override
        public Method getResourceMethod() {
          return null;
        }

        @Override
        public Class<?> getResourceClass() {
          return expectedClass;
        }
      };

  @Test
  public void TestAccessControlAuthorizationForbidden() {
    AccessControlAuthorizationFilter filter = new AccessControlAuthorizationFilter();
    filter.setHttpRequest(httpRequest);
    filter.setResourceInfo(resourceInfo);
    filter.setMetalakeManager(metalakeManager);
    filter.setAccessControlManager(accessControlManager);
    filter.setResourceInfo(resourceInfo);

    // Test with  forbidden service admin operation
    expectedClass = MetalakeAdminOperations.class;
    when(httpRequest.getAttribute(any())).thenReturn(new UserPrincipal("user"));
    when(accessControlManager.isServiceAdmin(any())).thenReturn(false);
    Assertions.assertDoesNotThrow(() -> filter.filter(requestContext));
    verify(requestContext).abortWith(any());

    // Test with forbidden user operations if user is not metalake admin
    reset(requestContext);
    expectedClass = UserOperations.class;
    when(httpRequest.getAttribute(any())).thenReturn(new UserPrincipal("user"));
    when(accessControlManager.isMetalakeAdmin(any())).thenReturn(false);
    Assertions.assertDoesNotThrow(() -> filter.filter(requestContext));
    verify(requestContext).abortWith(any());

    // Test with forbidden user operations if user is not creator
    reset(requestContext);
    expectedClass = UserOperations.class;
    when(httpRequest.getAttribute(any())).thenReturn(new UserPrincipal("user"));
    when(accessControlManager.isMetalakeAdmin(any())).thenReturn(true);
    when(metalakeManager.loadMetalake(any())).thenReturn(metalake);
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("user-not").withCreateTime(Instant.now()).build();
    when(metalake.auditInfo()).thenReturn(auditInfo);
    when(requestContext.getUriInfo()).thenReturn(uriInfo);
    when(uriInfo.getPathParameters()).thenReturn(pathParameters);
    when(pathParameters.getFirst(any())).thenReturn("metalake");

    Assertions.assertDoesNotThrow(() -> filter.filter(requestContext));
    verify(requestContext).abortWith(any());
    verify(requestContext).getUriInfo();
  }

  @Test
  public void TestAccessControlAuthorizationPassed() throws IOException {
    AccessControlAuthorizationFilter filter = new AccessControlAuthorizationFilter();
    filter.setHttpRequest(httpRequest);
    filter.setResourceInfo(resourceInfo);
    filter.setMetalakeManager(metalakeManager);
    filter.setAccessControlManager(accessControlManager);
    filter.setResourceInfo(resourceInfo);

    // Test with service admin operations
    expectedClass = MetalakeAdminOperations.class;
    when(httpRequest.getAttribute(any())).thenReturn(new UserPrincipal("user"));
    when(accessControlManager.isServiceAdmin(any())).thenReturn(true);

    Assertions.assertDoesNotThrow(() -> filter.filter(requestContext));
    verify(requestContext, never()).abortWith(any());

    // Test with metalake admin operations
    reset(requestContext);

    expectedClass = UserOperations.class;
    when(httpRequest.getAttribute(any())).thenReturn(new UserPrincipal("user"));
    when(accessControlManager.isMetalakeAdmin(any())).thenReturn(true);
    when(metalakeManager.loadMetalake(any())).thenReturn(metalake);
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("user").withCreateTime(Instant.now()).build();
    when(metalake.auditInfo()).thenReturn(auditInfo);
    when(requestContext.getUriInfo()).thenReturn(uriInfo);
    when(uriInfo.getPathParameters()).thenReturn(pathParameters);
    when(pathParameters.getFirst(any())).thenReturn("metalake");

    Assertions.assertDoesNotThrow(() -> filter.filter(requestContext));
    verify(requestContext, never()).abortWith(any());
  }
}
