/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.filter;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastrato.gravitino.UserPrincipal;
import com.datastrato.gravitino.authorization.AccessControlManager;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.BaseMetalake;
import com.datastrato.gravitino.metalake.MetalakeManager;
import java.time.Instant;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestMetalakeAuthorizationFilter {

  private static final AccessControlManager accessControlManager =
      Mockito.mock(AccessControlManager.class);
  private static final MetalakeManager metalakeManager = Mockito.mock(MetalakeManager.class);
  private static final ContainerRequestContext requestContext =
      Mockito.mock(ContainerRequestContext.class);
  private static final HttpServletRequest httpRequest = Mockito.mock(HttpServletRequest.class);
  private static final BaseMetalake metalake = Mockito.mock(BaseMetalake.class);
  private static final UriInfo uriInfo = Mockito.mock(UriInfo.class);
  private static final MultivaluedMap pathParameters = Mockito.mock(MultivaluedMap.class);

  @Test
  public void testMetalakeAuthorizationForbidden() {
    MetalakeAuthorizationFilter filter = new MetalakeAuthorizationFilter();
    filter.setHttpRequest(httpRequest);
    filter.setMetalakeManager(metalakeManager);
    filter.setAccessControlManager(accessControlManager);

    // Test with user who isn't in the metalake to load the metalake
    reset(requestContext);
    when(httpRequest.getAttribute(any())).thenReturn(new UserPrincipal("user"));
    when(accessControlManager.isServiceAdmin(any())).thenReturn(false);
    when(metalakeManager.loadMetalake(any())).thenReturn(metalake);
    when(requestContext.getMethod()).thenReturn(HttpMethod.GET);
    when(requestContext.getUriInfo()).thenReturn(uriInfo);
    when(uriInfo.getPathParameters()).thenReturn(pathParameters);
    when(pathParameters.getFirst(any())).thenReturn("metalake");
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("user1").withCreateTime(Instant.now()).build();
    when(metalake.auditInfo()).thenReturn(auditInfo);

    assertDoesNotThrow(() -> filter.filter(requestContext));
    verify(requestContext).abortWith(any());

    // Test with user to list metalakes
    reset(requestContext);
    when(requestContext.getMethod()).thenReturn(HttpMethod.GET);
    when(requestContext.getUriInfo()).thenReturn(uriInfo);
    when(pathParameters.getFirst(any())).thenReturn(null);
    Mockito.when(accessControlManager.isServiceAdmin(any())).thenReturn(false);

    assertDoesNotThrow(() -> filter.filter(requestContext));
    verify(requestContext).abortWith(any());

    // Test with non-admin to create the metalake
    reset(requestContext);
    when(requestContext.getMethod()).thenReturn(HttpMethod.POST);
    when(requestContext.getUriInfo()).thenReturn(uriInfo);
    when(httpRequest.getAttribute(any())).thenReturn(new UserPrincipal("user"));
    when(accessControlManager.isServiceAdmin(any())).thenReturn(false);

    assertDoesNotThrow(() -> filter.filter(requestContext));
    verify(requestContext).abortWith(any());

    // Test with non-creator to alter/delete the metalake
    reset(requestContext);
    when(pathParameters.getFirst(any())).thenReturn("metalake");
    when(requestContext.getMethod()).thenReturn(HttpMethod.PUT);
    when(requestContext.getUriInfo()).thenReturn(uriInfo);

    assertDoesNotThrow(() -> filter.filter(requestContext));
    verify(requestContext).abortWith(any());

    reset(requestContext);
    when(requestContext.getMethod()).thenReturn(HttpMethod.DELETE);
    when(requestContext.getUriInfo()).thenReturn(uriInfo);

    assertDoesNotThrow(() -> filter.filter(requestContext));
    verify(requestContext).abortWith(any());
  }

  @Test
  public void testMetalakeAuthorizationPassed() {
    MetalakeAuthorizationFilter filter = new MetalakeAuthorizationFilter();
    filter.setHttpRequest(httpRequest);
    filter.setMetalakeManager(metalakeManager);
    filter.setAccessControlManager(accessControlManager);

    // Test to create a metalake
    reset(requestContext);
    when(requestContext.getMethod()).thenReturn(HttpMethod.POST);
    when(requestContext.getUriInfo()).thenReturn(uriInfo);
    when(httpRequest.getAttribute(any())).thenReturn(new UserPrincipal("user"));
    when(accessControlManager.isMetalakeAdmin(any())).thenReturn(true);

    assertDoesNotThrow(() -> filter.filter(requestContext));

    verify(requestContext, never()).abortWith(any());

    // Test to list metalakes
    reset(requestContext);
    when(requestContext.getMethod()).thenReturn(HttpMethod.GET);
    when(requestContext.getUriInfo()).thenReturn(uriInfo);
    when(uriInfo.getPathParameters()).thenReturn(pathParameters);
    when(pathParameters.getFirst(any())).thenReturn(null);

    assertDoesNotThrow(() -> filter.filter(requestContext));

    verify(requestContext, never()).abortWith(any());

    // Test to alter a metalake
    reset(requestContext);
    when(requestContext.getMethod()).thenReturn(HttpMethod.PUT);
    when(requestContext.getUriInfo()).thenReturn(uriInfo);
    when(pathParameters.getFirst(any())).thenReturn("metalake");
    when(metalakeManager.loadMetalake(any())).thenReturn(metalake);

    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("user").withCreateTime(Instant.now()).build();
    when(metalake.auditInfo()).thenReturn(auditInfo);

    assertDoesNotThrow(() -> filter.filter(requestContext));

    verify(requestContext, never()).abortWith(any());

    // Test to drop a metalake
    reset(requestContext);
    when(requestContext.getMethod()).thenReturn(HttpMethod.DELETE);
    when(requestContext.getUriInfo()).thenReturn(uriInfo);

    assertDoesNotThrow(() -> filter.filter(requestContext));
    verify(requestContext, never()).abortWith(any());

    // Test to load a metalake
    reset(requestContext);
    when(requestContext.getMethod()).thenReturn(HttpMethod.GET);
    when(requestContext.getUriInfo()).thenReturn(uriInfo);
    assertDoesNotThrow(() -> filter.filter(requestContext));
    verify(requestContext, never()).abortWith(any());
  }
}
