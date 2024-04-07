/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web;

import static javax.servlet.http.HttpServletResponse.SC_METHOD_NOT_ALLOWED;
import static org.mockito.ArgumentMatchers.any;

import java.io.IOException;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

public class TestAccessControlNotAllowedFilter {

  @Test
  public void testAccessControlPath() {
    AccessControlNotAllowedFilter filter = new AccessControlNotAllowedFilter();
    Assertions.assertTrue(filter.isAccessControlPath("/api/admins"));
    Assertions.assertTrue(filter.isAccessControlPath("/api/admins/"));
    Assertions.assertFalse(filter.isAccessControlPath("/api/metalakes/"));
    Assertions.assertFalse(filter.isAccessControlPath("/api/metalakes/metalake"));
    Assertions.assertFalse(filter.isAccessControlPath("/api/metalakes/metalake/"));
    Assertions.assertTrue(filter.isAccessControlPath("/api/metalakes/metalake/users"));
    Assertions.assertTrue(filter.isAccessControlPath("/api/metalakes/metalake/users/"));
    Assertions.assertTrue(filter.isAccessControlPath("/api/metalakes/metalake/users/user"));
    Assertions.assertTrue(filter.isAccessControlPath("/api/metalakes/metalake/users/userRandom/"));
    Assertions.assertTrue(filter.isAccessControlPath("/api/metalakes/metalake/groups"));
    Assertions.assertTrue(filter.isAccessControlPath("/api/metalakes/metalake/groups/"));
    Assertions.assertTrue(filter.isAccessControlPath("/api/metalakes/metalake/groups/group1"));
    Assertions.assertTrue(
        filter.isAccessControlPath("/api/metalakes/metalake/groups/groupRandom/"));
    Assertions.assertFalse(filter.isAccessControlPath("/api/metalakes/metalake/catalogs"));
    Assertions.assertFalse(filter.isAccessControlPath("/api/metalakes/metalake/catalogs/"));
    Assertions.assertFalse(filter.isAccessControlPath("/api/metalakes/metalake/catalogs/catalog1"));
  }

  @Test
  public void testAccessControlNotAllowed() throws ServletException, IOException {
    AccessControlNotAllowedFilter filter = new AccessControlNotAllowedFilter();
    FilterChain mockChain = Mockito.mock(FilterChain.class);
    HttpServletRequest mockRequest = Mockito.mock(HttpServletRequest.class);
    HttpServletResponse mockResponse = Mockito.mock(HttpServletResponse.class);

    Mockito.when(mockRequest.getRequestURI()).thenReturn("/api/admins/");

    filter.doFilter(mockRequest, mockResponse, mockChain);
    Mockito.verify(mockChain, Mockito.never()).doFilter(any(), any());
    InOrder order = Mockito.inOrder(mockResponse);
    order.verify(mockResponse).setHeader("Allow", "");
    order
        .verify(mockResponse)
        .sendError(
            SC_METHOD_NOT_ALLOWED,
            "You should set 'gravitino.authorization.enable'"
                + " to true in the server side `gravitino.conf`"
                + " to enable the authorization of the system, otherwise these interfaces can't work.");
    order.verifyNoMoreInteractions();
  }
}
