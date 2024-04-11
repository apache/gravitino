/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.filter;

import com.datastrato.gravitino.UserPrincipal;
import com.datastrato.gravitino.authorization.AccessControlManager;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.container.ContainerRequestContext;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class TestVersionAuthorizationFilter {

    private static final AccessControlManager accessControlManager = mock(AccessControlManager.class);
    private static final ContainerRequestContext requestContext = mock(ContainerRequestContext.class);
    private static final HttpServletRequest httpRequest = mock(HttpServletRequest.class);

    @Test
    public void testVersionAuthorizationFilter() throws IOException {
        VersionAuthorizationFilter filter = new VersionAuthorizationFilter();
        filter.setHttpRequest(httpRequest);
        filter.setAccessControlManager(accessControlManager);

        when(httpRequest.getAttribute(any())).thenReturn(new UserPrincipal("user"));
        when(accessControlManager.isServiceAdmin(any())).thenReturn(false);

        filter.filter(requestContext);
        verify(requestContext).abortWith(any());

        reset(requestContext);
        when(accessControlManager.isServiceAdmin(any())).thenReturn(true);
        filter.filter(requestContext);
        verify(requestContext, never()).abortWith(any());
    }
}
