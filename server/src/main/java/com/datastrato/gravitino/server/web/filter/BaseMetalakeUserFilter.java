/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.filter;

import com.datastrato.gravitino.GravitinoEnv;
import com.datastrato.gravitino.auth.AuthConstants;
import com.datastrato.gravitino.authorization.AccessControlManager;
import com.google.common.annotations.VisibleForTesting;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Context;
import java.io.IOException;
import java.security.Principal;

/*
*  It's a common operation to check whether the user is in the metalake.
*  This is a base class to provide the ability.
* */
public abstract class BaseMetalakeUserFilter implements ContainerRequestFilter {

    @Context
    protected HttpServletRequest httpRequest;

    protected AccessControlManager accessControlManager =
            GravitinoEnv.getInstance().accessControlManager();

    protected boolean checkUserInMetalake(ContainerRequestContext requestContext) throws IOException {
        Principal principal =
                (Principal) httpRequest.getAttribute(AuthConstants.AUTHENTICATED_PRINCIPAL_ATTRIBUTE_NAME);

        String metalake = requestContext.getUriInfo().getPathParameters().getFirst("metalake");

        return accessControlManager.isUserInMetalake(principal.getName(), metalake);
    }

    @VisibleForTesting
    void setHttpRequest(HttpServletRequest httpRequest) {
        this.httpRequest = httpRequest;
    }

    @VisibleForTesting
    void setAccessControlManager(AccessControlManager accessControlManager) {
        this.accessControlManager = accessControlManager;
    }
}
