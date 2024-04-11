/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.filter;

import static javax.servlet.http.HttpServletResponse.SC_FORBIDDEN;

import com.datastrato.gravitino.GravitinoEnv;
import com.datastrato.gravitino.auth.AuthConstants;
import com.datastrato.gravitino.authorization.AccessControlManager;
import com.datastrato.gravitino.server.authorization.NameBindings;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.security.Principal;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;

/**
 * From a security point of view knowing the version will tell you what security issues it has.
 * Gravitino may still want to restrict this, so Gravitino just allows service admin to use it.
 */
@Provider
@NameBindings.VersionInterfaces
public class VersionAuthorizationFilter implements ContainerRequestFilter {

  @Context private HttpServletRequest httpRequest;

  private AccessControlManager accessControlManager =
      GravitinoEnv.getInstance().accessControlManager();

  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {

    Principal principal =
        (Principal) httpRequest.getAttribute(AuthConstants.AUTHENTICATED_PRINCIPAL_ATTRIBUTE_NAME);

    if (!accessControlManager.isServiceAdmin(principal.getName())) {
      requestContext.abortWith(
          Response.status(SC_FORBIDDEN, "Only service admins can manage metalake admins").build());
    }
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
