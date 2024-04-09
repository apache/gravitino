/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.filter;

import static javax.servlet.http.HttpServletResponse.SC_FORBIDDEN;

import com.datastrato.gravitino.GravitinoEnv;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.auth.AuthConstants;
import com.datastrato.gravitino.authorization.AccessControlManager;
import com.datastrato.gravitino.exceptions.NoSuchMetalakeException;
import com.datastrato.gravitino.metalake.MetalakeManager;
import com.datastrato.gravitino.server.authorization.NameBindings;
import com.datastrato.gravitino.server.web.rest.MetalakeAdminOperations;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.security.Principal;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;

/**
 * AccessControlAuthorizationFilter is used for filter access control requests. First, only service
 * admins can manage metalake admins. Second, only metalake admins can manage access control of his
 * metalake. For some SaaS services, an organization is created by one user, the user will bind to a
 * credit card to pay for this service, and only the user can manage access control of the
 * organization. Maybe Gravitino can support one metalake is managed by multiple admins in the
 * future. But Gravitino prefers adopting a cautious style now. Some operations like `getUser`,
 * `getGroup` , `getRole` aren't allowed for non-admins. Because admin should be responsible to
 * manage access control, non-admins can't do more things though they have the privileges.
 */
@Provider
@NameBindings.AccessControlInterfaces
public class AccessControlAuthorizationFilter implements ContainerRequestFilter {

  @Context private HttpServletRequest httpRequest;
  @Context private ResourceInfo resourceInfo;

  private AccessControlManager accessControlManager =
      GravitinoEnv.getInstance().accessControlManager();
  private MetalakeManager metalakeManager = GravitinoEnv.getInstance().metalakesManager();

  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {
    Principal principal =
        (Principal) httpRequest.getAttribute(AuthConstants.AUTHENTICATED_PRINCIPAL_ATTRIBUTE_NAME);

    if (isManageMetalakeAminOperation()) {
      if (!accessControlManager.isServiceAdmin(principal.getName())) {
        requestContext.abortWith(
            Response.status(SC_FORBIDDEN, "Only service admins can manage metalake admins")
                .build());
      }

    } else {

      if (!accessControlManager.isMetalakeAdmin(principal.getName())) {
        requestContext.abortWith(
            Response.status(SC_FORBIDDEN, "Only metalake admins can use access control interfaces")
                .build());
        return;
      }

      String metalake = requestContext.getUriInfo().getPathParameters().getFirst("metalake");

      try {
        if (!principal
            .getName()
            .equals(
                metalakeManager
                    .loadMetalake(NameIdentifier.ofMetalake(metalake))
                    .auditInfo()
                    .creator())) {
          requestContext.abortWith(
              Response.status(
                      SC_FORBIDDEN,
                      "Only the creator can use access control interfaces of this metalake")
                  .build());
        }
      } catch (NoSuchMetalakeException nsm) {
        // If a metalake doesn't exist, authorization filter shouldn't block it, let the operations
        // related to access control to handle it.
      }
    }
  }

  private boolean isManageMetalakeAminOperation() {
    return resourceInfo.getResourceClass() == MetalakeAdminOperations.class;
  }

  @VisibleForTesting
  void setAccessControlManager(AccessControlManager accessControlManager) {
    this.accessControlManager = accessControlManager;
  }

  @VisibleForTesting
  void setMetalakeManager(MetalakeManager metalakeManager) {
    this.metalakeManager = metalakeManager;
  }

  @VisibleForTesting
  void setHttpRequest(HttpServletRequest httpRequest) {
    this.httpRequest = httpRequest;
  }

  @VisibleForTesting
  void setResourceInfo(ResourceInfo resourceInfo) {
    this.resourceInfo = resourceInfo;
  }
}
