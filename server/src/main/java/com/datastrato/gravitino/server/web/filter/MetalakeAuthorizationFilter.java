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
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.security.Principal;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;

/**
 * MetalakeAuthorizationFilter is responsible to manage the privileges of the metalake. First, only
 * metalake admins can create the metalake or list metalakes. Second, only metalake admins can alter
 * or drop the metalake which he created. Third, all the users of the metalake can load the
 * metalake.
 */
@Provider
@NameBindings.MetalakeInterfaces
public class MetalakeAuthorizationFilter implements ContainerRequestFilter {

  @Context private HttpServletRequest httpRequest;

  private AccessControlManager accessControlManager =
      GravitinoEnv.getInstance().accessControlManager();
  private MetalakeManager metalakeManager = GravitinoEnv.getInstance().metalakesManager();

  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {
    Principal principal =
        (Principal) httpRequest.getAttribute(AuthConstants.AUTHENTICATED_PRINCIPAL_ATTRIBUTE_NAME);

    String method = requestContext.getMethod();
    if (HttpMethod.POST.equalsIgnoreCase(method)) {
      if (!accessControlManager.isMetalakeAdmin(principal.getName())) {
        requestContext.abortWith(
            Response.status(SC_FORBIDDEN, "Only metalake admins can create metalake").build());
      }
    } else {
      try {
        String metalake = requestContext.getUriInfo().getPathParameters().getFirst("metalake");

        if (HttpMethod.GET.equalsIgnoreCase(method)) {
          // For list metalakes operation
          if (metalake == null) {
            if (!accessControlManager.isMetalakeAdmin(principal.getName())) {
              requestContext.abortWith(
                  Response.status(SC_FORBIDDEN, "Only metalake admins can list metalakes").build());
            }
          } else {
            // For load a metalake operation
            String creator =
                metalakeManager
                    .loadMetalake(NameIdentifier.ofMetalake(metalake))
                    .auditInfo()
                    .creator();
            if (!principal.getName().equals(creator)
                && !accessControlManager.isUserInMetalake(principal.getName(), metalake)) {
              requestContext.abortWith(
                  Response.status(
                          SC_FORBIDDEN, "Only the users in the metalake can load the metalake")
                      .build());
            }
          }
        } else {
          String creator =
              metalakeManager
                  .loadMetalake(NameIdentifier.ofMetalake(metalake))
                  .auditInfo()
                  .creator();

          if (!principal.getName().equals(creator)) {
            String operation = HttpMethod.PUT.equalsIgnoreCase(method) ? "alter" : "delete";
            requestContext.abortWith(
                Response.status(SC_FORBIDDEN, "Only the creator can " + operation + " the metalake")
                    .build());
          }
        }

      } catch (NoSuchMetalakeException nsm) {
        // If a metalake doesn't exist, authorization filter shouldn't block it, let the operations
        // related to access control to handle it.
      }
    }
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
}
