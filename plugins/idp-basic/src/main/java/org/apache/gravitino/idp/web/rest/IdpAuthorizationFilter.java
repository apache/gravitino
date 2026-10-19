/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.idp.web.rest;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import org.apache.gravitino.Configs;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.idp.web.IdpManagement;
import org.apache.gravitino.idp.web.IdpRESTUtils;
import org.apache.gravitino.utils.PrincipalUtils;

/**
 * Enforces built-in IdP management API access rules without server interception.
 *
<<<<<<< HEAD
 * <p>Caller must be listed in {@link Configs#SERVICE_ADMINS}. This filter runs as a Jersey request
 * filter after the servlet {@code AuthenticationFilter} has authenticated the caller and populated
 * the current user principal.
=======
 * <p>Callers listed in {@link Configs#SERVICE_ADMINS} may perform all IdP management operations.
 * Authenticated non-admin users may {@code GET} {@code /idp/users/{user}} or {@code PUT} {@code
 * /idp/users/{user}/password} only for their own username. Other IdP management APIs remain
 * service-admin only.
 *
 * <p>This filter runs as a Jersey request filter after the servlet {@code AuthenticationFilter} has
 * authenticated the caller and populated the current user principal.
>>>>>>> d7e8b22ca ([#13925] improvement(idp-basic): Require currentPassword for self password change (#13926))
 *
 * <p>Registered only when {@code basic} is configured in {@link Configs#AUTHENTICATORS}. Scoped to
 * resources annotated with {@link IdpManagement} via Jersey name binding.
 */
@IdpManagement
public class IdpAuthorizationFilter implements ContainerRequestFilter {

  /** Error message when the caller is not a service admin. */
  public static final String SERVICE_ADMIN_REQUIRED_MESSAGE =
      "Only service admins can manage built-in IdP users and groups.";

  private final Supplier<List<String>> serviceAdminsSupplier;
  private final Supplier<String> currentUserSupplier;

  /** Creates a filter backed by the running Gravitino server configuration. */
  public IdpAuthorizationFilter() {
    this(
        () -> GravitinoEnv.getInstance().config().get(Configs.SERVICE_ADMINS),
        PrincipalUtils::getCurrentUserName);
  }

  IdpAuthorizationFilter(
      Supplier<List<String>> serviceAdminsSupplier, Supplier<String> currentUserSupplier) {
    this.serviceAdminsSupplier = serviceAdminsSupplier;
    this.currentUserSupplier = currentUserSupplier;
  }

  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {
    if (!isServiceAdmin(serviceAdminsSupplier.get(), currentUserSupplier.get())) {
      requestContext.abortWith(IdpRESTUtils.forbidden(SERVICE_ADMIN_REQUIRED_MESSAGE, null));
    }
  }

  static boolean isServiceAdmin(List<String> serviceAdmins, String currentUser) {
    return currentUser != null && serviceAdmins != null && serviceAdmins.contains(currentUser);
  }
<<<<<<< HEAD
=======

  /**
   * Returns whether the request is a self-service user access: {@code GET /idp/users/{currentUser}}
   * or {@code PUT /idp/users/{currentUser}/password}.
   */
  static boolean isSelfUserAccess(ContainerRequestContext requestContext, String currentUser) {
    if (currentUser == null) {
      return false;
    }
    List<PathSegment> segments = requestContext.getUriInfo().getPathSegments();
    if (segments.size() < 3
        || !"idp".equals(segments.get(0).getPath())
        || !"users".equals(segments.get(1).getPath())
        || !currentUser.equals(segments.get(2).getPath())) {
      return false;
    }
    String method = requestContext.getMethod();
    if ("GET".equalsIgnoreCase(method) && segments.size() == 3) {
      return true;
    }
    return "PUT".equalsIgnoreCase(method)
        && segments.size() == 4
        && "password".equals(segments.get(3).getPath());
  }
>>>>>>> d7e8b22ca ([#13925] improvement(idp-basic): Require currentPassword for self password change (#13926))
}
