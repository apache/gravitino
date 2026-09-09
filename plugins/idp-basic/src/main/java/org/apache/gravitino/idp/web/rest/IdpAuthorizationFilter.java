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
import javax.ws.rs.core.PathSegment;
import org.apache.gravitino.Configs;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.idp.web.IdpManagement;
import org.apache.gravitino.idp.web.IdpRESTUtils;
import org.apache.gravitino.utils.PrincipalUtils;

/**
 * Enforces built-in IdP management API access rules without server interception.
 *
 * <p>Callers listed in {@link Configs#SERVICE_ADMINS} may perform all IdP management operations.
 * Authenticated non-admin users may {@code GET} or {@code PUT} {@code /idp/users/{user}} only for
 * their own username (read profile / change password). Other IdP management APIs remain
 * service-admin only.
 *
 * <p>This filter runs as a Jersey request filter after the servlet {@code AuthenticationFilter} has
 * authenticated the caller and populated the current user principal.
 *
 * <p>Registered only when {@code basic} is configured in {@link Configs#AUTHENTICATORS}. Scoped to
 * resources annotated with {@link IdpManagement} via Jersey name binding.
 */
@IdpManagement
public class IdpAuthorizationFilter implements ContainerRequestFilter {

  /** Error message when the caller is not allowed to perform the IdP management operation. */
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
    String currentUser = currentUserSupplier.get();
    if (isServiceAdmin(serviceAdminsSupplier.get(), currentUser)
        || isSelfUserAccess(requestContext, currentUser)) {
      return;
    }
    requestContext.abortWith(IdpRESTUtils.forbidden(SERVICE_ADMIN_REQUIRED_MESSAGE, null));
  }

  static boolean isServiceAdmin(List<String> serviceAdmins, String currentUser) {
    return currentUser != null && serviceAdmins != null && serviceAdmins.contains(currentUser);
  }

  /**
   * Returns whether the request is a GET or PUT to {@code /idp/users/{currentUser}}.
   *
   * <p>The resource method still rejects non-admin updates that change {@code enabled}.
   */
  static boolean isSelfUserAccess(ContainerRequestContext requestContext, String currentUser) {
    if (currentUser == null) {
      return false;
    }
    String method = requestContext.getMethod();
    if (!"GET".equalsIgnoreCase(method) && !"PUT".equalsIgnoreCase(method)) {
      return false;
    }
    List<PathSegment> segments = requestContext.getUriInfo().getPathSegments();
    return segments.size() == 3
        && "idp".equals(segments.get(0).getPath())
        && "users".equals(segments.get(1).getPath())
        && currentUser.equals(segments.get(2).getPath());
  }
}
