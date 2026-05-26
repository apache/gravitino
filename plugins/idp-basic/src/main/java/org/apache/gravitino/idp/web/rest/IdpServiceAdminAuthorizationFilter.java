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
import java.lang.reflect.Method;
import java.util.List;
import java.util.function.Supplier;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.Context;
import javax.ws.rs.ext.Provider;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Configs;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.idp.web.IdpRestUtils;

/**
 * Enforces built-in IdP management API access rules without server interception:
 *
 * <ul>
 *   <li>Available only when {@code basic} is configured in {@link Configs#AUTHENTICATORS}
 *   <li>Caller must be a service admin via {@link GravitinoAuthorizer#isServiceAdmin()}
 * </ul>
 */
@Provider
public class IdpServiceAdminAuthorizationFilter implements ContainerRequestFilter {

  /** Authenticator name that enables built-in IdP management APIs. */
  public static final String BASIC_AUTHENTICATOR = "basic";

  /** Authorization expression that matches the server-side service admin checks. */
  public static final String SERVICE_ADMIN_AUTHORIZATION_EXPRESSION = "SERVICE_ADMIN";

  /** Error message when the caller is not a service admin. */
  public static final String SERVICE_ADMIN_REQUIRED_MESSAGE =
      "Only service admins can manage built-in IdP users and groups.";

  /** Error message when the {@code basic} authenticator is not enabled. */
  public static final String BASIC_AUTHENTICATOR_REQUIRED_MESSAGE =
      "Built-in IdP management APIs are available only when the basic authenticator is enabled.";

  private final Supplier<List<String>> authenticatorsSupplier;
  private final Supplier<GravitinoAuthorizer> authorizerSupplier;
  private final Supplier<Method> resourceMethodSupplier;

  @Context private ResourceInfo resourceInfo;

  /** Creates a filter backed by the running Gravitino server configuration and authorizer. */
  public IdpServiceAdminAuthorizationFilter() {
    this.authenticatorsSupplier =
        () -> GravitinoEnv.getInstance().config().get(Configs.AUTHENTICATORS);
    this.authorizerSupplier = IdpServiceAdminAuthorizationFilter::resolveAuthorizer;
    this.resourceMethodSupplier = this::resolveResourceMethod;
  }

  IdpServiceAdminAuthorizationFilter(
      Supplier<List<String>> authenticatorsSupplier,
      Supplier<GravitinoAuthorizer> authorizerSupplier,
      Supplier<Method> resourceMethodSupplier) {
    this.authenticatorsSupplier = authenticatorsSupplier;
    this.authorizerSupplier = authorizerSupplier;
    this.resourceMethodSupplier = resourceMethodSupplier;
  }

  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {
    String path = requestContext.getUriInfo().getPath();
    if (!isIdpManagementPath(path)) {
      return;
    }

    if (!isBasicAuthenticatorEnabled(authenticatorsSupplier.get())) {
      requestContext.abortWith(
          IdpRestUtils.notFound(
              BASIC_AUTHENTICATOR_REQUIRED_MESSAGE,
              new org.apache.gravitino.idp.exception.NotFoundException(
                  BASIC_AUTHENTICATOR_REQUIRED_MESSAGE)));
      return;
    }

    GravitinoAuthorizer authorizer = authorizerSupplier.get();
    if (authorizer == null || !isAuthorized(authorizer)) {
      String message = resolveAuthorizationFailureMessage();
      requestContext.abortWith(IdpRestUtils.forbidden(message, null));
    }
  }

  private static boolean isIdpManagementPath(String path) {
    return path != null && (path.equals("idp") || path.startsWith("idp/"));
  }

  private static boolean isBasicAuthenticatorEnabled(List<String> authenticators) {
    return authenticators != null && authenticators.contains(BASIC_AUTHENTICATOR);
  }

  private boolean isAuthorized(GravitinoAuthorizer authorizer) {
    IdpAuthorizationExpression expression = resolveAuthorizationExpression();
    if (expression == null) {
      return authorizer.isServiceAdmin();
    }

    return IdpAuthorizationExpressionEvaluator.evaluate(expression, authorizer);
  }

  private IdpAuthorizationExpression resolveAuthorizationExpression() {
    Method method = resourceMethodSupplier.get();
    return method == null ? null : method.getAnnotation(IdpAuthorizationExpression.class);
  }

  private Method resolveResourceMethod() {
    return resourceInfo == null ? null : resourceInfo.getResourceMethod();
  }

  private String resolveAuthorizationFailureMessage() {
    IdpAuthorizationExpression expression = resolveAuthorizationExpression();
    if (expression != null && StringUtils.isNotBlank(expression.errorMessage())) {
      return expression.errorMessage();
    }
    return SERVICE_ADMIN_REQUIRED_MESSAGE;
  }

  private static GravitinoAuthorizer resolveAuthorizer() {
    return GravitinoEnv.getInstance().gravitinoAuthorizer();
  }
}
