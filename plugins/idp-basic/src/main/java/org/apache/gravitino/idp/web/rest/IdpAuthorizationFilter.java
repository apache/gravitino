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
import javax.ws.rs.ext.Provider;
import org.apache.gravitino.Configs;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.idp.exception.NotFoundException;
import org.apache.gravitino.idp.web.IdpManagement;
import org.apache.gravitino.idp.web.IdpRestUtils;

/**
 * Enforces built-in IdP management API access rules without server interception:
 *
 * <ul>
 *   <li>Available only when {@code basic} is configured in {@link Configs#AUTHENTICATORS}
 *   <li>Caller must be a service admin via {@link GravitinoAuthorizer#isServiceAdmin()}
 * </ul>
 *
 * <p>Scoped to resources annotated with {@link IdpManagement} via Jersey name binding.
 */
@Provider
@IdpManagement
public class IdpAuthorizationFilter implements ContainerRequestFilter {

  /** Authenticator name that enables built-in IdP management APIs. */
  public static final String BASIC_AUTHENTICATOR = "basic";

  /** Error message when the caller is not a service admin. */
  public static final String SERVICE_ADMIN_REQUIRED_MESSAGE =
      "Only service admins can manage built-in IdP users and groups.";

  /** Error message when the {@code basic} authenticator is not enabled. */
  public static final String BASIC_AUTHENTICATOR_REQUIRED_MESSAGE =
      "Built-in IdP management APIs are available only when the basic authenticator is enabled.";

  private final Supplier<List<String>> authenticatorsSupplier;
  private final Supplier<GravitinoAuthorizer> authorizerSupplier;

  /** Creates a filter backed by the running Gravitino server configuration and authorizer. */
  public IdpAuthorizationFilter() {
    this(
        () -> GravitinoEnv.getInstance().config().get(Configs.AUTHENTICATORS),
        () -> GravitinoEnv.getInstance().gravitinoAuthorizer());
  }

  IdpAuthorizationFilter(
      Supplier<List<String>> authenticatorsSupplier,
      Supplier<GravitinoAuthorizer> authorizerSupplier) {
    this.authenticatorsSupplier = authenticatorsSupplier;
    this.authorizerSupplier = authorizerSupplier;
  }

  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {
    if (!basicAuthenticatorEnabled(authenticatorsSupplier.get())) {
      requestContext.abortWith(
          IdpRestUtils.notFound(
              BASIC_AUTHENTICATOR_REQUIRED_MESSAGE,
              new NotFoundException(BASIC_AUTHENTICATOR_REQUIRED_MESSAGE)));
      return;
    }

    GravitinoAuthorizer authorizer = authorizerSupplier.get();
    if (authorizer == null || !authorizer.isServiceAdmin()) {
      requestContext.abortWith(IdpRestUtils.forbidden(SERVICE_ADMIN_REQUIRED_MESSAGE, null));
    }
  }

  private static boolean basicAuthenticatorEnabled(List<String> authenticators) {
    return authenticators != null && authenticators.contains(BASIC_AUTHENTICATOR);
  }
}
