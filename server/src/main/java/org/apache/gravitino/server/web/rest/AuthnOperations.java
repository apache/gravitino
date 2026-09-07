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
package org.apache.gravitino.server.web.rest;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.authorization.AccessControlDispatcher;
import org.apache.gravitino.dto.responses.AuthMeResponse;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.server.web.Utils;
import org.apache.gravitino.utils.PrincipalUtils;

/**
 * Provides the authenticated principal information. This endpoint returns the server-resolved
 * principal name and service-administrator status, ensuring the UI uses server-resolved identity
 * and authorization information.
 */
@Path("/authn")
public class AuthnOperations {

  @Nullable private final AccessControlDispatcher accessControlDispatcher;

  @Context private HttpServletRequest httpRequest;

  /** Creates an authenticated-principal REST resource. */
  public AuthnOperations() {
    this(GravitinoEnv.getInstance().accessControlDispatcher());
  }

  AuthnOperations(@Nullable AccessControlDispatcher accessControlDispatcher) {
    this.accessControlDispatcher = accessControlDispatcher;
  }

  @GET
  @Path("me")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "authn-me." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "authn-me", absolute = true)
  public Response me() {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            String principal = PrincipalUtils.getCurrentUserName();
            boolean serviceAdmin =
                accessControlDispatcher != null
                    && accessControlDispatcher.isServiceAdmin(principal);
            return Utils.ok(new AuthMeResponse(principal, serviceAdmin));
          });
    } catch (Exception e) {
      return Utils.internalError(e.getMessage(), e);
    }
  }
}
