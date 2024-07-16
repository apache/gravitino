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

package org.apache.gravitino.server.web.filter;

import static javax.servlet.http.HttpServletResponse.SC_METHOD_NOT_ALLOWED;

import java.io.IOException;
import java.util.Collections;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;
import org.apache.gravitino.Configs;
import org.apache.gravitino.server.authorization.NameBindings;

/**
 * AccessControlNotAllowedFilter is used for filter the requests related to access control if Apache
 * Gravitino doesn't enable authorization. The filter return 405 error code. You can refer to
 * https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/405. No methods will be returned in the
 * allow methods.
 */
@Provider
@NameBindings.AccessControlInterfaces
public class AccessControlNotAllowedFilter implements ContainerRequestFilter {

  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {
    requestContext.abortWith(
        Response.status(
                SC_METHOD_NOT_ALLOWED,
                String.format(
                    "You should set '%s' to true in the server side `gravitino.conf`"
                        + " to enable the authorization of the system, otherwise these interfaces can't work.",
                    Configs.ENABLE_AUTHORIZATION.getKey()))
            .allow(Collections.emptySet())
            .build());
  }
}
