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

import java.io.IOException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.ext.Provider;
import org.apache.gravitino.server.web.Utils;

/** Hides built-in IdP endpoints when the basic authenticator is not enabled. */
@Provider
public class IdpInterfaceNotFoundFilter implements ContainerRequestFilter {

  static final String IDP_PATH_PREFIX = "idp";
  private static final String NOT_FOUND_TYPE = "NotFoundException";
  private static final String NOT_FOUND_MESSAGE = "The requested IdP interface does not exist.";

  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {
    String path = requestContext.getUriInfo().getPath(false);
    if (path.equals(IDP_PATH_PREFIX) || path.startsWith(IDP_PATH_PREFIX + "/")) {
      requestContext.abortWith(Utils.notFound(NOT_FOUND_TYPE, NOT_FOUND_MESSAGE));
    }
  }
}
