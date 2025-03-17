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
package org.apache.gravitino.server.web.auth;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.Provider;
import org.apache.gravitino.server.web.auth.annotations.AuthorizeApi;
import org.apache.gravitino.server.web.auth.annotations.AuthorizeResource;

@Provider
public class AuthorizeFilter implements ContainerRequestFilter {

  GravitinoAuthorizer gravitinoAuthorizer =
      GravitinoAuthorizerProvider.INSTANCE.getGravitinoAuthorizer();

  @Context private ResourceInfo resourceInfo;

  @Override
  public void filter(ContainerRequestContext containerRequestContext) throws IOException {
    Method resourceMethod = resourceInfo.getResourceMethod();
    AuthorizeApi authorizeApi = resourceMethod.getAnnotation(AuthorizeApi.class);
    if (authorizeApi == null) {
      return;
    }
    String privilege = authorizeApi.privilege();
    Parameter[] parameters = resourceMethod.getParameters();
    MultivaluedMap<String, String> queryParameters =
        containerRequestContext.getUriInfo().getQueryParameters();
    Map<String, Object> resourceContext = getResourceContext(parameters, queryParameters);
    gravitinoAuthorizer.authorize(
        "3606534323078438382",
        String.valueOf(resourceContext.get("metalake")),
        String.valueOf(resourceContext.get(authorizeApi.resourceType())),
        privilege);
  }

  private Map<String, Object> getResourceContext(
      Parameter[] parameters, MultivaluedMap<String, String> queryParameters) {
    Map<String, Object> resourceContext = new HashMap<>();
    for (Parameter parameter : parameters) {
      AuthorizeResource authorizeResource = parameter.getAnnotation(AuthorizeResource.class);
      if (authorizeResource == null) {
        continue;
      }
      String resourceName = authorizeResource.value();
      String name = parameter.getName();
      resourceContext.put(resourceName, queryParameters.get(name));
    }
    return resourceContext;
  }
}
