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
import javax.ws.rs.HttpMethod;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.Provider;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.server.authorization.NameBindings;
import org.apache.gravitino.server.web.Utils;
import org.apache.gravitino.server.web.rest.ExceptionHandlers;
import org.apache.gravitino.server.web.rest.OperationType;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Provider
@NameBindings.CatalogReadOnlyCheck
public class CatalogReadOnlyFilter implements ContainerRequestFilter {

  private static final Logger LOG = LoggerFactory.getLogger(CatalogReadOnlyFilter.class);

  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {
    MultivaluedMap<String, String> pathParameters = requestContext.getUriInfo().getPathParameters();
    String metalakeName = pathParameters.getFirst("metalake");
    String catalogName = pathParameters.getFirst("catalog");

    NameIdentifier ident = NameIdentifierUtil.ofCatalog(metalakeName, catalogName);
    try {
      CatalogManager.checkCatalogCanWrite(GravitinoEnv.getInstance().entityStore(), ident);

    } catch (Exception e) {
      LOG.info("CatalogReadOnlyFilter exception: {}", e.getMessage());
      if (HttpMethod.DELETE.equals(requestContext.getMethod()) && e instanceof NotFoundException) {
        requestContext.abortWith(Utils.ok(new DropResponse(false)));

      } else {
        requestContext.abortWith(
            ExceptionHandlers.handleCatalogException(
                OperationType.CHECK_READ_ONLY, catalogName, metalakeName, e));
      }
    }
  }
}
