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
import java.util.Map;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.CatalogDispatcher;
import org.apache.gravitino.dto.responses.CatalogCredentialsResponse;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;
import org.apache.gravitino.server.web.Utils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * REST endpoint that exposes the sensitive credential properties of a catalog (e.g. {@code
 * jdbc-password}, cloud storage secret keys). These properties are intentionally omitted from the
 * standard catalog-info response ({@code GET /metalakes/{m}/catalogs/{c}}) to prevent unintentional
 * exposure. Engine connectors (Trino, Spark) that require these values must call this endpoint with
 * appropriate catalog-level ownership permissions.
 */
@Path("/metalakes/{metalake}/catalogs/{catalog}/credentials")
public class CatalogCredentialOperations {

  private static final Logger LOG = LoggerFactory.getLogger(CatalogCredentialOperations.class);

  private final CatalogDispatcher catalogDispatcher;

  @Context private HttpServletRequest httpRequest;

  @Inject
  public CatalogCredentialOperations(CatalogDispatcher catalogDispatcher) {
    this.catalogDispatcher = catalogDispatcher;
  }

  /**
   * Returns the credential properties of the specified catalog. Requires catalog-level ownership or
   * metalake-level ownership — more restrictive than the regular catalog GET endpoint.
   *
   * @param metalakeName The metalake name.
   * @param catalogName The catalog name.
   * @return A {@link CatalogCredentialsResponse} containing the credential key-value pairs.
   */
  @GET
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "get-catalog-credentials." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "get-catalog-credentials", absolute = true)
  @AuthorizationExpression(
      expression = "ANY(OWNER, METALAKE, CATALOG)",
      accessMetadataType = MetadataObject.Type.CATALOG)
  public Response getCatalogCredentials(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalakeName,
      @PathParam("catalog") @AuthorizationMetadata(type = Entity.EntityType.CATALOG)
          String catalogName) {
    LOG.info(
        "Received get catalog credentials request for catalog: {}.{}", metalakeName, catalogName);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            NameIdentifier ident = NameIdentifierUtil.ofCatalog(metalakeName, catalogName);
            Map<String, String> credentials = catalogDispatcher.getCatalogCredentials(ident);
            Response response = Utils.ok(new CatalogCredentialsResponse(credentials));
            LOG.info(
                "Successfully fetched credentials for catalog: {}.{}", metalakeName, catalogName);
            return response;
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleCatalogException(
          OperationType.GET, catalogName, metalakeName, e);
    }
  }
}
