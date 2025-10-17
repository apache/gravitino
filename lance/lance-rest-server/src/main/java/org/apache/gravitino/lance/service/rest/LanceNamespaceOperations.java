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
package org.apache.gravitino.lance.service.rest;

import java.util.NoSuchElementException;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.Encoded;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.lance.common.ops.LanceCatalogService;

@Path("/v1/namespace")
@Produces(MediaType.APPLICATION_JSON)
public class LanceNamespaceOperations {

  private final LanceCatalogService catalogService;

  public LanceNamespaceOperations(LanceCatalogService catalogService) {
    this.catalogService = catalogService;
  }

  @GET
  @Path("/{id}/list")
  public Response listNamespaces(
      @Encoded @PathParam("id") String namespaceId,
      @DefaultValue(".") @QueryParam("delimiter") String delimiter,
      @QueryParam("page_token") String pageToken,
      @QueryParam("limit") Integer limit) {
    try {
      LanceCatalogService.NamespaceListingResult result =
          catalogService.listChildNamespaces(namespaceId, delimiter, pageToken, limit);
      LanceListNamespacesResponse payload =
          new LanceListNamespacesResponse(
              result.getParentId(),
              result.getDelimiter(),
              result.getNamespaces(),
              result.getNextPageToken().orElse(null));
      return Response.ok(payload).build();
    } catch (NoSuchElementException nse) {
      throw new NotFoundException(nse.getMessage(), nse);
    } catch (IllegalArgumentException iae) {
      throw new BadRequestException(iae.getMessage(), iae);
    }
  }

  @GET
  @Path("/{id}/table/list")
  public Response listTables(
      @Encoded @PathParam("id") String namespaceId,
      @DefaultValue(".") @QueryParam("delimiter") String delimiter,
      @QueryParam("page_token") String pageToken,
      @QueryParam("limit") Integer limit) {
    try {
      LanceCatalogService.TableListingResult result =
          catalogService.listTables(namespaceId, delimiter, pageToken, limit);
      LanceListTablesResponse payload =
          new LanceListTablesResponse(
              result.getNamespaceId(),
              result.getDelimiter(),
              result.getTables(),
              result.getNextPageToken().orElse(null));
      return Response.ok(payload).build();
    } catch (NoSuchElementException nse) {
      throw new NotFoundException(nse.getMessage(), nse);
    } catch (IllegalArgumentException iae) {
      throw new BadRequestException(iae.getMessage(), iae);
    }
  }
}
