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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.lance.common.ops.LanceCatalogService;

/** Basic metadata endpoints for Lance tables. */
@Path("/v1/metadata")
@Produces(MediaType.APPLICATION_JSON)
public class LanceMetadataOperations {

  private final LanceCatalogService catalogService;

  public LanceMetadataOperations(LanceCatalogService catalogService) {
    this.catalogService = catalogService;
  }

  @GET
  public Response describeCatalog() {
    Map<String, Object> payload = new HashMap<>();
    payload.put("catalog", catalogService.catalogName());
    payload.put("namespaces", catalogService.listNamespaceNames());
    payload.put("properties", catalogService.listNamespaces());
    return Response.ok(payload).build();
  }

  @GET
  @Path("/table")
  public Response loadTable(
      @QueryParam("namespace") String namespace, @QueryParam("name") String tableName) {
    if (namespace == null || namespace.trim().isEmpty()) {
      throw new BadRequestException("namespace query parameter is required");
    }
    if (tableName == null || tableName.trim().isEmpty()) {
      throw new BadRequestException("table name query parameter is required");
    }
    Optional<Map<String, Object>> table =
        catalogService.loadTable(namespace.trim(), tableName.trim());
    if (table.isPresent()) {
      return Response.ok(table.get()).build();
    }
    return Response.status(Response.Status.NOT_FOUND)
        .entity(
            Map.of(
                "error", "table not found",
                "namespace", namespace.trim(),
                "name", tableName.trim()))
        .build();
  }
}
