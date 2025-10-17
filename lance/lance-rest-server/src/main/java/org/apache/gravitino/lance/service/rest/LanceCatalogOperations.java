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
import java.util.List;
import java.util.Map;
import javax.ws.rs.BadRequestException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.lance.common.ops.LanceCatalogService;

/** Basic catalog endpoints mirroring the Iceberg REST surface, currently stubbed out. */
@Path("/v1/catalog")
@Produces(MediaType.APPLICATION_JSON)
public class LanceCatalogOperations {

  private final LanceCatalogService catalogService;

  public LanceCatalogOperations(LanceCatalogService catalogService) {
    this.catalogService = catalogService;
  }

  @GET
  public Response describeCatalog() {
    Map<String, Object> payload = new HashMap<>();
    payload.put("name", catalogService.catalogName());
    payload.put("namespaces", catalogService.listNamespaceNames());
    return Response.ok(payload).build();
  }

  @GET
  @Path("/namespaces")
  public Response listNamespaces() {
    Map<String, Object> payload = new HashMap<>();
    payload.put("namespaces", catalogService.listNamespaceNames());
    payload.put("properties", catalogService.listNamespaces());
    return Response.ok(payload).build();
  }

  @POST
  @Path("/namespaces")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response createNamespace(Map<String, String> request) {
    if (request == null) {
      throw new BadRequestException("request body is required");
    }
    String namespace = request.getOrDefault("namespace", "").trim();
    if (namespace.isEmpty()) {
      throw new BadRequestException("namespace is required");
    }
    boolean created = catalogService.createNamespace(namespace);
    Response.Status status = created ? Response.Status.CREATED : Response.Status.OK;
    return Response.status(status)
        .entity(Map.of("namespace", namespace, "created", created))
        .build();
  }

  @DELETE
  @Path("/namespaces/{namespace}")
  public Response dropNamespace(@PathParam("namespace") String namespace) {
    if (namespace == null || namespace.trim().isEmpty()) {
      throw new BadRequestException("namespace is required");
    }
    String ns = namespace.trim();
    boolean dropped = catalogService.dropNamespace(ns);
    if (!dropped) {
      if (!catalogService.namespaceExists(ns)) {
        return Response.status(Response.Status.NOT_FOUND)
            .entity(Map.of("error", "namespace not found"))
            .build();
      }
      return Response.status(Response.Status.CONFLICT)
          .entity(Map.of("error", "namespace is not empty"))
          .build();
    }
    return Response.ok(Map.of("namespace", ns, "dropped", true)).build();
  }

  @GET
  @Path("/tables")
  public Response listTables(@QueryParam("namespace") String namespace) {
    if (namespace == null || namespace.trim().isEmpty()) {
      throw new BadRequestException("namespace query parameter is required");
    }
    String ns = namespace.trim();
    if (!catalogService.namespaceExists(ns)) {
      return Response.status(Response.Status.NOT_FOUND)
          .entity(Map.of("error", "namespace not found"))
          .build();
    }
    List<String> tables;
    try {
      tables = catalogService.listTables(ns);
    } catch (IllegalArgumentException e) {
      return Response.status(Response.Status.NOT_FOUND)
          .entity(Map.of("error", e.getMessage()))
          .build();
    }
    Map<String, Object> payload = new HashMap<>();
    payload.put("namespace", ns);
    payload.put("tables", tables);
    return Response.ok(payload).build();
  }
}
