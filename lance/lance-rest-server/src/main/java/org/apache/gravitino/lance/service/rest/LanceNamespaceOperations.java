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

import static org.apache.gravitino.lance.common.ops.NamespaceWrapper.NAMESPACE_DELIMITER_DEFAULT;

import com.lancedb.lance.namespace.model.ListNamespacesResponse;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.Encoded;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.lance.common.ops.NamespaceWrapper;
import org.apache.gravitino.lance.service.LanceExceptionMapper;

@Path("/v1/namespace")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class LanceNamespaceOperations {

  private final NamespaceWrapper lanceNamespace;

  @Inject
  public LanceNamespaceOperations(NamespaceWrapper lanceNamespace) {
    this.lanceNamespace = lanceNamespace;
  }

  @GET
  @Path("/{id}/list")
  public Response listNamespaces(
      @Encoded @PathParam("id") String namespaceId,
      @DefaultValue(NAMESPACE_DELIMITER_DEFAULT) @QueryParam("delimiter") String delimiter,
      @QueryParam("page_token") String pageToken,
      @QueryParam("limit") Integer limit) {
    try {
      ListNamespacesResponse response =
          lanceNamespace.asNamespaceOps().listNamespaces(namespaceId, delimiter, pageToken, limit);
      return Response.ok(response).build();
    } catch (Exception e) {
      return LanceExceptionMapper.toRESTResponse(namespaceId, e);
    }
  }
}
