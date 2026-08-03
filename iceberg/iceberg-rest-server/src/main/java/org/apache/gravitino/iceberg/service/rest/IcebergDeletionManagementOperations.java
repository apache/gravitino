/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.iceberg.service.rest;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Encoded;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.iceberg.service.IcebergExceptionMapper;
import org.apache.gravitino.iceberg.service.IcebergRESTUtils;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.iceberg.service.deletion.IcebergDeletionException;
import org.apache.gravitino.iceberg.service.deletion.IcebergDeletionResponses;
import org.apache.gravitino.iceberg.service.deletion.IcebergRetainedTableDeletion;
import org.apache.gravitino.iceberg.service.deletion.IcebergTableDeletionLifecycle;
import org.apache.gravitino.iceberg.service.dispatcher.IcebergTableOperationDispatcher;
import org.apache.gravitino.listener.api.event.IcebergRequestContext;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;
import org.apache.gravitino.server.authorization.annotations.IcebergAuthorizationMetadata;
import org.apache.gravitino.server.authorization.annotations.IcebergAuthorizationMetadata.RequestType;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.apache.gravitino.server.web.Utils;
import org.apache.gravitino.server.web.filter.IcebergTableDeletionAuthzHandler;
import org.apache.gravitino.server.web.filter.IcebergTableDeletionAuthzHandler.AuthorizedDeletionTarget;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.responses.LoadTableResponse;

/** Name-addressed UNDROP for retained Iceberg REST tables. */
@Path("/management/v1/{prefix:([^/]*/)?}namespaces/{namespace}/tables")
@Produces(MediaType.APPLICATION_JSON)
public class IcebergDeletionManagementOperations {

  private final IcebergTableDeletionLifecycle lifecycle;
  private final IcebergTableOperationDispatcher tableOperationDispatcher;

  /**
   * Creates the deletion management resource.
   *
   * @param lifecycle retained table lifecycle
   * @param tableOperationDispatcher live table operation dispatcher
   */
  @Inject
  public IcebergDeletionManagementOperations(
      IcebergTableDeletionLifecycle lifecycle,
      IcebergTableOperationDispatcher tableOperationDispatcher) {
    this.lifecycle = lifecycle;
    this.tableOperationDispatcher = tableOperationDispatcher;
  }

  /**
   * Reactivates the retained table currently reserving the routed name.
   *
   * @param prefix Iceberg REST catalog prefix
   * @param namespace encoded Iceberg namespace
   * @param table encoded Iceberg table name
   * @param servletRequest current HTTP request
   * @return the ordinary live table response after restoration
   */
  @POST
  @Path("{table}/undrop")
  @Timed(name = "undrop-table." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "undrop-table", absolute = true)
  @AuthorizationExpression(
      expression = AuthorizationExpressionConstants.ICEBERG_DROP_TABLE_AUTHORIZATION_EXPRESSION,
      accessMetadataType = MetadataObject.Type.TABLE)
  public Response undrop(
      @AuthorizationMetadata(type = Entity.EntityType.CATALOG) @PathParam("prefix") String prefix,
      @AuthorizationMetadata(type = Entity.EntityType.SCHEMA) @Encoded() @PathParam("namespace")
          String namespace,
      @AuthorizationMetadata(type = Entity.EntityType.TABLE)
          @IcebergAuthorizationMetadata(type = RequestType.MANAGE_TABLE_DELETION)
          @Encoded()
          @PathParam("table")
          String table,
      @Context HttpServletRequest servletRequest) {
    String catalogName = IcebergRESTUtils.getCatalogName(prefix);
    TableIdentifier identifier = identifier(namespace, table);
    try {
      return Utils.doAs(
          servletRequest,
          () -> {
            AuthorizedDeletionTarget target =
                authorizedDeletion(catalogName, identifier, servletRequest);
            IcebergRequestContext context = new IcebergRequestContext(servletRequest, catalogName);
            lifecycle.undrop(context, identifier, target.deletionId(), target.tableId());
            LoadTableResponse response = tableOperationDispatcher.loadTable(context, identifier);
            return IcebergRESTUtils.buildResponseWithETag(response);
          });
    } catch (IcebergDeletionException e) {
      return IcebergDeletionResponses.toResponse(e);
    } catch (Exception e) {
      return IcebergExceptionMapper.toRESTResponse(e);
    }
  }

  private AuthorizedDeletionTarget authorizedDeletion(
      String catalogName, TableIdentifier identifier, HttpServletRequest servletRequest) {
    AuthorizedDeletionTarget target =
        IcebergTableDeletionAuthzHandler.authorizedDeletion(servletRequest);
    if (target != null) {
      return target;
    }

    IcebergRetainedTableDeletion deletion = lifecycle.getDeleted(catalogName, identifier);
    if (IcebergRESTServerContext.getInstance().isAuthorizationEnabled()) {
      throw new IllegalStateException("Retained table authorization context is missing");
    }
    return new AuthorizedDeletionTarget(
        deletion.getTable().getTableId(), deletion.getTable().getDeletionId());
  }

  private static TableIdentifier identifier(String namespace, String table) {
    Namespace decodedNamespace =
        RESTUtil.decodeNamespace(namespace, IcebergRESTUtils.NAMESPACE_SEPARATOR_URLENCODED_UTF_8);
    return TableIdentifier.of(decodedNamespace, RESTUtil.decodeString(table));
  }
}
