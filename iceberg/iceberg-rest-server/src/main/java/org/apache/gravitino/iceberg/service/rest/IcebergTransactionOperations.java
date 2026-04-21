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
package org.apache.gravitino.iceberg.service.rest;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import com.google.common.annotations.VisibleForTesting;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
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
import org.apache.gravitino.iceberg.service.dispatcher.IcebergTransactionOperationDispatcher;
import org.apache.gravitino.listener.api.event.IcebergRequestContext;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;
import org.apache.gravitino.server.web.Utils;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handles {@code POST /v1/{prefix}/transactions/commit} for atomic multi-table commits. */
@Path("/v1/{prefix:([^/]*/)?}transactions")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class IcebergTransactionOperations {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergTransactionOperations.class);

  @Context private HttpServletRequest httpRequest;

  private final IcebergTransactionOperationDispatcher transactionOperationDispatcher;

  @Inject
  public IcebergTransactionOperations(
      IcebergTransactionOperationDispatcher transactionOperationDispatcher) {
    this.transactionOperationDispatcher = transactionOperationDispatcher;
  }

  @POST
  @Path("commit")
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "commit-transaction." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "commit-transaction", absolute = true)
  @AuthorizationExpression(
      expression =
          "ANY(OWNER, METALAKE, CATALOG) || "
              + "SCHEMA_OWNER_WITH_USE_CATALOG || "
              + "ANY_USE_CATALOG",
      accessMetadataType = MetadataObject.Type.CATALOG)
  public Response commitTransaction(
      @AuthorizationMetadata(type = Entity.EntityType.CATALOG) @PathParam("prefix") String prefix,
      CommitTransactionRequest commitTransactionRequest) {
    String catalogName = IcebergRESTUtils.getCatalogName(prefix);
    LOG.info(
        "Commit Iceberg transaction, catalog: {}, tableChanges count: {}",
        catalogName,
        commitTransactionRequest.tableChanges().size());
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            IcebergRequestContext context =
                new IcebergRequestContext(httpServletRequest(), catalogName);
            transactionOperationDispatcher.commitTransaction(context, commitTransactionRequest);
            return IcebergRESTUtils.noContent();
          });
    } catch (Exception e) {
      return IcebergExceptionMapper.toRESTResponse(e);
    }
  }

  // HTTP request is null in Jersey test, override with a mock request when testing.
  @VisibleForTesting
  HttpServletRequest httpServletRequest() {
    return httpRequest;
  }
}
