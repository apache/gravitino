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
import org.apache.gravitino.iceberg.service.IcebergRestUtils;
import org.apache.gravitino.iceberg.service.dispatcher.IcebergTableOperationDispatcher;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/v1/{prefix:([^/]*/)?}tables/rename")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class IcebergTableRenameOperations {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergTableOperations.class);

  @SuppressWarnings("UnusedVariable")
  @Context
  private HttpServletRequest httpRequest;

  private IcebergTableOperationDispatcher tableOperationDispatcher;

  @Inject
  public IcebergTableRenameOperations(IcebergTableOperationDispatcher tableOperationDispatcher) {
    this.tableOperationDispatcher = tableOperationDispatcher;
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "rename-table." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "rename-table", absolute = true)
  public Response renameTable(
      @PathParam("prefix") String prefix, RenameTableRequest renameTableRequest) {
    String catalogName = IcebergRestUtils.getCatalogName(prefix);
    LOG.info(
        "Rename Iceberg tables, catalog: {}, source: {}, destination: {}.",
        catalogName,
        renameTableRequest.source(),
        renameTableRequest.destination());
    tableOperationDispatcher.renameTable(catalogName, renameTableRequest);
    return IcebergRestUtils.okWithoutContent();
  }
}
