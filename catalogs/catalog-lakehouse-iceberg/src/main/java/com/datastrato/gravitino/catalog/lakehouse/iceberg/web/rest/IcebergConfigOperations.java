/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.iceberg.web.rest;

import com.datastrato.gravitino.catalog.lakehouse.iceberg.web.IcebergRestUtils;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.iceberg.rest.responses.ConfigResponse;

@Path("/v1/{prefix:([^/]*/)?}config")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class IcebergConfigOperations {

  @Context private HttpServletRequest httpRequest;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getConfig() {
    ConfigResponse response = ConfigResponse.builder().build();
    return IcebergRestUtils.ok(response);
  }
}
