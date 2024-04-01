/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.iceberg.web.rest;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.web.IcebergRestUtils;
import com.datastrato.gravitino.metrics.MetricNames;
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

  @SuppressWarnings("UnusedVariable")
  @Context
  private HttpServletRequest httpRequest;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "config." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "config", absolute = true)
  public Response getConfig() {
    ConfigResponse response = ConfigResponse.builder().build();
    return IcebergRestUtils.ok(response);
  }
}
