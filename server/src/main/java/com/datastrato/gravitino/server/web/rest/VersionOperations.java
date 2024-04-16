/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.rest;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import com.datastrato.gravitino.VersionUtil;
import com.datastrato.gravitino.dto.responses.VersionResponse;
import com.datastrato.gravitino.metrics.MetricNames;
import com.datastrato.gravitino.server.web.Utils;
import javax.servlet.http.HttpServlet;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/version")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class VersionOperations extends HttpServlet {
  @GET
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "version." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "version", absolute = true)
  public Response getVersion() {
    return Utils.ok(new VersionResponse(VersionUtil.getCurrentVersionDTO()));
  }
}
