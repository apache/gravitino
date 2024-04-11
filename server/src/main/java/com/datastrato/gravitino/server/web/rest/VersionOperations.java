/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.rest;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import com.datastrato.gravitino.dto.VersionDTO;
import com.datastrato.gravitino.dto.responses.VersionResponse;
import com.datastrato.gravitino.metrics.MetricNames;
import com.datastrato.gravitino.server.authorization.NameBindings;
import com.datastrato.gravitino.server.web.Utils;
import java.io.IOException;
import java.util.Properties;
import javax.servlet.http.HttpServlet;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@NameBindings.VersionInterfaces
@Path("/version")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class VersionOperations extends HttpServlet {
  @GET
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "version." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "version", absolute = true)
  public Response getVersion() {
    Properties projectProperties = new Properties();
    try {
      projectProperties.load(
          VersionOperations.class.getClassLoader().getResourceAsStream("project.properties"));
      String version = projectProperties.getProperty("project.version");
      String compileDate = projectProperties.getProperty("compile.date");
      String gitCommit = projectProperties.getProperty("git.commit.id");

      VersionDTO versionDTO = new VersionDTO(version, compileDate, gitCommit);

      return Utils.ok(new VersionResponse(versionDTO));
    } catch (IOException e) {
      return Utils.internalError("Failed to get Gravitino version", e);
    }
  }
}
