/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.server.web.rest;

import com.datastrato.graviton.dto.VersionDTO;
import com.datastrato.graviton.dto.responses.VersionResponse;
import com.datastrato.graviton.server.web.Utils;
import java.io.IOException;
import java.util.Properties;
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
  @Produces("application/vnd.graviton.v1+json")
  public Response getVersion() {
    Properties projectProperties = new Properties();
    try {
      projectProperties.load(VersionOperations.class.getResourceAsStream("/project.properties"));
      String version = projectProperties.getProperty("project.version");
      String compileDate = projectProperties.getProperty("compile.date");
      String gitCommit = projectProperties.getProperty("git.commit.id");

      VersionDTO versionDTO =
          new VersionDTO.Builder()
              .withVersion(version)
              .withComment(compileDate)
              .withGitCommit(gitCommit)
              .build();

      return Utils.ok(new VersionResponse(versionDTO));
    } catch (IOException e) {
      return Utils.internalError("Failed to get Graviton version", e);
    }
  }
}
