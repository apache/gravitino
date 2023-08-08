/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.server.web.rest;

import java.io.IOException;
import java.util.Properties;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class ProjectVersion extends HttpServlet {
  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    Properties projectProperties = new Properties();
    projectProperties.load(ProjectVersion.class.getResourceAsStream("/project.properties"));
    String version = projectProperties.getProperty("version");

    resp.getWriter().write(version);
    resp.setStatus(200);
  }
}
