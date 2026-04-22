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
package org.apache.gravitino.server.web;

import java.io.IOException;
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Serves root-level health paths ({@code /health}, {@code /health/live}, {@code /health/ready}) by
 * forwarding to the canonical {@code /api/health/*} endpoints.
 *
 * <p>This alias exists for compatibility with enterprise global traffic managers that require
 * probes at well-known root paths. The canonical implementation remains at {@code /api/health}.
 */
public class HealthAliasServlet extends HttpServlet {

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    // Prepend /api to the incoming path: /health → /api/health, /health/live → /api/health/live
    String targetPath = "/api" + req.getRequestURI();
    RequestDispatcher dispatcher = req.getRequestDispatcher(targetPath);
    if (dispatcher == null) {
      resp.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE, "health dispatcher unavailable");
      return;
    }
    dispatcher.forward(req, resp);
  }
}
