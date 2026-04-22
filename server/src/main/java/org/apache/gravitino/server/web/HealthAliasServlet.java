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
 * Serves root-level health check paths ({@code /health}, {@code /health.html}) by forwarding to the
 * canonical {@code /api/health} endpoint.
 *
 * <p>These aliases exist for compatibility with enterprise global traffic managers that require
 * probes at well-known root paths. The canonical implementation remains at {@code /api/health};
 * these servlets are a thin compatibility layer.
 */
public class HealthAliasServlet extends HttpServlet {

  private static final String CANONICAL_HEALTH_PATH = "/api/health";

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    RequestDispatcher dispatcher = req.getRequestDispatcher(CANONICAL_HEALTH_PATH);
    if (dispatcher == null) {
      resp.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE, "health dispatcher unavailable");
      return;
    }
    dispatcher.forward(req, resp);
  }

  @Override
  protected void doHead(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    doGet(req, resp);
  }
}
