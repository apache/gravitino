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
 * Forwards root-level health probe paths to canonical health endpoints.
 *
 * <p>The no-arg constructor targets {@code /api/health} (for the main Gravitino server). Pass a
 * custom {@code targetPrefix} (e.g. {@code "/iceberg"}) to forward to a different base path.
 *
 * <p>This alias exists for compatibility with enterprise global traffic managers that require
 * probes at well-known root paths such as {@code /health}, {@code /health/live}, {@code
 * /health/ready}, and {@code /health.html}.
 */
public class HealthAliasServlet extends HttpServlet {

  private final String targetPrefix;

  /** Forwards to {@code /api/health*} (main Gravitino server default). */
  public HealthAliasServlet() {
    this.targetPrefix = "/api";
  }

  /**
   * Forwards to {@code <targetPrefix>/health*}.
   *
   * @param targetPrefix the path prefix of the canonical health endpoint, e.g. {@code "/iceberg"}
   */
  public HealthAliasServlet(String targetPrefix) {
    this.targetPrefix = targetPrefix;
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    // /health.html maps to the aggregate endpoint; other paths keep their sub-path.
    String uri = req.getRequestURI();
    String targetPath = "/health.html".equals(uri) ? targetPrefix + "/health" : targetPrefix + uri;
    RequestDispatcher dispatcher = req.getRequestDispatcher(targetPath);
    if (dispatcher == null) {
      resp.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE, "health dispatcher unavailable");
      return;
    }
    dispatcher.forward(req, resp);
  }
}
