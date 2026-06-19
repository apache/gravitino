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

/**
 * Determines whether a URI path targets a health check endpoint.
 *
 * <p>The base implementation covers the canonical Gravitino health paths ({@code /health}, {@code
 * /health/*}, {@code /health.html}, {@code /api/health}, {@code /api/health/*}). Server-specific
 * subclasses (e.g. {@code IcebergHealthCheckPathMatcher}) extend this class to add additional
 * health endpoints served by their respective REST servers.
 *
 * <p>Both {@link org.apache.gravitino.server.authentication.AuthenticationFilter} (to bypass
 * authentication for probe traffic) and {@link HttpAuditFilter} (to skip audit logging for probe
 * traffic) use an instance of this class. Passing the same subclass instance to both filters on a
 * given server ensures they agree on which paths are health checks.
 */
public class HealthCheckPathMatcher {

  /**
   * Returns {@code true} if {@code path} targets a Gravitino health check endpoint.
   *
   * <p>Covers the canonical API path ({@code /api/health} and {@code /api/health/*}) and the
   * root-level aliases ({@code /health}, {@code /health/*}, {@code /health.html}) that forward to
   * the canonical paths. During a {@link javax.servlet.RequestDispatcher} forward, {@code
   * getRequestURI()} returns the original URI rather than the target, so these aliases must be
   * included here.
   *
   * <p>Subclasses should override this method and call {@code super.isHealthCheckPath(path)} first
   * to preserve the base paths.
   *
   * @param path the URI path to test; may be {@code null}
   * @return {@code true} if {@code path} is a health check endpoint
   */
  public boolean isHealthCheckPath(String path) {
    if (path == null) {
      return false;
    }
    return path.equals("/health")
        || path.startsWith("/health/")
        || path.equals("/health.html")
        || path.equals("/api/health")
        || path.startsWith("/api/health/");
  }
}
