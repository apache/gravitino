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
package org.apache.gravitino.lance.service;

import javax.servlet.ServletRequest;
import javax.servlet.http.HttpServletRequest;
import org.apache.gravitino.server.authentication.AuthenticationFilter;

/**
 * An {@link AuthenticationFilter} subclass for the Lance REST server that allows health check
 * endpoints to bypass authentication.
 *
 * <p>The default {@link AuthenticationFilter} only whitelists /health, /api/health paths. This
 * subclass additionally permits /lance/health and /lance/health/* so that Kubernetes probes and
 * monitoring systems can reach the Lance health endpoint without credentials.
 */
public class LanceAuthenticationFilter extends AuthenticationFilter {

  @Override
  protected boolean isHealthCheckRequest(ServletRequest request) {
    if (super.isHealthCheckRequest(request)) {
      return true;
    }

    if (!(request instanceof HttpServletRequest)) {
      return false;
    }
    String path = ((HttpServletRequest) request).getRequestURI();
    if (path == null) {
      return false;
    }
    return path.equals("/lance/health") || path.startsWith("/lance/health/");
  }
}
