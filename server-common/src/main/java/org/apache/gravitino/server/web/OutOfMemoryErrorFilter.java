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
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

/** Records OOM escaping downstream filters and servlets before Jetty consumes the failure. */
public final class OutOfMemoryErrorFilter implements Filter {
  private final ServerHealth health = ServerHealth.getInstance();

  /** Creates a filter using the shared server health state. */
  public OutOfMemoryErrorFilter() {}

  /** {@inheritDoc} */
  @Override
  public void init(FilterConfig filterConfig) {}

  /** {@inheritDoc} */
  @Override
  public void destroy() {}

  /** {@inheritDoc} */
  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {
    try {
      chain.doFilter(request, response);
    } catch (Throwable failure) {
      health.recordFailure(failure);
      throw failure;
    }
  }
}
