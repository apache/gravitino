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

import java.io.IOException;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.utils.PrincipalUtils;

/**
 * Supplies the configured service identity for anonymous Lance REST requests in auxiliary mode.
 * Authenticated requests keep their caller identity.
 */
public class LanceServiceIdentityFilter implements Filter {

  private final UserPrincipal servicePrincipal;

  /**
   * Creates a service identity filter.
   *
   * @param userName the configured Lance REST service user name
   */
  public LanceServiceIdentityFilter(String userName) {
    this.servicePrincipal = new UserPrincipal(userName);
  }

  @Override
  public void init(FilterConfig filterConfig) {}

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {
    try {
      // AuthenticationFilter runs before this filter and keeps the rest of the filter chain inside
      // the caller's Subject. Starting another doAs block here would make the service user the
      // effective identity, so downstream code would no longer see the real caller. Continue the
      // existing chain directly to preserve the caller's active roles and other principal details.
      if (!AuthConstants.ANONYMOUS_USER.equals(PrincipalUtils.getCurrentUserName())) {
        chain.doFilter(request, response);
        return;
      }

      // When authentication is disabled, or simple authentication accepted an anonymous request,
      // there is no user identity for internal Gravitino dispatcher calls. Use the configured
      // service identity only for this fallback case.
      PrincipalUtils.doAs(
          servicePrincipal,
          () -> {
            chain.doFilter(request, response);
            return null;
          });
    } catch (IOException | ServletException e) {
      throw e;
    } catch (Exception e) {
      throw new ServletException("Failed to execute the Lance REST request", e);
    }
  }

  @Override
  public void destroy() {}
}
