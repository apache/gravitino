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

import org.apache.gravitino.server.web.HealthCheckPathMatcher;

/**
 * A {@link HealthCheckPathMatcher} for the Lance REST server that additionally recognises {@code
 * /lance/health} and {@code /lance/health/*} as health check endpoints.
 *
 * <p>Pass an instance of this class to both {@link
 * org.apache.gravitino.server.authentication.AuthenticationFilter} (via {@code
 * LanceAuthenticationFilter}) and {@link org.apache.gravitino.server.web.HttpAuditFilter} when
 * constructing the Lance REST server so that both filters agree on which paths are probe traffic.
 */
public class LanceHealthCheckPathMatcher extends HealthCheckPathMatcher {

  @Override
  public boolean isHealthCheckPath(String path) {
    if (super.isHealthCheckPath(path)) {
      return true;
    }
    return path.equals("/lance/health") || path.startsWith("/lance/health/");
  }
}
