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

import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlets.CrossOriginFilter;

class CorsFilterHolder {

  static final String PREFLIGHT_MAX_AGE = "preflightMaxAge";

  private CorsFilterHolder() {}

  public static FilterHolder create(JettyServerConfig config) {
    FilterHolder filterHolder = new FilterHolder();
    filterHolder.setClassName(CrossOriginFilter.class.getName());
    filterHolder.setInitParameter(
        JettyServerConfig.ALLOWED_ORIGINS.getKey(), config.getAllowedOrigins());
    filterHolder.setInitParameter(
        JettyServerConfig.ALLOWED_TIMING_ORIGINS.getKey(), config.getAllowedTimingOrigins());
    filterHolder.setInitParameter(
        JettyServerConfig.CHAIN_PREFLIGHT.getKey(), String.valueOf(config.isChainPreflight()));
    filterHolder.setInitParameter(
        PREFLIGHT_MAX_AGE, String.valueOf(config.getPreflightMaxAgeInSecs()));
    filterHolder.setInitParameter(
        JettyServerConfig.ALLOW_CREDENTIALS.getKey(), String.valueOf(config.isAllowCredentials()));
    filterHolder.setInitParameter(
        JettyServerConfig.ALLOWED_METHODS.getKey(), config.getAllowedMethods());
    filterHolder.setInitParameter(
        JettyServerConfig.ALLOWED_HEADERS.getKey(), config.getAllowedHeaders());
    filterHolder.setInitParameter(
        JettyServerConfig.EXPOSED_HEADERS.getKey(), config.getExposedHeaders());
    return filterHolder;
  }
}
