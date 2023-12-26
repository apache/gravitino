/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.server.web;

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
