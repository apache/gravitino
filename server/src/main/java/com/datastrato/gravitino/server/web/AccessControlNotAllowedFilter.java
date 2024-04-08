/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web;

import static javax.servlet.http.HttpServletResponse.SC_METHOD_NOT_ALLOWED;

import com.datastrato.gravitino.Configs;
import java.io.IOException;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * AccessControlNotAllowedFilter is used for filter the requests related to access control if
 * Gravitino doesn't enable authorization. The filter return 405 error code. You can refer to
 * https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/405. No methods will be returned in the
 * allow methods.
 */
public class AccessControlNotAllowedFilter implements Filter {

  public static final String ALLOW = "Allow";
  public static final String API_METALAKES = "/api/metalakes";
  public static final String USERS = "users";
  public static final String GROUPS = "groups";
  public static final String GRANTS = "grants";

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {}

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {
    HttpServletRequest req = (HttpServletRequest) request;
    String path = req.getRequestURI();
    if (isAccessControlPath(path)) {
      HttpServletResponse resp = (HttpServletResponse) response;
      resp.setHeader(ALLOW, "");
      resp.sendError(
          SC_METHOD_NOT_ALLOWED,
          String.format(
              "You should set '%s' to true in the server side `gravitino.conf`"
                  + " to enable the authorization of the system, otherwise these interfaces can't work.",
              Configs.ENABLE_AUTHORIZATION.getKey()));
    } else {
      chain.doFilter(request, response);
    }
  }

  boolean isAccessControlPath(String path) {
    if (path.startsWith(API_METALAKES)) {
      String[] segments = path.substring(API_METALAKES.length()).split("/");

      if (segments.length > 2) {
        return USERS.equalsIgnoreCase(segments[2])
            || GROUPS.equalsIgnoreCase(segments[2])
            || GRANTS.equalsIgnoreCase(segments[2]);
      } else {
        return false;
      }
    } else {
      return true;
    }
  }

  @Override
  public void destroy() {}
}
