/*
* Copyright 2023 Datastrato.
* This software is licensed under the Apache License version 2.
*/

package com.datastrato.graviton.server.web;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VersioningFilter implements Filter {

  private static final Logger LOG = LoggerFactory.getLogger(VersioningFilter.class);

  static final class MutableHttpServletRequest extends HttpServletRequestWrapper {

    private final Map<String, String> customHeaders;

    public MutableHttpServletRequest(HttpServletRequest request) {
      super(request);
      this.customHeaders = new HashMap<>();
    }

    public void putHeader(String name, String value) {
      this.customHeaders.put(name, value);
    }

    @Override
    public String getHeader(String name) {
      String headerValue = customHeaders.get(name);
      if (headerValue != null) {
        return headerValue;
      }
      return ((HttpServletRequest) getRequest()).getHeader(name);
    }

    @Override
    public Enumeration<String> getHeaderNames() {
      List<String> combinedHeaderNames = new ArrayList<>(customHeaders.keySet());

      Enumeration<String> headerNames = ((HttpServletRequest) getRequest()).getHeaderNames();
      while (headerNames.hasMoreElements()) {
        combinedHeaderNames.add(headerNames.nextElement());
      }

      return Collections.enumeration(combinedHeaderNames);
    }
  }

  private static final Pattern ACCEPT_VERSION_REGEX =
      Pattern.compile("application/vnd\\.graviton\\.v(\\d+)\\+json");
  private static final String ACCEPT_VERSION_HEADER = "Accept";
  private static final String ACCEPT_VERSION = "application/vnd.graviton.v%d+json";

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {}

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {
    HttpServletRequest req = (HttpServletRequest) request;
    Enumeration<String> acceptHeader = req.getHeaders("Accept");
    while (acceptHeader.hasMoreElements()) {
      String value = acceptHeader.nextElement();

      // If version accept header is set, then we need to check if it is supported.
      Matcher m = ACCEPT_VERSION_REGEX.matcher(value);
      if (m.matches()) {
        int version = Integer.parseInt(m.group(1));

        if (!ApiVersion.isSupportedVersion(version)) {
          LOG.error("Unsupported version v{} in Request Header {}.", version, value);

          HttpServletResponse resp = (HttpServletResponse) response;
          resp.sendError(HttpServletResponse.SC_NOT_ACCEPTABLE, "Unsupported version");
        } else {
          chain.doFilter(request, response);
        }

        return;
      }
    }

    // If no version accept header not is set, then we need to set the latest version.
    MutableHttpServletRequest mutableRequest = new MutableHttpServletRequest(req);
    ApiVersion latest = ApiVersion.latestVersion();
    mutableRequest.putHeader(
        ACCEPT_VERSION_HEADER, String.format(ACCEPT_VERSION, latest.version()));

    chain.doFilter(mutableRequest, response);
  }

  @Override
  public void destroy() {}
}
