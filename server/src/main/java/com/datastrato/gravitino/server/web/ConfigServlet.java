/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web;

import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.config.ConfigEntry;
import com.datastrato.gravitino.json.JsonUtils;
import com.datastrato.gravitino.server.ServerConfig;
import com.datastrato.gravitino.server.auth.OAuthConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class ConfigServlet extends HttpServlet {

  private static final ImmutableSet<ConfigEntry<?>> configEntries =
      ImmutableSet.of(
          Configs.AUTHENTICATOR, OAuthConfig.DEFAULT_SERVER_URI, OAuthConfig.DEFAULT_TOKEN_PATH);

  private final Map<String, String> configs = Maps.newHashMap();

  public ConfigServlet(ServerConfig serverConfig) {
    for (ConfigEntry<?> key : configEntries) {
      String config = String.valueOf(serverConfig.get(key));
      configs.put(key.getKey(), config);
    }
  }

  protected void doGet(HttpServletRequest req, HttpServletResponse res)
      throws ServletException, IOException {
    try (PrintWriter writer = res.getWriter()) {
      ObjectMapper objectMapper = JsonUtils.objectMapper();
      res.setContentType("application/json;charset=utf-8");
      writer.write(objectMapper.writeValueAsString(configs));
    }
  }
}
