/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web;

import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.auth.AuthenticatorType;
import com.datastrato.gravitino.config.ConfigEntry;
import com.datastrato.gravitino.json.JsonUtils;
import com.datastrato.gravitino.server.ServerConfig;
import com.datastrato.gravitino.server.authentication.OAuthConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigServlet extends HttpServlet {

  private static final Logger LOG = LoggerFactory.getLogger(ConfigServlet.class);

  private static final ImmutableSet<ConfigEntry<?>> oauthConfigEntries =
      ImmutableSet.of(OAuthConfig.DEFAULT_SERVER_URI, OAuthConfig.DEFAULT_TOKEN_PATH);

  private static final ImmutableSet<ConfigEntry<?>> basicConfigEntries =
      ImmutableSet.of(Configs.AUTHENTICATOR);

  private final Map<String, String> configs = Maps.newHashMap();

  public ConfigServlet(ServerConfig serverConfig) {
    for (ConfigEntry<?> key : basicConfigEntries) {
      String config = String.valueOf(serverConfig.get(key));
      configs.put(key.getKey(), config);
    }
    if (serverConfig.get(Configs.AUTHENTICATOR).equalsIgnoreCase(AuthenticatorType.OAUTH.name())) {
      for (ConfigEntry<?> key : oauthConfigEntries) {
        String config = String.valueOf(serverConfig.get(key));
        configs.put(key.getKey(), config);
      }
    }
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse res)
      throws IllegalStateException, IOException {
    try (PrintWriter writer = res.getWriter()) {
      ObjectMapper objectMapper = JsonUtils.objectMapper();
      res.setContentType("application/json;charset=utf-8");
      writer.write(objectMapper.writeValueAsString(configs));
    } catch (IllegalStateException exception) {
      LOG.error("Illegal state occurred when calling getWriter()");
    } catch (IOException exception) {
      LOG.error("Failed to perform IO");
    } catch (Exception e) {
      LOG.error(e.getMessage());
    }
  }
}
