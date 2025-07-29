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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.gravitino.Configs;
import org.apache.gravitino.auth.AuthenticatorType;
import org.apache.gravitino.config.ConfigEntry;
import org.apache.gravitino.server.ServerConfig;
import org.apache.gravitino.server.authentication.OAuthConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigServlet extends HttpServlet {

  private static final Logger LOG = LoggerFactory.getLogger(ConfigServlet.class);

  private static final ImmutableSet<ConfigEntry<?>> oauthConfigEntries =
      ImmutableSet.of(
          OAuthConfig.DEFAULT_SERVER_URI,
          OAuthConfig.DEFAULT_TOKEN_PATH,
          OAuthConfig.PROVIDER,
          OAuthConfig.CLIENT_ID,
          OAuthConfig.AUTHORITY,
          OAuthConfig.SCOPE);

  private static final ImmutableSet<ConfigEntry<?>> basicConfigEntries =
      ImmutableSet.of(Configs.AUTHENTICATORS, Configs.ENABLE_AUTHORIZATION);

  private final Map<String, Object> configs = Maps.newHashMap();

  public ConfigServlet(ServerConfig serverConfig) {
    for (ConfigEntry<?> key : basicConfigEntries) {
      configs.put(key.getKey(), serverConfig.get(key));
    }

    if (serverConfig
        .get(Configs.AUTHENTICATORS)
        .contains(AuthenticatorType.OAUTH.name().toLowerCase())) {
      for (ConfigEntry<?> key : oauthConfigEntries) {
        configs.put(key.getKey(), serverConfig.get(key));
      }
    }

    List<String> visibleConfigs = serverConfig.get(Configs.VISIBLE_CONFIGS);

    for (String config : visibleConfigs) {
      String configValue = serverConfig.getRawString(config);
      if (configValue != null) {
        configs.put(config, configValue);
      }
    }
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse res)
      throws IllegalStateException, IOException {
    try (PrintWriter writer = res.getWriter()) {
      res.setContentType("application/json;charset=utf-8");
      writer.write(ObjectMapperProvider.objectMapper().writeValueAsString(configs));
    } catch (IllegalStateException exception) {
      LOG.error("Illegal state occurred when calling getWriter()");
    } catch (IOException exception) {
      LOG.error("Failed to perform IO");
    } catch (Exception e) {
      LOG.error(e.getMessage());
    }
  }
}
