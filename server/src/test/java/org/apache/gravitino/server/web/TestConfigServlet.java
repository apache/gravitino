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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import javax.servlet.http.HttpServletResponse;
import org.apache.gravitino.Configs;
import org.apache.gravitino.auth.AuthenticatorType;
import org.apache.gravitino.config.ConfigBuilder;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.config.ConfigEntry;
import org.apache.gravitino.server.ServerConfig;
import org.apache.gravitino.server.authentication.OAuthConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class TestConfigServlet {

  @Test
  public void testConfigServlet() throws Exception {
    Map<String, Object> configs = fetchConfigs(new ServerConfig());
    Assertions.assertEquals(
        Lists.newArrayList("simple"), configs.get(Configs.AUTHENTICATORS.getKey()));
    Assertions.assertEquals(false, configs.get(Configs.ENABLE_AUTHORIZATION.getKey()));
    Assertions.assertEquals(":", configs.get(Configs.SCHEMA_SEPARATOR.getKey()));
  }

  @Test
  public void testConfigServletExposesCustomSchemaSeparator() throws Exception {
    // The schema separator must reflect the configured value, not a hard-coded default.
    ServerConfig serverConfig = new ServerConfig();
    serverConfig.set(Configs.SCHEMA_SEPARATOR, "|");
    Map<String, Object> configs = fetchConfigs(serverConfig);
    Assertions.assertEquals("|", configs.get(Configs.SCHEMA_SEPARATOR.getKey()));
  }

  @Test
  public void testConfigServletWithAuthEnabledButNoServiceAdmins() throws Exception {
    // When authorization is enabled but serviceAdmins is not configured, the key should be
    // absent from the response (no crash) rather than null or empty.
    ServerConfig serverConfig = new ServerConfig();
    serverConfig.set(Configs.ENABLE_AUTHORIZATION, true);
    Map<String, Object> configs = fetchConfigs(serverConfig);
    Assertions.assertEquals(true, configs.get(Configs.ENABLE_AUTHORIZATION.getKey()));
    Assertions.assertFalse(configs.containsKey(Configs.SERVICE_ADMINS.getKey()));
  }

  @Test
  public void testConfigServletWithAuthEnabledAndServiceAdmins() throws Exception {
    ServerConfig serverConfig = new ServerConfig();
    serverConfig.set(Configs.ENABLE_AUTHORIZATION, true);
    serverConfig.set(Configs.SERVICE_ADMINS, Lists.newArrayList("admin1", "admin2"));
    Map<String, Object> configs = fetchConfigs(serverConfig);
    Assertions.assertEquals(true, configs.get(Configs.ENABLE_AUTHORIZATION.getKey()));
    Assertions.assertEquals(
        Lists.newArrayList("admin1", "admin2"), configs.get(Configs.SERVICE_ADMINS.getKey()));
  }

  @Test
  public void testConfigServletWithVisibleConfigs() throws Exception {
    ServerConfig serverConfig = new ServerConfig();

    ConfigEntry<String> customConfig =
        new ConfigBuilder("gravitino.extended.custom.config")
            .doc("Gravitino custom config")
            .version(ConfigConstants.VERSION_0_9_0)
            .stringConf()
            .createWithDefault("default");

    serverConfig.set(customConfig, "test");
    serverConfig.set(Configs.VISIBLE_CONFIGS, Lists.newArrayList(customConfig.getKey()));
    Map<String, Object> configs = fetchConfigs(serverConfig);
    Assertions.assertEquals("test", configs.get(customConfig.getKey()));
  }

  @Test
  public void testConfigServletWithOAuthJwksValidator() throws Exception {
    ServerConfig serverConfig = new ServerConfig();

    serverConfig.set(
        Configs.AUTHENTICATORS, Lists.newArrayList(AuthenticatorType.OAUTH.name().toLowerCase()));
    serverConfig.set(OAuthConfig.SERVICE_AUDIENCE, "test-service");
    serverConfig.set(OAuthConfig.JWKS_URI, "https://example.com/.well-known/jwks.json");
    serverConfig.set(
        OAuthConfig.TOKEN_VALIDATOR_CLASS,
        "org.apache.gravitino.server.authentication.JwksTokenValidator");

    assertDoesNotThrow(() -> new ConfigServlet(serverConfig));
  }

  @Test
  public void testConfigServletHandlesIOException() throws Exception {
    ServerConfig serverConfig = new ServerConfig();
    ConfigServlet configServlet = new ConfigServlet(serverConfig);
    configServlet.init();
    HttpServletResponse res = mock(HttpServletResponse.class);
    PrintWriter writer = mock(PrintWriter.class);
    when(res.getWriter()).thenReturn(writer);
    doThrow(new IOException("Test IO error")).when(writer).write(any(String.class));

    assertDoesNotThrow(() -> configServlet.doGet(null, res));
    verify(res).setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    configServlet.destroy();
  }

  @Test
  public void testConfigServletHandlesIllegalStateException() throws Exception {
    ServerConfig serverConfig = new ServerConfig();
    ConfigServlet configServlet = new ConfigServlet(serverConfig);
    configServlet.init();
    HttpServletResponse res = mock(HttpServletResponse.class);
    when(res.getWriter()).thenThrow(new IllegalStateException("Test state error"));

    assertDoesNotThrow(() -> configServlet.doGet(null, res));
    verify(res).setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    configServlet.destroy();
  }

  /**
   * Invokes the servlet and parses the JSON it writes back into a map, so tests can assert on
   * individual config entries instead of matching a brittle, order-dependent JSON string.
   */
  private Map<String, Object> fetchConfigs(ServerConfig serverConfig) throws Exception {
    ConfigServlet configServlet = new ConfigServlet(serverConfig);
    configServlet.init();
    HttpServletResponse res = mock(HttpServletResponse.class);
    PrintWriter writer = mock(PrintWriter.class);
    when(res.getWriter()).thenReturn(writer);
    configServlet.doGet(null, res);
    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    verify(writer).write(captor.capture());
    configServlet.destroy();
    return ObjectMapperProvider.objectMapper()
        .readValue(captor.getValue(), new TypeReference<Map<String, Object>>() {});
  }
}
