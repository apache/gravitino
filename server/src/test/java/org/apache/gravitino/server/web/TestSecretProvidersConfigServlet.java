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
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.servlet.http.HttpServletResponse;
import org.apache.gravitino.secret.SecretProviderRegistry;
import org.apache.gravitino.secret.memory.InMemorySecretsProvider;
import org.apache.gravitino.server.ServerConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class TestSecretProvidersConfigServlet {

  @Test
  public void testEmptyProviders() throws Exception {
    ServerConfig serverConfig = new ServerConfig(false);
    Assertions.assertTrue(fetchProviderList(serverConfig).isEmpty());
  }

  @Test
  public void testListsConfiguredProvider() throws Exception {
    ServerConfig serverConfig = new ServerConfig(false);
    Properties properties = new Properties();
    properties.setProperty(SecretProviderRegistry.GRAVITINO_SECRET_PROVIDERS, "memory");
    properties.setProperty(
        SecretProviderRegistry.GRAVITINO_SECRET_PROVIDER_PREFIX
            + "memory."
            + SecretProviderRegistry.CLASS_NAME,
        InMemorySecretsProvider.class.getName());
    serverConfig.loadFromProperties(properties);

    List<Map<String, Object>> providers = fetchProviderList(serverConfig);
    Assertions.assertEquals(1, providers.size());
    Assertions.assertEquals("memory", providers.get(0).get("name"));
    Assertions.assertEquals("memory", providers.get(0).get("type"));
    Assertions.assertFalse(providers.get(0).containsKey("uri"));
  }

  @Test
  public void testListsOptionalUri() throws Exception {
    ServerConfig serverConfig = new ServerConfig(false);
    Properties properties = new Properties();
    properties.setProperty(SecretProviderRegistry.GRAVITINO_SECRET_PROVIDERS, "vault");
    properties.setProperty(
        SecretProviderRegistry.GRAVITINO_SECRET_PROVIDER_PREFIX
            + "vault."
            + SecretProviderRegistry.CLASS_NAME,
        InMemorySecretsProvider.class.getName());
    properties.setProperty(
        SecretProviderRegistry.GRAVITINO_SECRET_PROVIDER_PREFIX
            + "vault."
            + SecretProviderRegistry.URI,
        "https://vault.example.com");
    serverConfig.loadFromProperties(properties);

    List<Map<String, Object>> providers = fetchProviderList(serverConfig);
    Assertions.assertEquals(1, providers.size());
    Assertions.assertEquals("vault", providers.get(0).get("name"));
    Assertions.assertEquals("memory", providers.get(0).get("type"));
    Assertions.assertEquals("https://vault.example.com", providers.get(0).get("uri"));
  }

  @Test
  public void testHandlesIOException() throws Exception {
    SecretProvidersConfigServlet servlet =
        new SecretProvidersConfigServlet(new ServerConfig(false));
    servlet.init();
    HttpServletResponse res = mock(HttpServletResponse.class);
    PrintWriter writer = mock(PrintWriter.class);
    when(res.getWriter()).thenReturn(writer);
    doThrow(new IOException("Test IO error")).when(writer).write(any(String.class));

    assertDoesNotThrow(() -> servlet.doGet(null, res));
    verify(res).setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    servlet.destroy();
  }

  private Map<String, Object> fetchProviders(ServerConfig serverConfig) throws Exception {
    SecretProvidersConfigServlet servlet = new SecretProvidersConfigServlet(serverConfig);
    servlet.init();
    HttpServletResponse res = mock(HttpServletResponse.class);
    PrintWriter writer = mock(PrintWriter.class);
    when(res.getWriter()).thenReturn(writer);
    servlet.doGet(null, res);
    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    verify(writer).write(captor.capture());
    servlet.destroy();
    return ObjectMapperProvider.objectMapper()
        .readValue(captor.getValue(), new TypeReference<Map<String, Object>>() {});
  }

  private List<Map<String, Object>> fetchProviderList(ServerConfig serverConfig) throws Exception {
    Map<String, Object> body = fetchProviders(serverConfig);
    return ObjectMapperProvider.objectMapper()
        .convertValue(body.get("providers"), new TypeReference<List<Map<String, Object>>>() {});
  }
}
