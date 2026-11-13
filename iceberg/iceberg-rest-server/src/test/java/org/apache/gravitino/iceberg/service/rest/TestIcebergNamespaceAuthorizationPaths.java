/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.iceberg.service.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import javax.inject.Singleton;
import javax.ws.rs.core.Application;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.gravitino.iceberg.service.IcebergExceptionMapper;
import org.apache.gravitino.iceberg.service.IcebergObjectMapper;
import org.apache.gravitino.iceberg.service.IcebergObjectMapperProvider;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.iceberg.service.dispatcher.IcebergNamespaceOperationDispatcher;
import org.apache.gravitino.iceberg.service.idempotency.IcebergIdempotencyManager;
import org.apache.gravitino.iceberg.service.provider.IcebergConfigProvider;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.server.web.filter.IcebergRESTAuthInterceptionService;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.glassfish.hk2.api.InterceptionService;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests encoded and literal dotted namespace paths through HTTP and the authorization proxy. */
public class TestIcebergNamespaceAuthorizationPaths extends JerseyTest {

  private static final String UPDATE_BODY = "{\"updates\":{\"k\":\"v\"}}";

  private IcebergNamespaceOperationDispatcher dispatcher;

  /**
   * Registers the production namespace resource and authorization interception service.
   *
   * @return the test application
   */
  @Override
  protected Application configure() {
    try {
      forceSet(
          TestProperties.CONTAINER_PORT, String.valueOf(RESTUtils.findAvailablePort(2000, 3000)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    IcebergConfigProvider provider = mock(IcebergConfigProvider.class);
    when(provider.getMetalakeName()).thenReturn("metalake");
    when(provider.getDefaultCatalogName()).thenReturn("catalog");
    IcebergRESTServerContext.create(provider, true, true, false, null);
    dispatcher = mock(IcebergNamespaceOperationDispatcher.class);
    return new ResourceConfig(IcebergNamespaceOperations.class)
        .register(IcebergExceptionMapper.class)
        .register(IcebergObjectMapperProvider.class)
        .register(JacksonFeature.withoutExceptionMappers())
        .register(
            new AbstractBinder() {
              /** {@inheritDoc} */
              @Override
              protected void configure() {
                bind(dispatcher).to(IcebergNamespaceOperationDispatcher.class);
                bind(new IcebergIdempotencyManager(new IcebergConfig()))
                    .to(IcebergIdempotencyManager.class);
                bind(IcebergRESTAuthInterceptionService.class)
                    .to(InterceptionService.class)
                    .in(Singleton.class);
              }
            });
  }

  @ParameterizedTest
  @CsvSource({"catalog/, a%2Eb", "'', a%2Eb", "catalog/, %2e%2e", "'', %2e%2e"})
  void testEncodedDotPathsNeverReturnInternalError(String prefix, String namespace)
      throws Exception {
    HttpClient client =
        HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(5))
            .build();
    for (String method : new String[] {"GET", "HEAD", "POST", "DELETE"}) {
      String path = namespacePath(prefix, namespace, method);
      HttpRequest request =
          HttpRequest.newBuilder(URI.create(getBaseUri() + path))
              .timeout(Duration.ofSeconds(10))
              .header("Accept", "application/json")
              .header("Content-Type", "application/json")
              .method(
                  method,
                  method.equals("POST")
                      ? HttpRequest.BodyPublishers.ofString(UPDATE_BODY)
                      : HttpRequest.BodyPublishers.noBody())
              .build();
      HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
      assertTrue(
          response.statusCode() == 400 || response.statusCode() == 404,
          () -> method + " " + path + ": " + response.statusCode() + " " + response.body());
      if (namespace.equals("a%2Eb")) {
        assertEquals(400, response.statusCode());
        if (!method.equals("HEAD")) {
          ErrorResponse error =
              IcebergObjectMapper.getInstance().readValue(response.body(), ErrorResponse.class);
          assertEquals(400, error.code());
          assertEquals("BadRequestException", error.type());
        }
      }
    }
    verifyNoInteractions(dispatcher);
  }

  @ParameterizedTest
  @ValueSource(strings = {"catalog/", ""})
  void testLiteralDotDotPathWithoutClientNormalization(String prefix) throws IOException {
    for (String method : new String[] {"GET", "HEAD", "POST", "DELETE"}) {
      String path = "/" + namespacePath(prefix, "..", method);
      String body = method.equals("POST") ? UPDATE_BODY : "";
      // Send the request target directly so the client cannot normalize the '..' segment away.
      String request =
          method
              + " "
              + path
              + " HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\n"
              + "Connection: close\r\nContent-Length: "
              + body.length()
              + "\r\n\r\n"
              + body;
      try (Socket socket = new Socket()) {
        socket.connect(new InetSocketAddress(getBaseUri().getHost(), getBaseUri().getPort()), 5000);
        socket.setSoTimeout(10000);
        socket.getOutputStream().write(request.getBytes(StandardCharsets.US_ASCII));
        socket.getOutputStream().flush();
        String response =
            new String(socket.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        String statusLine = response.split("\r\n", 2)[0];
        assertTrue(
            statusLine.startsWith("HTTP/1.1 400") || statusLine.startsWith("HTTP/1.1 404"),
            () -> method + " " + path + ": " + statusLine);
      }
    }
    verifyNoInteractions(dispatcher);
  }

  private static String namespacePath(String prefix, String namespace, String method) {
    return "v1/"
        + prefix
        + "namespaces/"
        + namespace
        + (method.equals("POST") ? "/properties" : "");
  }
}
