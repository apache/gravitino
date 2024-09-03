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
package org.apache.gravitino.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.matchers.Times;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.model.Parameter;

public abstract class TestBase {

  protected static final ObjectMapper MAPPER = ObjectMapperProvider.objectMapper();

  protected static ClientAndServer mockServer;

  protected static GravitinoAdminClient client;

  @BeforeAll
  public static void setUp() throws Exception {
    mockServer = ClientAndServer.startClientAndServer(0);
    int port = mockServer.getLocalPort();
    client =
        GravitinoAdminClient.builder("http://127.0.0.1:" + port).withVersionCheckDisabled().build();
  }

  @AfterAll
  public static void tearDown() {
    mockServer.stop();
    client.close();
  }

  protected static <T, R> void buildMockResource(
      Method method,
      String path,
      Map<String, String> queryParams,
      T reqBody,
      R respBody,
      int statusCode)
      throws JsonProcessingException {
    List<Parameter> parameters =
        queryParams.entrySet().stream()
            .map(kv -> new Parameter(kv.getKey(), kv.getValue()))
            .collect(Collectors.toList());

    HttpRequest mockRequest =
        HttpRequest.request(path).withMethod(method.name()).withQueryStringParameters(parameters);
    if (reqBody != null) {
      String reqJson = MAPPER.writeValueAsString(reqBody);
      mockRequest = mockRequest.withBody(reqJson);
    }

    HttpResponse mockResponse = HttpResponse.response().withStatusCode(statusCode);
    if (respBody != null) {
      String respJson = MAPPER.writeValueAsString(respBody);
      mockResponse = mockResponse.withBody(respJson);
    }

    // Using Times.exactly(1) will only match once for the request, so we could set difference
    // responses for the same request and path.
    mockServer.when(mockRequest, Times.exactly(1)).respond(mockResponse);
  }

  protected static <T, R> void buildMockResource(
      Method method, String path, T reqBody, R respBody, int statusCode)
      throws JsonProcessingException {
    buildMockResource(method, path, Collections.emptyMap(), reqBody, respBody, statusCode);
  }

  protected static String withSlash(String path) {
    return "/" + path;
  }
}
