/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.filesystem.hadoop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.MetalakeDTO;
import org.apache.gravitino.dto.responses.MetalakeResponse;
import org.apache.gravitino.exceptions.RESTException;
import org.apache.gravitino.json.JsonUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockserver.matchers.Times;
import org.mockserver.model.Header;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;

/** Tests Basic authentication for the Java Gravitino Virtual File System client. */
public class TestBasicClient extends TestGvfsBase {
  private static final String USERNAME = "admin";
  private static final String PASSWORD = "YourSecurePassword12";

  /** Sets up the mock server and Basic authentication configuration. */
  @BeforeAll
  public static void setup() {
    TestGvfsBase.setup();
    conf.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_AUTH_TYPE_KEY,
        GravitinoVirtualFileSystemConfiguration.BASIC_AUTH_TYPE);
    conf.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_BASIC_USERNAME_KEY, USERNAME);
    conf.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_BASIC_PASSWORD_KEY, PASSWORD);
  }

  /** Verifies that GVFS sends the configured Basic authentication credentials. */
  @Test
  public void testBasicAuthToken() throws IOException {
    String testMetalake = "test_basic_token";
    HttpRequest mockRequest =
        HttpRequest.request("/api/metalakes/" + testMetalake)
            .withMethod(Method.GET.name())
            .withQueryStringParameters(Collections.emptyMap());

    MetalakeDTO mockMetalake =
        MetalakeDTO.builder()
            .withName(testMetalake)
            .withComment("comment")
            .withAudit(
                AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();
    MetalakeResponse response = new MetalakeResponse(mockMetalake);

    AtomicReference<String> actualTokenValue = new AtomicReference<>();
    mockServer()
        .when(mockRequest, Times.unlimited())
        .respond(
            request -> {
              List<Header> headers = request.getHeaders().getEntries();
              for (Header header : headers) {
                if (header.getName().equalsIgnoreCase("Authorization")) {
                  actualTokenValue.set(header.getValues().get(0).getValue());
                }
              }
              return HttpResponse.response()
                  .withStatusCode(HttpStatus.SC_OK)
                  .withBody(JsonUtils.objectMapper().writeValueAsString(response));
            });

    Path managedFilesetPath =
        FileSystemTestUtils.createFilesetPath(catalogName, schemaName, "testBasicAuthToken", true);
    Path path = new Path(managedFilesetPath.toString().replace(metalakeName, testMetalake));

    Configuration configuration = new Configuration(conf);
    configuration.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_METALAKE_KEY, testMetalake);
    try (FileSystem fileSystem = path.getFileSystem(configuration)) {
      assertThrows(RESTException.class, () -> fileSystem.exists(path));
    }

    String credentials = USERNAME + ":" + PASSWORD;
    assertEquals(
        AuthConstants.AUTHORIZATION_BASIC_HEADER
            + Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8)),
        actualTokenValue.get());
  }

  /** Verifies Basic authentication configuration validation and filtering. */
  @Test
  public void testBasicAuthConfigs() {
    Configuration configuration = new Configuration(false);
    configuration.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_SERVER_URI_KEY, serverUri());
    configuration.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_METALAKE_KEY, metalakeName);
    configuration.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_AUTH_TYPE_KEY,
        GravitinoVirtualFileSystemConfiguration.BASIC_AUTH_TYPE);

    assertThrows(
        IllegalArgumentException.class,
        () -> GravitinoVirtualFileSystemUtils.createClient(configuration));

    configuration.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_BASIC_USERNAME_KEY, USERNAME);
    assertThrows(
        IllegalArgumentException.class,
        () -> GravitinoVirtualFileSystemUtils.createClient(configuration));

    configuration.set(
        GravitinoVirtualFileSystemConfiguration.FS_GRAVITINO_CLIENT_BASIC_PASSWORD_KEY, PASSWORD);
    try (GravitinoClient ignored = GravitinoVirtualFileSystemUtils.createClient(configuration)) {
      Map<String, String> clientConfig =
          GravitinoVirtualFileSystemUtils.extractClientConfig(
              GravitinoVirtualFileSystemUtils.getConfigMap(configuration));
      assertEquals(Collections.emptyMap(), clientConfig);
    }
  }
}
