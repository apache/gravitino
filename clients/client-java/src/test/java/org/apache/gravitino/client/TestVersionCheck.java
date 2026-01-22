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
import java.util.Collections;
import org.apache.gravitino.Version;
import org.apache.gravitino.dto.VersionDTO;
import org.apache.gravitino.dto.responses.VersionResponse;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockserver.matchers.Times;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;

public class TestVersionCheck extends TestBase {

  private static class TestClient extends GravitinoClientBase {
    protected TestClient(String uri, AuthDataProvider authDataProvider, boolean checkVersion) {
      super(uri, authDataProvider, checkVersion, Collections.emptyMap(), Collections.emptyMap());
    }
  }

  private static class TestBuilder extends GravitinoClientBase.Builder<TestClient> {
    private static String envValue;

    protected TestBuilder() {
      super("http://localhost:12345");
    }

    @Override
    protected String versionCheckDisabledEnvValue() {
      return envValue;
    }

    @Override
    public TestClient build() {
      return new TestClient(uri, authDataProvider, isVersionCheckEnabled());
    }

    private boolean isEnabled() {
      return isVersionCheckEnabled();
    }
  }

  @AfterEach
  public void resetEnvValue() {
    TestBuilder.envValue = null;
  }

  @Test
  public void testEnvDisablesVersionCheck() {
    TestBuilder.envValue = "true";
    TestBuilder builder = new TestBuilder();

    Assertions.assertFalse(builder.isEnabled());
  }

  @Test
  public void testEnvIsCaseInsensitive() {
    TestBuilder.envValue = "TrUe";
    TestBuilder builder = new TestBuilder();

    Assertions.assertFalse(builder.isEnabled());
  }

  @Test
  public void testEnvFalseKeepsVersionCheckEnabled() {
    TestBuilder.envValue = "false";
    TestBuilder builder = new TestBuilder();

    Assertions.assertTrue(builder.isEnabled());
  }

  @Test
  public void testExplicitDisableOverridesEnvFalse() {
    TestBuilder.envValue = "false";
    TestBuilder builder = new TestBuilder();
    builder.withVersionCheckDisabled();

    Assertions.assertFalse(builder.isEnabled());
  }

  @Test
  public void testGetServerVersion() throws JsonProcessingException {
    String version = "0.1.3";
    String date = "2024-01-03 12:28:33";
    String commitId = "6ef1f9d";

    VersionResponse resp = new VersionResponse(new VersionDTO(version, date, commitId));
    buildMockResource(Method.GET, "/api/version", null, resp, HttpStatus.SC_OK);
    GravitinoVersion gravitinoVersion = client.serverVersion();

    Assertions.assertEquals(version, gravitinoVersion.version());
    Assertions.assertEquals(date, gravitinoVersion.compileDate());
    Assertions.assertEquals(commitId, gravitinoVersion.gitCommit());
  }

  @Test
  public void testGetClientVersion() {
    GravitinoVersion version = client.clientVersion();
    Version.VersionInfo currentVersion = Version.getCurrentVersion();

    Assertions.assertEquals(currentVersion.version, version.version());
    Assertions.assertEquals(currentVersion.compileDate, version.compileDate());
    Assertions.assertEquals(currentVersion.gitCommit, version.gitCommit());
  }

  @Test
  public void testCheckVersionFailed() throws JsonProcessingException {
    String version = "0.1.1";
    String date = "2024-01-03 12:28:33";
    String commitId = "6ef1f9d";

    VersionResponse resp = new VersionResponse(new VersionDTO(version, date, commitId));
    buildMockResource(Method.GET, "/api/version", null, resp, HttpStatus.SC_OK);

    // check the client version is greater than server version
    Assertions.assertThrows(GravitinoRuntimeException.class, () -> client.checkVersion());
  }

  @Test
  public void testCheckVersionSuccess() throws JsonProcessingException {
    VersionResponse resp = new VersionResponse(Version.getCurrentVersionDTO());
    buildMockResource(Method.GET, "/api/version", null, resp, HttpStatus.SC_OK);

    // check the client version is equal to server version
    Assertions.assertDoesNotThrow(() -> client.checkVersion());

    String version = "100.1.1-SNAPSHOT";
    String date = "2024-01-03 12:28:33";
    String commitId = "6ef1f9d";

    resp = new VersionResponse(new VersionDTO(version, date, commitId));
    buildMockResource(Method.GET, "/api/version", null, resp, HttpStatus.SC_OK);

    // check the client version is less than server version
    Assertions.assertDoesNotThrow(() -> client.checkVersion());
  }

  @Test
  public void testUnusedDTOAttribute() throws JsonProcessingException {
    VersionResponse resp = new VersionResponse(Version.getCurrentVersionDTO());

    HttpRequest mockRequest = HttpRequest.request("/api/version").withMethod(Method.GET.name());
    HttpResponse mockResponse = HttpResponse.response().withStatusCode(HttpStatus.SC_OK);
    String respJson = MAPPER.writeValueAsString(resp);

    // add unused attribute for version DTO
    respJson = respJson.replace("\"gitCommit\"", "\"unused_key\":\"unused_value\", \"gitCommit\"");
    mockResponse = mockResponse.withBody(respJson);
    mockServer.when(mockRequest, Times.exactly(1)).respond(mockResponse);

    Assertions.assertDoesNotThrow(
        () -> {
          GravitinoVersion version = client.serverVersion();
          Version.VersionInfo currentVersion = Version.getCurrentVersion();
          Assertions.assertEquals(currentVersion.version, version.version());
          Assertions.assertEquals(currentVersion.compileDate, version.compileDate());
          Assertions.assertEquals(currentVersion.gitCommit, version.gitCommit());
        });
  }
}
