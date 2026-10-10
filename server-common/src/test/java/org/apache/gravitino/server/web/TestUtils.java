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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.audit.FilesetAuditConstants;
import org.apache.gravitino.audit.FilesetDataOperation;
import org.apache.gravitino.audit.InternalClientType;
import org.apache.gravitino.auxiliary.AuxiliaryServiceManager;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.exceptions.UnmodifiableStatisticException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

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
public class TestUtils {

  @Test
  public void testRemoteUser() {
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    when(mockRequest.getRemoteUser()).thenReturn("user");
    String remoteUser = Utils.remoteUser(mockRequest);
    assertEquals("user", remoteUser);
  }

  @Test
  public void testRemoteUserDefault() {
    HttpServletRequest mockRequest = mock(HttpServletRequest.class);
    when(mockRequest.getRemoteUser()).thenReturn(null);
    String remoteUser = Utils.remoteUser(mockRequest);
    assertEquals("gravitino", remoteUser);
  }

  @Test
  public void testOkWithData() {
    Response response = Utils.ok("data");
    assertNotNull(response);
    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    assertEquals("data", response.getEntity());
    assertEquals(MediaType.APPLICATION_JSON, response.getMediaType().toString());
  }

  @Test
  public void testOkWithoutData() {
    Response response = Utils.ok();
    assertNotNull(response);
    assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
    assertEquals(MediaType.APPLICATION_JSON, response.getMediaType().toString());
  }

  @Test
  public void testIllegalArguments() {
    Response response = Utils.illegalArguments("Invalid argument");
    assertNotNull(response);
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    assertEquals(MediaType.APPLICATION_JSON, response.getMediaType().toString());
    ErrorResponse errorResponse = (ErrorResponse) response.getEntity();
    assertEquals("Invalid argument", errorResponse.getMessage());
  }

  @Test
  public void testConnectionFailed() {
    Response response = Utils.connectionFailed("Connection failed");
    assertNotNull(response);
    assertEquals(Response.Status.BAD_GATEWAY.getStatusCode(), response.getStatus());
    assertEquals(MediaType.APPLICATION_JSON, response.getMediaType().toString());
    ErrorResponse errorResponse = (ErrorResponse) response.getEntity();
    assertEquals("Connection failed", errorResponse.getMessage());
  }

  @Test
  public void testInternalError() {
    Response response = Utils.internalError("Internal error");
    assertNotNull(response);
    assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());
    assertEquals(MediaType.APPLICATION_JSON, response.getMediaType().toString());
    ErrorResponse errorResponse = (ErrorResponse) response.getEntity();
    assertEquals("Internal error", errorResponse.getMessage());
  }

  @Test
  public void testNotFoundWithType() {
    Response response = Utils.notFound("Resource", "Not found");
    assertNotNull(response);
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
    assertEquals(MediaType.APPLICATION_JSON, response.getMediaType().toString());
    ErrorResponse errorResponse = (ErrorResponse) response.getEntity();
    assertEquals("Resource", errorResponse.getType());
    assertEquals("Not found", errorResponse.getMessage());
  }

  @Test
  public void testNotFoundWithThrowable() {
    Throwable throwable = new RuntimeException("Some error");
    Response response = Utils.notFound("Resource", throwable);
    assertNotNull(response);
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
    assertEquals(MediaType.APPLICATION_JSON, response.getMediaType().toString());
    ErrorResponse errorResponse = (ErrorResponse) response.getEntity();
    assertEquals("RuntimeException", errorResponse.getType());
    assertEquals("Resource", errorResponse.getMessage());
  }

  @Test
  public void testAlreadyExistsWithType() {
    Response response = Utils.alreadyExists("Resource", "Already exists");
    assertNotNull(response);
    assertEquals(Response.Status.CONFLICT.getStatusCode(), response.getStatus());
    assertEquals(MediaType.APPLICATION_JSON, response.getMediaType().toString());
    ErrorResponse errorResponse = (ErrorResponse) response.getEntity();
    assertEquals("Resource", errorResponse.getType());
    assertEquals("Already exists", errorResponse.getMessage());
  }

  @Test
  public void testAlreadyExistsWithThrowable() {
    Throwable throwable = new RuntimeException("Already exists");
    Response response = Utils.alreadyExists("New message", throwable);
    assertNotNull(response);
    assertEquals(Response.Status.CONFLICT.getStatusCode(), response.getStatus());
    assertEquals(MediaType.APPLICATION_JSON, response.getMediaType().toString());
    ErrorResponse errorResponse = (ErrorResponse) response.getEntity();
    assertEquals("RuntimeException", errorResponse.getType());
    assertEquals("New message", errorResponse.getMessage());
  }

  @Test
  public void testUnsupportedOperation() {
    Response response = Utils.unsupportedOperation("Unsupported operation");
    assertNotNull(response);
    assertEquals(Response.Status.NOT_IMPLEMENTED.getStatusCode(), response.getStatus());
    assertEquals(MediaType.APPLICATION_JSON, response.getMediaType().toString());
    ErrorResponse errorResponse = (ErrorResponse) response.getEntity();
    assertEquals("Unsupported operation", errorResponse.getMessage());
  }

  @Test
  public void testOperationConflict() {
    UnmodifiableStatisticException exception =
        new UnmodifiableStatisticException("Unmodifiable statistic");
    Response response = Utils.operationConflict(exception.getMessage(), exception);

    assertNotNull(response);
    assertEquals(Response.Status.CONFLICT.getStatusCode(), response.getStatus());
    assertEquals(MediaType.APPLICATION_JSON, response.getMediaType().toString());
    ErrorResponse errorResponse = (ErrorResponse) response.getEntity();
    assertEquals(ErrorConstants.UNSUPPORTED_OPERATION_CODE, errorResponse.getCode());
    assertEquals(UnmodifiableStatisticException.class.getSimpleName(), errorResponse.getType());
  }

  @Test
  public void testMethodNotAllowed() {
    Response response = Utils.methodNotAllowed("Method not allowed");

    assertNotNull(response);
    assertEquals(Response.Status.METHOD_NOT_ALLOWED.getStatusCode(), response.getStatus());
    assertEquals(MediaType.APPLICATION_JSON, response.getMediaType().toString());
    ErrorResponse errorResponse = (ErrorResponse) response.getEntity();
    assertEquals(ErrorConstants.UNSUPPORTED_OPERATION_CODE, errorResponse.getCode());
  }

  @Test
  public void testFilterFilesetAuditHeaders() {
    // test invalid internal client type
    HttpServletRequest mockRequest1 = Mockito.mock(HttpServletRequest.class);
    when(mockRequest1.getHeader(FilesetAuditConstants.HTTP_HEADER_INTERNAL_CLIENT_TYPE))
        .thenReturn("test");
    Map<String, String> auditMap1 = Utils.filterFilesetAuditHeaders(mockRequest1);
    Assertions.assertEquals(
        InternalClientType.UNKNOWN.name(),
        auditMap1.get(FilesetAuditConstants.HTTP_HEADER_INTERNAL_CLIENT_TYPE));

    // test invalid fileset data operation
    HttpServletRequest mockRequest2 = Mockito.mock(HttpServletRequest.class);
    when(mockRequest2.getHeader(FilesetAuditConstants.HTTP_HEADER_FILESET_DATA_OPERATION))
        .thenReturn("test");
    Map<String, String> auditMap2 = Utils.filterFilesetAuditHeaders(mockRequest2);
    Assertions.assertEquals(
        FilesetDataOperation.UNKNOWN.name(),
        auditMap2.get(FilesetAuditConstants.HTTP_HEADER_FILESET_DATA_OPERATION));

    // test normal audit headers
    HttpServletRequest mockRequest3 = Mockito.mock(HttpServletRequest.class);
    when(mockRequest3.getHeader(FilesetAuditConstants.HTTP_HEADER_INTERNAL_CLIENT_TYPE))
        .thenReturn(InternalClientType.HADOOP_GVFS.name());
    when(mockRequest3.getHeader(FilesetAuditConstants.HTTP_HEADER_FILESET_DATA_OPERATION))
        .thenReturn(FilesetDataOperation.GET_FILE_STATUS.name());
    Map<String, String> filteredMap = Utils.filterFilesetAuditHeaders(mockRequest3);
    Assertions.assertEquals(2, filteredMap.size());
    Assertions.assertEquals(
        InternalClientType.HADOOP_GVFS.name(),
        filteredMap.get(FilesetAuditConstants.HTTP_HEADER_INTERNAL_CLIENT_TYPE));
    Assertions.assertEquals(
        FilesetDataOperation.GET_FILE_STATUS.name(),
        filteredMap.get(FilesetAuditConstants.HTTP_HEADER_FILESET_DATA_OPERATION));
  }

  @Test
  void testIcebergRestServiceUriRequiresRegisteredService() {
    assertNull(
        Utils.resolveIcebergRestServiceUri(
            mockAuxServiceManager(false),
            Map.of(
                "catalog-config-provider", "dynamic-config-provider",
                "advertised-uri", "invalid"),
            "test",
            "gravitino-host"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "static-config-provider"})
  void testIcebergRestServiceUriRequiresDynamicProvider(String provider) {
    assertNull(
        Utils.resolveIcebergRestServiceUri(
            mockAuxServiceManager(true),
            Map.of("catalog-config-provider", provider, "advertised-uri", "invalid"),
            "test",
            "gravitino-host"));
  }

  @Test
  void testIcebergRestServiceUriMatchesRequestedMetalake() {
    AuxiliaryServiceManager serviceManager = mockAuxServiceManager(true);
    String advertisedUri = "https://iceberg.example.com/proxy/iceberg/";
    Map<String, String> config =
        Map.of(
            "catalog-config-provider", "dynamic-config-provider",
            "gravitino-metalake", "prod",
            "advertised-uri", advertisedUri);

    assertNull(
        Utils.resolveIcebergRestServiceUri(serviceManager, config, "test", "gravitino-host"));
    assertEquals(
        advertisedUri,
        Utils.resolveIcebergRestServiceUri(serviceManager, config, "prod", "gravitino-host"));
    assertEquals(
        advertisedUri,
        Utils.resolveIcebergRestServiceUri(serviceManager, config, null, "gravitino-host"));
    assertEquals(
        advertisedUri,
        Utils.resolveIcebergRestServiceUri(serviceManager, config, "", "gravitino-host"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"", " ", "0.0.0.0", "::", "[::]", "0:0:0:0:0:0:0:0"})
  void testIcebergRestServiceUriUsesProvidedRequestHost(String listenerHost) {
    assertEquals(
        "http://[::1]:9001/iceberg",
        Utils.resolveIcebergRestServiceUri(
            mockAuxServiceManager(true),
            Map.of("catalog-config-provider", "dynamic-config-provider", "host", listenerHost),
            "test",
            "::1"));
  }

  @Test
  void testIcebergRestServiceUriUsesListenerConfiguration() {
    assertEquals(
        "https://iceberg-host:19433/iceberg",
        Utils.resolveIcebergRestServiceUri(
            mockAuxServiceManager(true),
            Map.of(
                "catalog-config-provider", "dynamic-config-provider",
                "host", "iceberg-host",
                "enableHttps", "true",
                "httpPort", "19001",
                "httpsPort", "19433"),
            "test",
            "gravitino-host"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testIcebergRestServiceUriUsesDefaultPorts(boolean enableHttps) {
    assertEquals(
        enableHttps ? "https://gravitino-host:9433/iceberg" : "http://gravitino-host:9001/iceberg",
        Utils.resolveIcebergRestServiceUri(
            mockAuxServiceManager(true),
            Map.of(
                "catalog-config-provider",
                "dynamic-config-provider",
                "enableHttps",
                Boolean.toString(enableHttps)),
            "test",
            "gravitino-host"));
  }

  @Test
  void testIcebergRestServiceUriPrefersAdvertisedUri() {
    assertEquals(
        "https://iceberg.example.com:8443/proxy/iceberg/",
        Utils.resolveIcebergRestServiceUri(
            mockAuxServiceManager(true),
            Map.of(
                "catalog-config-provider", "dynamic-config-provider",
                "host", "iceberg-host",
                "httpPort", "19001",
                "advertised-uri", " https://iceberg.example.com:8443/proxy/iceberg/ "),
            "test",
            "gravitino-host"));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "iceberg.example.com/iceberg",
        "ftp://iceberg.example.com/iceberg",
        "https:///iceberg",
        "https://iceberg.example.com/iceberg?x=1",
        "https://iceberg.example.com/iceberg#frag",
        "https://iceberg.example.com:0/iceberg",
        "https://iceberg.example.com:70000/iceberg",
        "http://bad host/iceberg"
      })
  void testIcebergRestServiceUriRejectsInvalidAdvertisedUri(String advertisedUri) {
    assertThrows(
        IllegalStateException.class,
        () ->
            Utils.resolveIcebergRestServiceUri(
                mockAuxServiceManager(true),
                Map.of(
                    "catalog-config-provider",
                    "dynamic-config-provider",
                    "advertised-uri",
                    advertisedUri),
                "test",
                "gravitino-host"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"", " ", "not-a-number"})
  void testIcebergRestServiceUriUsesDefaultForInvalidPort(String port) {
    assertEquals(
        "http://gravitino-host:9001/iceberg",
        Utils.resolveIcebergRestServiceUri(
            mockAuxServiceManager(true),
            Map.of("catalog-config-provider", "dynamic-config-provider", "httpPort", port),
            "test",
            "gravitino-host"));
  }

  private static AuxiliaryServiceManager mockAuxServiceManager(boolean registered) {
    AuxiliaryServiceManager serviceManager = mock(AuxiliaryServiceManager.class);
    when(serviceManager.isAuxServiceRegistered("iceberg-rest")).thenReturn(registered);
    return serviceManager;
  }
}
